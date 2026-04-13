import re
import time
import pandas as pd
from pathlib import Path
from config import (
    INPUT_DIR, OUT_STEP1,
    UNWANTED_NAMES, UNWANTED_OWNER_TYPES,
)
from utils.file_helpers import (
    get_excel_files, read_excel, save_excel,
    prompt_yes_no, print_header, print_step, print_done, print_warn, print_error,
    make_output_path,
)


# ── Filters ────────────────────────────────────────────────────────────────────

def _fix_link_properties(df: pd.DataFrame) -> pd.DataFrame:
    """Convert LINK PROPERTIES column to plain text, stripping any Excel hyperlink formulas."""
    if "LINK PROPERTIES" not in df.columns:
        return df
    df["LINK PROPERTIES"] = df["LINK PROPERTIES"].astype(str).str.strip()
    def _extract(val):
        match = re.match(r'=HYPERLINK\("[^"]*",\s*"([^"]*)"\)', val, re.IGNORECASE)
        return match.group(1) if match else val
    df["LINK PROPERTIES"] = df["LINK PROPERTIES"].apply(_extract)
    return df


def _filter_empty_owner_name(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    mask = df["OWNER FULL NAME"].isnull() | (df["OWNER FULL NAME"].astype(str).str.strip() == "")
    rej  = df[mask].copy()
    rej["Rejection_Stage"] = "Empty Owner Full Name"
    rej["Rejection_Value"] = ""
    return df[~mask], rej


def _filter_unwanted_names(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    pattern = re.compile("|".join(map(re.escape, UNWANTED_NAMES)), re.IGNORECASE)
    mask    = df["OWNER FULL NAME"].str.contains(pattern, na=False)
    rej     = df[mask].copy()
    rej["Rejection_Stage"] = "Unwanted Names"
    rej["Rejection_Value"] = rej["OWNER FULL NAME"]
    return df[~mask], rej


def _filter_owner_types(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "OWNER TYPE" not in df.columns:
        return df, pd.DataFrame(columns=df.columns.tolist() + ["Rejection_Stage", "Rejection_Value"])
    pattern = re.compile("|".join(map(re.escape, UNWANTED_OWNER_TYPES)), re.IGNORECASE)
    mask    = df["OWNER TYPE"].str.contains(pattern, na=False)
    rej     = df[mask].copy()
    rej["Rejection_Stage"] = "Unwanted Owner Type"
    rej["Rejection_Value"] = rej["OWNER TYPE"]
    return df[~mask], rej


def _filter_empty_action_plans(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    mask = df["ACTION PLANS"].notna()
    rej  = df[~mask].copy()
    rej["Rejection_Stage"] = "Empty Action Plans"
    rej["Rejection_Value"] = ""
    return df[mask], rej


def _filter_duplicates(df: pd.DataFrame, subset: list[str], stage: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    duped_mask = df.duplicated(subset=subset, keep=False)
    rej = df[duped_mask].copy()
    rej["Rejection_Stage"] = stage
    rej["Rejection_Value"] = rej.apply(
        lambda r: " | ".join(str(r[c]) for c in subset), axis=1
    )
    return df[~duped_mask], rej


# ── Single File ────────────────────────────────────────────────────────────────

def _process_file(file: Path, output_dir: Path, rejected_all: list[pd.DataFrame]) -> dict:
    t0  = time.time()
    df  = read_excel(file)
    if df is None:
        return {"file": file.name, "status": "error", "rows": 0}

    original_rows = len(df)
    rejects: list[pd.DataFrame] = []

    # ── Fill owner last name fallback ──────────────────────────────────────────
    if "OWNER LAST NAME" in df.columns and "OWNER FULL NAME" in df.columns:
        df["OWNER LAST NAME"] = df["OWNER LAST NAME"].fillna(df["OWNER FULL NAME"])

    # ── Sort by scores (BUYBOX → LIKELY DEAL → SCORE, all high to low) ─────────
    score_cols = [c for c in ["BUYBOX SCORE", "LIKELY DEAL SCORE", "SCORE"] if c in df.columns]
    if score_cols:
        for c in score_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df.sort_values(by=score_cols, ascending=[False] * len(score_cols), inplace=True)
        df.reset_index(drop=True, inplace=True)

    # ── Fill mailing address fallback ──────────────────────────────────────────
    if "MAILING ADDRESS" in df.columns and "ADDRESS" in df.columns:
        df["MAILING ADDRESS"] = df["MAILING ADDRESS"].fillna(df["ADDRESS"])
    if "MAILING ZIP" in df.columns and "ZIP" in df.columns:
        df["MAILING ZIP"] = df["MAILING ZIP"].fillna(df["ZIP"])

    # ── Filters (order matters) ────────────────────────────────────────────────
    def _apply(filter_fn, *args):
        nonlocal df
        try:
            df, rej = filter_fn(*args)
            if not rej.empty:
                rej["Source_File"] = file.name
                rejects.append(rej)
        except Exception as e:
            print_warn(f"  Filter '{filter_fn.__name__}' skipped: {e}")

    if "ACTION PLANS" in df.columns:
        _apply(_filter_empty_action_plans, df)

    if {"MAILING ADDRESS", "MAILING ZIP"}.issubset(df.columns):
        _apply(_filter_duplicates, df, ["MAILING ADDRESS", "MAILING ZIP"], "Duplicate Address")

    if {"OWNER FULL NAME", "ADDRESS", "ZIP"}.issubset(df.columns):
        _apply(_filter_duplicates, df, ["OWNER FULL NAME", "ADDRESS", "ZIP"], "Duplicate Owner")

    if "OWNER FULL NAME" in df.columns:
        _apply(_filter_empty_owner_name, df)
        _apply(_filter_unwanted_names, df)

    _apply(_filter_owner_types, df)

    # ── Fix LINK PROPERTIES to plain text ─────────────────────────────────────
    df = _fix_link_properties(df)

    # ── Drop OWNER TYPE column after filtering ─────────────────────────────────
    df = df.drop(columns=["OWNER TYPE"], errors="ignore")

    # ── Save cleaned file ──────────────────────────────────────────────────────
    out_path = make_output_path(output_dir, file.name, prefix="cleaned_")
    save_excel(df, out_path)

    # ── Accumulate rejects ─────────────────────────────────────────────────────
    non_empty = [r for r in rejects if isinstance(r, pd.DataFrame) and not r.empty]
    if non_empty:
        combined_rej = pd.concat(non_empty, ignore_index=True)
        rejected_all.append(combined_rej)

    elapsed = time.time() - t0
    return {
        "file":     file.name,
        "status":   "ok",
        "original": original_rows,
        "cleaned":  len(df),
        "rejected": original_rows - len(df),
        "time":     round(elapsed, 2),
    }


# ── Rejection Summary ──────────────────────────────────────────────────────────

def _save_rejection_summary(rejected_all: list[pd.DataFrame], output_dir: Path):
    if not rejected_all:
        print_warn("No rejected rows found across all files.")
        return

    all_rej = pd.concat([r for r in rejected_all if not r.empty], ignore_index=True)
    if all_rej.empty:
        print_warn("No rejected rows found across all files.")
        return

    rej_path = output_dir / "Rejected_Properties.xlsx"
    save_excel(all_rej, rej_path)
    print_done(f"Rejected properties saved → {rej_path.name}")

    summary = (
        all_rej.groupby(["Source_File", "Rejection_Stage"])
        .size()
        .reset_index(name="Rejected_Count")
    )
    summary_path = output_dir / "Rejection_Summary.xlsx"
    save_excel(summary, summary_path)
    print_done(f"Rejection summary saved → {summary_path.name}")


# ── Entry Point ────────────────────────────────────────────────────────────────

def _prompt_clear_output_folders():
    """Ask user to clear all output folders before starting a new clean run."""
    from config import OUTPUT_DIR
    print("\n  ⚠  Starting a new clean process.")
    print("  It is recommended to clear all output folders to avoid mixing old and new files.")
    print(f"  Output folder: {OUTPUT_DIR}")

    folders = [f for f in OUTPUT_DIR.rglob("*.xlsx")]
    if not folders:
        print_done("  Output folders are already empty.")
        return

    print(f"\n  Found {len(folders)} existing file(s) across output folders.")
    confirm = prompt_yes_no("  Clear all output folders now?", default=True)
    if not confirm:
        print_warn("  Output folders not cleared — old files may mix with new results.")
        return

    deleted = 0
    for f in folders:
        try:
            f.unlink()
            deleted += 1
        except Exception as e:
            print_warn(f"  Could not delete {f.name}: {e}")
    print_done(f"  Cleared {deleted} file(s) from output folders.")


def run():
    print_header("STEP 1 — CLEAN")

    # Prompt to clear output folders before starting
    _prompt_clear_output_folders()

    files = get_excel_files(INPUT_DIR)
    if not files:
        print_error(f"No Excel files found in {INPUT_DIR}")
        return

    print_step(f"Found {len(files)} file(s) in {INPUT_DIR.name}/")

    rejected_all: list[pd.DataFrame] = []
    results = []

    for f in files:
        print_step(f"Processing: {f.name}")
        result = _process_file(f, OUT_STEP1, rejected_all)
        results.append(result)

        if result["status"] == "ok":
            print_done(
                f"{result['file']} — "
                f"{result['original']:,} rows in, "
                f"{result['cleaned']:,} cleaned, "
                f"{result['rejected']:,} rejected "
                f"({result['time']}s)"
            )
        else:
            print_error(f"Failed to process {result['file']}")

    _save_rejection_summary(rejected_all, OUT_STEP1)

    total_in  = sum(r.get("original", 0) for r in results)
    total_out = sum(r.get("cleaned",  0) for r in results)
    total_rej = sum(r.get("rejected", 0) for r in results)

    print("\n" + "-" * 60)
    print(f"  Files processed : {len(results)}")
    print(f"  Total rows in   : {total_in:,}")
    print(f"  Total cleaned   : {total_out:,}")
    print(f"  Total rejected  : {total_rej:,}")
    print(f"  Output folder   : {OUT_STEP1}")
    print("-" * 60)