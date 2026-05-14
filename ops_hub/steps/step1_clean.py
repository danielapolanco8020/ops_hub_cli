import re
import time
import pandas as pd
from pathlib import Path

from config import (
    INPUT_DIR, OUT_STEP1,
    UNWANTED_NAMES,
)
from utils.file_helpers import (
    get_excel_files, read_excel, save_excel,
    prompt_yes_no, prompt_int, print_header, print_step, print_done,
    print_warn, print_error, make_output_path,
)


# ── Link properties fix ────────────────────────────────────────────────────────

def _fix_link_properties(df: pd.DataFrame) -> pd.DataFrame:
    if "LINK PROPERTIES" not in df.columns:
        return df
    df["LINK PROPERTIES"] = df["LINK PROPERTIES"].astype(str).str.strip()
    def _extract(val):
        match = re.match(r'=HYPERLINK\("[^"]*",\s*"([^"]*)"\)', val, re.IGNORECASE)
        return match.group(1) if match else val
    df["LINK PROPERTIES"] = df["LINK PROPERTIES"].apply(_extract)
    return df


# ── Filters ────────────────────────────────────────────────────────────────────

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


def _filter_absentee_same_address(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not {"ABSENTEE", "ADDRESS", "MAILING ADDRESS"}.issubset(df.columns):
        return df, pd.DataFrame()
    mask = (df["ABSENTEE"] >= 1) & (df["ADDRESS"] == df["MAILING ADDRESS"])
    rej  = df[mask].copy()
    rej["Rejection_Stage"] = "Absentee Same Address"
    rej["Rejection_Value"] = rej["ADDRESS"]
    return df[~mask], rej


# ── Geographic distribution ────────────────────────────────────────────────────

def _capture_distribution(df: pd.DataFrame, col: str) -> dict[str, float]:
    counts = df[col].value_counts(normalize=True)
    return counts.to_dict()


def _apply_distribution(df: pd.DataFrame, col: str, distribution: dict[str, float], goal: int) -> pd.DataFrame:
    score_cols = [c for c in ["BUYBOX SCORE", "LIKELY DEAL SCORE", "SCORE"] if c in df.columns]
    result_frames = []
    allocated     = 0
    areas         = list(distribution.keys())

    for i, area in enumerate(areas):
        area_df = df[df[col] == area].copy()
        if area_df.empty:
            continue
        if i == len(areas) - 1:
            n = goal - allocated
        else:
            n = round(distribution[area] * goal)
        n = min(n, len(area_df))
        if score_cols:
            for c in score_cols:
                area_df[c] = pd.to_numeric(area_df[c], errors="coerce")
            area_df = area_df.sort_values(by=score_cols, ascending=[False] * len(score_cols))
        result_frames.append(area_df.head(n))
        allocated += n

    if not result_frames:
        return df
    return pd.concat(result_frames, ignore_index=True)


# ── Filename K-count update ────────────────────────────────────────────────────

def _update_filename_k(name: str, row_count: int) -> str:
    k_val = row_count / 1000
    k_str = f"{int(k_val)}K" if k_val == int(k_val) else f"{round(k_val, 1)}K"
    new_name = re.sub(r'\d+(\.\d+)?K', k_str, name)
    return new_name if new_name != name else name


# ── Output folder cleanup ──────────────────────────────────────────────────────

def _prompt_clear_output_folders():
    from config import OUTPUT_DIR
    print("\n  ⚠  Starting a new clean process.")
    print("  It is recommended to clear all output folders to avoid mixing old and new files.")

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

    # ── Sort by scores ─────────────────────────────────────────────────────────
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

    # ── Fix LINK PROPERTIES ────────────────────────────────────────────────────
    df = _fix_link_properties(df)

    # ── Capture geographic distribution BEFORE cleaning ────────────────────────
    geo_col          = None
    geo_distribution = {}
    preserve_geo     = False

    if {"COUNTY", "ZIP"}.intersection(df.columns):
        preserve_geo = prompt_yes_no(
            f"\n  [{file.name}] Preserve geographic distribution after cleaning?",
            default=False
        )
        if preserve_geo:
            print("    Which column defines geographic areas?")
            choices = []
            if "COUNTY" in df.columns: choices.append("COUNTY")
            if "ZIP"    in df.columns: choices.append("ZIP")
            if len(choices) == 1:
                geo_col = choices[0]
                print(f"    Using: {geo_col}")
            else:
                while True:
                    c = input("    Enter COUNTY or ZIP: ").strip().upper()
                    if c in choices:
                        geo_col = c
                        break
                    print(f"    Enter one of: {', '.join(choices)}")
            geo_distribution = _capture_distribution(df, geo_col)
            print_done(f"    Captured distribution across {len(geo_distribution)} {geo_col} values.")

    # ── Define _apply helper ───────────────────────────────────────────────────
    def _apply(filter_fn, *args):
        nonlocal df
        try:
            df, rej = filter_fn(*args)
            if not rej.empty:
                rej["Source_File"] = file.name
                rejects.append(rej)
        except Exception as e:
            print_warn(f"  Filter '{filter_fn.__name__}' skipped: {e}")

    # ── Filters (order matters) ────────────────────────────────────────────────
    if "ACTION PLANS" in df.columns:
        _apply(_filter_empty_action_plans, df)

    if {"MAILING ADDRESS", "MAILING ZIP"}.issubset(df.columns):
        _apply(_filter_duplicates, df, ["MAILING ADDRESS", "MAILING ZIP"], "Duplicate Address")

    if {"OWNER FULL NAME", "ADDRESS", "ZIP"}.issubset(df.columns):
        _apply(_filter_duplicates, df, ["OWNER FULL NAME", "ADDRESS", "ZIP"], "Duplicate Owner")

    if "OWNER FULL NAME" in df.columns:
        _apply(_filter_empty_owner_name, df)
        _apply(_filter_unwanted_names, df)

    # ── Absentee same address — automatic for DM files only ───────────────────
    is_dm = "direct mail" in file.name.lower()
    if is_dm and {"ABSENTEE", "ADDRESS", "MAILING ADDRESS"}.issubset(df.columns):
        absentee_count = ((df["ABSENTEE"] >= 1) & (df["ADDRESS"] == df["MAILING ADDRESS"])).sum()
        if absentee_count > 0:
            _apply(_filter_absentee_same_address, df)
            print_done(f"  Removed {absentee_count:,} absentee rows with matching address (DM only).")

    cleaned_rows = len(df)
    print_done(f"  Cleaned: {cleaned_rows:,} rows (from {original_rows:,})")

    # ── Row goal prompt ────────────────────────────────────────────────────────
    goal = cleaned_rows
    print(f"\n  [{file.name}] How many rows do you want to keep?")
    print(f"  Available after cleaning: {cleaned_rows:,}")
    while True:
        raw = input(f"  Enter goal (or press Enter to keep all {cleaned_rows:,}): ").strip()
        if raw == "":
            goal = cleaned_rows
            break
        try:
            goal = int(raw)
            if 1 <= goal <= cleaned_rows:
                break
            print(f"  Enter a number between 1 and {cleaned_rows:,}.")
        except ValueError:
            print("  Enter a valid number.")

    # ── Apply geographic distribution or simple trim ───────────────────────────
    if goal < cleaned_rows:
        if preserve_geo and geo_col and geo_distribution:
            df = _apply_distribution(df, geo_col, geo_distribution, goal)
            print_done(f"  Trimmed to {len(df):,} rows preserving {geo_col} distribution.")
        else:
            df = df.head(goal)
            print_done(f"  Trimmed to {goal:,} rows keeping highest scores.")

    # ── Auto-update filename K-count ───────────────────────────────────────────
    updated_name = _update_filename_k(file.stem, len(df))
    out_filename = f"cleaned_{updated_name}.xlsx"
    out_path     = output_dir / out_filename
    save_excel(df, out_path)
    print_done(f"  Saved → {out_path.name}")

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

def run():
    print_header("STEP 1 — CLEAN")

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
        try:
            result = _process_file(f, OUT_STEP1, rejected_all)
        except Exception as e:
            import traceback
            traceback.print_exc()
            results.append({"file": f.name, "status": "error", "original": 0,
                            "cleaned": 0, "rejected": 0, "time": 0})
            continue
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