import re
import pandas as pd
from pathlib import Path

from config import OUT_STEP1, OUT_STEP2, REQUIRED_COLUMNS, CADENCES
from utils.file_helpers import (
    get_files_by_cadence, read_excel, save_excel,
    prompt_cadence_or_all, format_k,
    print_header, print_step, print_done, print_warn, print_error,
)


# ── Phone column helpers ───────────────────────────────────────────────────────

def _get_phone_columns(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Return sorted PHONE NUMBER ## and PHONE TYPE ## columns found in df."""
    num_cols  = sorted(
        [c for c in df.columns if re.match(r'PHONE NUMBER\s*\d+', c, re.IGNORECASE)],
        key=lambda x: int(re.search(r'\d+', x).group())
    )
    type_cols = sorted(
        [c for c in df.columns if re.match(r'PHONE TYPE\s*\d+', c, re.IGNORECASE)],
        key=lambda x: int(re.search(r'\d+', x).group())
    )
    return num_cols, type_cols


def _unify_phone_columns(dataframes: list[pd.DataFrame]) -> list[pd.DataFrame]:
    """
    Across all files find the full set of PHONE NUMBER ## / PHONE TYPE ## columns.
    Add missing phone columns as empty to files that don't have them.
    """
    all_num_cols:  set[str] = set()
    all_type_cols: set[str] = set()

    for df in dataframes:
        n, t = _get_phone_columns(df)
        all_num_cols.update(n)
        all_type_cols.update(t)

    all_num_cols  = sorted(all_num_cols,  key=lambda x: int(re.search(r'\d+', x).group()))
    all_type_cols = sorted(all_type_cols, key=lambda x: int(re.search(r'\d+', x).group()))

    if all_num_cols or all_type_cols:
        print_step(f"Phone columns detected across all files:")
        print(f"    Numbers : {', '.join(all_num_cols) or 'none'}")
        print(f"    Types   : {', '.join(all_type_cols) or 'none'}")

    unified = []
    for df in dataframes:
        for col in all_num_cols + all_type_cols:
            if col not in df.columns:
                df[col] = None
        unified.append(df)

    return unified


# ── Column validation & extra column handling ──────────────────────────────────

def _is_phone_column(col: str) -> bool:
    return bool(re.match(r'PHONE (NUMBER|TYPE)\s*\d+', col, re.IGNORECASE))


def _validate_and_collect_extras(
    dataframes: list[pd.DataFrame],
    filenames:  list[str],
    cadence:    str,
) -> tuple[list[pd.DataFrame], dict[str, str]]:
    """
    Validate required columns per file.
    Collect all extra columns across files and prompt user once per extra column:
      - Drop it from all files
      - Match it (keep it, fill None for files missing it)
    Returns filtered dataframes and a dict of {extra_col: 'drop'|'match'}.
    """
    required     = set(REQUIRED_COLUMNS[cadence])
    extra_cols:  set[str] = set()
    valid_frames: list[pd.DataFrame] = []
    valid_names:  list[str]          = []

    for df, fname in zip(dataframes, filenames):
        present = set(df.columns)
        missing = required - present
        extra   = present - required

        # Filter out phone columns from extra — handled separately
        extra = {c for c in extra if not _is_phone_column(c)}

        if missing:
            print_error(f"  '{fname}' missing required columns — skipping:")
            for c in sorted(missing):
                print(f"       - {c}")
            continue

        extra_cols.update(extra)
        valid_frames.append(df)
        valid_names.append(fname)
        print_done(f"  '{fname}' passed — {len(df):,} rows")

    if not valid_frames:
        return [], {}

    # ── Prompt once per extra column ───────────────────────────────────────────
    decisions: dict[str, str] = {}
    if extra_cols:
        print_step(f"Extra columns found across files (not in required list):")
        for col in sorted(extra_cols):
            files_with_col = [n for df, n in zip(valid_frames, valid_names) if col in df.columns]
            print(f"\n    Column : '{col}'")
            print(f"    Found in: {', '.join(files_with_col)}")
            while True:
                choice = input("    Action — (d) Drop  /  (m) Match across all files: ").strip().lower()
                if choice in ("d", "drop"):
                    decisions[col] = "drop"
                    break
                elif choice in ("m", "match"):
                    decisions[col] = "match"
                    break
                else:
                    print("    Enter 'd' to drop or 'm' to match.")

    # ── Apply decisions ────────────────────────────────────────────────────────
    result_frames: list[pd.DataFrame] = []
    for df, fname in zip(valid_frames, valid_names):
        for col, action in decisions.items():
            if action == "drop":
                df = df.drop(columns=[col], errors="ignore")
            elif action == "match":
                if col not in df.columns:
                    print_warn(f"  '{fname}' skipped for column '{col}' (not present)")
                    # Column will be added as None during concat via pd.concat fill
                    df[col] = None
        result_frames.append(df)

    return result_frames, decisions


# ── Main merge per cadence ─────────────────────────────────────────────────────

def _merge_cadence(cadence: str, output_dir: Path):
    # Resolve input dir per cadence — find the folder that has files for this cadence
    input_dir = None
    for folder in [OUT_STEP2, OUT_STEP1]:
        if get_files_by_cadence(folder, cadence):
            input_dir = folder
            break

    if not input_dir:
        print_warn(f"No files found for cadence '{cadence}' in any output folder.")
        return

    files = get_files_by_cadence(input_dir, cadence)
    if not files:
        print_warn(f"No files found for cadence '{cadence}' in {input_dir.name}/")
        return

    print_step(f"Processing '{cadence}' — {len(files)} file(s) found")

    # Load all files
    raw_frames: list[pd.DataFrame] = []
    raw_names:  list[str]          = []
    for f in files:
        df = read_excel(f)
        if df is None:
            continue
        print(f"    Loading: {f.name}")
        raw_frames.append(df)
        raw_names.append(f.name)

    if not raw_frames:
        print_error(f"No readable files for '{cadence}'")
        return

    # Validate + handle extra columns
    valid_frames, decisions = _validate_and_collect_extras(raw_frames, raw_names, cadence)
    if not valid_frames:
        print_error(f"No valid files to merge for '{cadence}'")
        return

    # Unify phone columns for CC and SMS
    if cadence in ("Cold Calling", "SMS"):
        valid_frames = _unify_phone_columns(valid_frames)

    # Merge
    merged = pd.concat(valid_frames, ignore_index=True)

    # Sort by scores
    sort_cols = [c for c in ["BUYBOX SCORE", "LIKELY DEAL SCORE", "SCORE"] if c in merged.columns]
    for c in sort_cols:
        merged[c] = pd.to_numeric(merged[c], errors="coerce")
    if sort_cols:
        merged.sort_values(by=sort_cols, ascending=[False] * len(sort_cols), inplace=True)

    total_k  = format_k(len(merged))
    out_name = _construct_filename(files[0].name, total_k)
    out_path = output_dir / out_name

    save_excel(merged, out_path)
    print_done(f"Saved: {out_name}  ({len(merged):,} rows)")

    # Print phone column summary for CC/SMS
    if cadence in ("Cold Calling", "SMS"):
        num_cols, type_cols = _get_phone_columns(merged)
        print_done(f"  Phone columns in merged file: {len(num_cols)} number col(s), {len(type_cols)} type col(s)")


def _construct_filename(base: str, total_k: str) -> str:
    new = re.sub(r'\s\d+(\.\d+)?K\s', f" {total_k} ", base)
    return new if new != base else f"{total_k} {base}"


# ── Entry Point ────────────────────────────────────────────────────────────────

def run():
    print_header("STEP 2A — MERGE")
    cadences = prompt_cadence_or_all("cadence to merge")
    for cadence in cadences:
        _merge_cadence(cadence, OUT_STEP2)
    print_done(f"Merge complete → {OUT_STEP2}")