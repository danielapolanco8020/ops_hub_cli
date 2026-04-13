import re
import pandas as pd
from pathlib import Path

from config import (
    AUDIT_URGENT_PLAN, AUDIT_HIGH_PLAN,
    AUDIT_URGENT_MIN_SCORE, AUDIT_HIGH_MIN_SCORE,
    AUDIT_OWNER_KEYWORDS, TAGS_BLACKLIST,
)
from utils.file_helpers import (
    get_excel_files, read_excel, save_excel,
    prompt_int, prompt_yes_no,
    print_header, print_step, print_done, print_warn, print_error,
)


def _check(label: str, passed: bool, detail: str = ""):
    status = "PASS" if passed else "FAIL"
    line   = f"  [{status}] {label}"
    if detail:
        line += f"  → {detail}"
    print(line)


def _audit_file(df: pd.DataFrame, file_path: Path):
    print(f"\n  File : {file_path.name}")
    print(f"  Rows : {len(df):,}")
    print("  " + "-" * 54)

    if {"MAILING ADDRESS", "MAILING ZIP"}.issubset(df.columns):
        dupes = df.duplicated(subset=["MAILING ADDRESS", "MAILING ZIP"], keep=False)
        _check("Duplicates (Mailing Address + ZIP)", not dupes.any(),
               f"{dupes.sum():,} duplicate rows" if dupes.any() else "")
    else:
        print_warn("  MAILING ADDRESS / MAILING ZIP columns missing — skipped.")

    if {"OWNER FULL NAME", "ADDRESS", "ZIP"}.issubset(df.columns):
        dupes2 = df.duplicated(subset=["OWNER FULL NAME", "ADDRESS", "ZIP"], keep=False)
        _check("Duplicates (Owner + Address + ZIP)", not dupes2.any(),
               f"{dupes2.sum():,} duplicate rows" if dupes2.any() else "")
    else:
        print_warn("  OWNER FULL NAME / ADDRESS / ZIP columns missing — skipped.")

    if "FOLIO" in df.columns:
        dup_folio = df[df.duplicated(subset=["FOLIO"], keep=False)]
        _check("Unique FOLIO", dup_folio.empty,
               f"{len(dup_folio):,} rows with duplicate FOLIO" if not dup_folio.empty else "")
    else:
        print_warn("  FOLIO column missing — skipped.")

    if "OWNER FULL NAME" in df.columns:
        pattern = "|".join(AUDIT_OWNER_KEYWORDS)
        flagged = df[df["OWNER FULL NAME"].str.contains(pattern, na=False)]
        _check("Owner Full Name (keywords)", flagged.empty,
               f"{len(flagged):,} flagged names" if not flagged.empty else "")
    else:
        print_warn("  OWNER FULL NAME column missing — skipped.")

    for col in ["OWNER FULL NAME", "OWNER LAST NAME", "ADDRESS", "ZIP",
                "MAILING ADDRESS", "MAILING ZIP", "PROPERTY STATUS"]:
        if col in df.columns:
            empty = df[col].isnull().sum()
            _check(f"{col} complete", empty == 0,
                   f"{empty:,} empty cells" if empty else "")

    if "COUNTY" in df.columns:
        unique_counties = df["COUNTY"].dropna().str.lower().unique()
        print(f"  [INFO] Unique counties: {', '.join(sorted(unique_counties))}")

    if "PROPERTY TYPE" in df.columns:
        unique_types = df["PROPERTY TYPE"].dropna().str.lower().unique()
        print(f"  [INFO] Unique property types: {', '.join(sorted(unique_types))}")

    if "TAGS" in df.columns:
        df["TAGS"]   = df["TAGS"].astype(str).fillna("")
        tags_pattern = "|".join(map(re.escape, TAGS_BLACKLIST))
        bad_tags     = df[df["TAGS"].str.contains(tags_pattern, na=False)]
        _check("Tags review", bad_tags.empty,
               f"{len(bad_tags):,} properties with unwanted tags" if not bad_tags.empty else "")

    if {"ABSENTEE", "ADDRESS", "MAILING ADDRESS"}.issubset(df.columns):
        absentee_same = df[(df["ABSENTEE"] >= 1) & (df["ADDRESS"] == df["MAILING ADDRESS"])]
        _check("Absentee ≠ Mailing Address", absentee_same.empty,
               f"{len(absentee_same):,} absentee with same address" if not absentee_same.empty else "")

    if {"ACTION PLANS", "SCORE"}.issubset(df.columns):
        urgent = df[
            (df["ACTION PLANS"] == AUDIT_URGENT_PLAN) &
            (df["SCORE"] < AUDIT_URGENT_MIN_SCORE)
        ]
        _check(f"Urgent score (30 DAYS ≥ {AUDIT_URGENT_MIN_SCORE})", urgent.empty,
               f"{len(urgent):,} below threshold" if not urgent.empty else "")

        high = df[
            (df["ACTION PLANS"] == AUDIT_HIGH_PLAN) &
            (df["SCORE"] < AUDIT_HIGH_MIN_SCORE)
        ]
        _check(f"High score (60 DAYS ≥ {AUDIT_HIGH_MIN_SCORE})", high.empty,
               f"{len(high):,} below threshold" if not high.empty else "")

    fname_lower = file_path.name.lower()
    is_cc  = "cold calling" in fname_lower or re.search(r'\bcc\b', fname_lower) is not None
    is_sms = "sms" in fname_lower

    if is_cc or is_sms:
        phone_type_cols = [c for c in df.columns if c.upper().startswith("PHONE TYPE")]
        phone_num_cols  = [c for c in df.columns if c.upper().startswith("PHONE NUMBER")]
        all_phone_cols  = phone_type_cols + phone_num_cols
        keywords        = "void|null|failed" + ("|landline" if is_sms else "")

        for col in phone_type_cols:
            # Convert to string only for keyword matching, don't modify original df
            col_str = df[col].fillna("").astype(str)
            flagged  = df[col_str.str.contains(keywords, case=False, na=False)]
            _check(f"{col} ({'SMS' if is_sms else 'CC'} check)", flagged.empty,
                   f"{len(flagged):,} invalid phone types" if not flagged.empty else "")

        if all_phone_cols:
            # Count BEFORE any string conversion
            has_phone  = df[all_phone_cols].apply(
                lambda col: col.replace("", pd.NA).notna()
            ).any(axis=1)
            with_phone = int(has_phone.sum())
            no_phone   = int((~has_phone).sum())
            total      = len(df)

            print(f"\n  --- PHONE COUNT ---")
            print(f"  Total properties                       : {total:,}")
            print(f"  Properties with active phone numbers   : {with_phone:,}")
            print(f"  Properties without active phone numbers: {no_phone:,}")

            if "TAGS" in df.columns:
                skip_pattern = re.compile(r'\bskip\b', re.IGNORECASE)
                no_skip = df[~has_phone & ~df["TAGS"].str.contains(skip_pattern, na=False)]
                print(f"  Properties without SKIPTRACE tag       : {len(no_skip):,}")

            # ── Goal prompt — reporting only, no rows dropped ──────────────────
            print(f"\n  How many rows do you want to keep from this file?")
            print(f"  Available with active phones: {with_phone:,}")
            print(f"  Maximum possible (all rows) : {total:,}")
            while True:
                raw = input(f"  Enter goal (or press Enter for max {total:,}): ").strip()
                if raw == "":
                    goal = total
                    break
                try:
                    goal = int(raw)
                    if 1 <= goal <= total:
                        break
                    print(f"  Enter a number between 1 and {total:,}.")
                except ValueError:
                    print("  Enter a valid number.")

            if goal > with_phone:
                shortfall = goal - with_phone
                print_warn(f"  Goal of {goal:,} set — note that {shortfall:,} rows will not have active phone numbers.")
            else:
                print_done(f"  Goal of {goal:,} rows — all will have active phone numbers.")


def run():
    print_header("STEP 3 — AUDIT & PHONE COUNT")

    # Resolve input dir — use step2 if it has files, otherwise step1
    from config import OUT_STEP2, OUT_STEP1
    if list(OUT_STEP2.glob("*.xlsx")):
        input_dir = OUT_STEP2
    elif list(OUT_STEP1.glob("*.xlsx")):
        input_dir = OUT_STEP1
    else:
        print_error("No processed files found in any output folder.")
        return

    files = get_excel_files(input_dir)
    if not files:
        print_error(f"No Excel files found in {input_dir.name}/")
        return

    print_step(f"Found {len(files)} file(s) in {input_dir.name}/")

    for f in files:
        df = read_excel(f)
        if df is None:
            continue
        _audit_file(df, f)

    print("\n" + "=" * 60)
    print_done("Audit complete.")