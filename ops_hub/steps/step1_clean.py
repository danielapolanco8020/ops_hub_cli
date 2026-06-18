
import re
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict

from config import (
    INPUT_DIR, OUT_STEP1,
    UNWANTED_NAMES, INSTITUTIONAL_KEYWORDS,
    TAGS_ALL_CHANNELS, TAGS_DM_ONLY, TAGS_CC_SMS_ONLY, TAGS_CC_ONLY,
    TAGS_NEVER_FILTER,
    ADDRESS_VALIDATE_TYPES, ADDRESS_SKIP_TYPES, VALID_MAILING_PATTERNS,
)
from utils.file_helpers import (
    get_excel_files, read_excel, save_excel,
    prompt_yes_no, print_header, print_step, print_done,
    print_warn, print_error,
)


# ── Cadence detection ──────────────────────────────────────────────────────────

def _get_cadence(filename: str) -> str:
    name = filename.lower()
    if "direct mail" in name: return "dm"
    if "cold calling" in name: return "cc"
    if "sms" in name:          return "sms"
    return "unknown"


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


# ── Data quality flags helper ──────────────────────────────────────────────────

def _add_flag(df: pd.DataFrame, mask: pd.Series, flag: str) -> pd.DataFrame:
    if "data_quality_flags" not in df.columns:
        df["data_quality_flags"] = ""
    df.loc[mask, "data_quality_flags"] = df.loc[mask, "data_quality_flags"].apply(
        lambda x: f"{x}|{flag}" if x else flag
    )
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


def _filter_institutional_owners(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    pattern = re.compile("|".join(map(re.escape, INSTITUTIONAL_KEYWORDS)), re.IGNORECASE)
    mask    = df["OWNER FULL NAME"].str.contains(pattern, na=False)
    rej     = df[mask].copy()
    rej["Rejection_Stage"] = "Institutional Owner"
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


# ── Tag normalization & channel-aware filter ───────────────────────────────────

def _normalize_tag(tag: str) -> str:
    tag = tag.lower().strip()
    tag = re.sub(
        r'[-_\s]+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|'
        r'jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'[-_\s]*\d{0,4}', '', tag)
    tag = re.sub(r'[-_\s]+\d{1,2}[-_]\d{2,4}$', '', tag)
    tag = re.sub(r'[-_\s]+\d{4}$', '', tag)
    tag = re.sub(r'\s+list\s*$', '', tag)
    return tag.strip()


def _build_tag_list(cadence: str) -> list[str]:
    tags = list(TAGS_ALL_CHANNELS)
    if cadence == "dm":
        tags += TAGS_DM_ONLY
    elif cadence in ("cc", "sms"):
        tags += TAGS_CC_SMS_ONLY
        if cadence == "cc":
            tags += TAGS_CC_ONLY
    return [t.lower() for t in tags]


def _filter_tags(df: pd.DataFrame, cadence: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "TAGS" not in df.columns:
        return df, pd.DataFrame()

    tag_list   = _build_tag_list(cadence)
    never      = [t.lower() for t in TAGS_NEVER_FILTER]
    tag_counts: dict[str, int] = {}

    def _should_suppress(cell_value) -> str | None:
        if not isinstance(cell_value, str):
            return None
        tags = [t.strip() for t in cell_value.replace(";", ",").split(",")]
        for tag in tags:
            normalized = _normalize_tag(tag)
            if normalized in never:
                continue
            for suppressed in tag_list:
                if suppressed in normalized:
                    tag_counts[suppressed] = tag_counts.get(suppressed, 0) + 1
                    return tag
        return None

    matched = df["TAGS"].apply(_should_suppress)
    mask    = matched.notna()
    rej     = df[mask].copy()
    rej["Rejection_Stage"] = f"Blacklisted Tag ({cadence.upper()})"
    rej["Rejection_Value"] = matched[mask]

    if tag_counts:
        for tag, count in sorted(tag_counts.items(), key=lambda x: -x[1]):
            print(f"      {tag:<30}: {count:,}")

    return df[~mask], rej


# ── Name logic validation ──────────────────────────────────────────────────────

ROAD_KEYWORDS = {
    "rd", "ave", "blvd", "ln", "dr", "ct", "way", "pl", "tr", "rn", "ret",
    "hwy", "pkwy", "cir", "loop", "ter", "pass"
}


def _check_name_logic(row) -> str | None:
    full  = str(row.get("OWNER FULL NAME",  "") or "").strip()
    first = str(row.get("OWNER FIRST NAME", "") or "").strip()
    last  = str(row.get("OWNER LAST NAME",  "") or "").strip()

    if not full or not first or not last:
        return None

    if re.match(r'^\d+$', first):
        return f"First name is a number: '{first}'"

    if re.match(r'^[A-Za-z]\.?$', first):
        return f"First name is a single initial: '{first}'"

    last_words = {w.lower().rstrip(".") for w in last.split()}
    if last_words & ROAD_KEYWORDS:
        return f"Last name contains road keyword: '{last}'"

    full_words  = {w.lower().rstrip(".,") for w in full.split() if len(w) > 1}
    first_words = {w.lower().rstrip(".,") for w in first.split() if len(w) > 1}
    last_words2 = {w.lower().rstrip(".,") for w in last.split() if len(w) > 1}
    combined    = first_words | last_words2

    if combined and not (combined & full_words):
        return f"No match between '{first} {last}' and full name '{full}'"

    return None


def _filter_name_logic(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    required = {"OWNER FULL NAME", "OWNER FIRST NAME", "OWNER LAST NAME"}
    if not required.issubset(df.columns):
        return df, pd.DataFrame()

    df["OWNER FULL NAME ORIGINAL"] = df["OWNER FULL NAME"]

    issues = df.apply(_check_name_logic, axis=1)
    mask   = issues.notna()

    if not mask.any():
        return df, pd.DataFrame()

    flagged = df[mask].copy()
    flagged["Name_Issue"] = issues[mask]

    print_warn(f"  Found {mask.sum():,} rows with name logic issues. Sample:")
    for _, row in flagged.head(5).iterrows():
        print(f"    FULL: '{row['OWNER FULL NAME']}' → "
              f"FIRST: '{row['OWNER FIRST NAME']}' / "
              f"LAST: '{row['OWNER LAST NAME']}' — {row['Name_Issue']}")

    remove = prompt_yes_no(
        f"  Remove these {mask.sum():,} rows from output?",
        default=False
    )

    if remove:
        rej = flagged.copy()
        rej["Rejection_Stage"] = "Name Logic Issue"
        rej["Rejection_Value"] = rej["Name_Issue"]
        return df[~mask], rej
    else:
        df.loc[mask, "Name_Issue"] = issues[mask]
        df = _add_flag(df, mask, "wrong_owner")
        print_done(f"  {mask.sum():,} rows flagged in 'Name_Issue' column but kept.")
        return df, pd.DataFrame()


# ── Address validation ─────────────────────────────────────────────────────────

def _is_valid_mailing_format(address: str) -> bool:
    for pattern in VALID_MAILING_PATTERNS:
        if re.match(pattern, address, re.IGNORECASE):
            return True
    return False


def _validate_property_address(address: str) -> str | None:
    if not isinstance(address, str) or not address.strip():
        return "empty"
    addr = address.strip()
    if _is_valid_mailing_format(addr):
        return "po_box_or_rural_route_as_property_address"
    if re.match(r'^0\s+\w', addr):
        return "leading_zero_house_number"
    if re.match(r'^\d+$', addr):
        return "number_only_no_street"
    if not re.match(r'^\d', addr):
        return "no_leading_house_number"
    return None


def _validate_mailing_address(address: str) -> str | None:
    if not isinstance(address, str) or not address.strip():
        return "empty"
    addr = address.strip()
    if _is_valid_mailing_format(addr):
        return None
    if not re.match(r'^\d', addr) and not re.match(r'^[A-Z]{2,}\s+\d', addr):
        return "no_leading_house_number_or_junk"
    return None


def _run_address_validation(df: pd.DataFrame) -> pd.DataFrame:
    if "PROPERTY TYPE" in df.columns:
        prop_type_lower = df["PROPERTY TYPE"].astype(str).str.lower()
        skip_mask       = prop_type_lower.apply(
            lambda t: any(s in t for s in ADDRESS_SKIP_TYPES)
        )
        validate_idx = df[~skip_mask].index
    else:
        validate_idx = df.index

    prop_issues   = {}
    mail_issues   = {}
    prop_examples = {}
    mail_examples = {}

    if "ADDRESS" in df.columns:
        for idx in validate_idx:
            issue = _validate_property_address(df.at[idx, "ADDRESS"])
            if issue:
                prop_issues[issue] = prop_issues.get(issue, 0) + 1
                if issue not in prop_examples:
                    prop_examples[issue] = df.at[idx, "ADDRESS"]
                flag_mask = df.index == idx
                df = _add_flag(df, pd.Series(flag_mask, index=df.index),
                               "incomplete_property_address")

    if "MAILING ADDRESS" in df.columns:
        for idx in validate_idx:
            issue = _validate_mailing_address(df.at[idx, "MAILING ADDRESS"])
            if issue:
                mail_issues[issue] = mail_issues.get(issue, 0) + 1
                if issue not in mail_examples:
                    mail_examples[issue] = df.at[idx, "MAILING ADDRESS"]
                flag_mask = df.index == idx
                df = _add_flag(df, pd.Series(flag_mask, index=df.index),
                               "incomplete_mailing_address")

    total_prop = sum(prop_issues.values())
    total_mail = sum(mail_issues.values())

    if total_prop > 0:
        print_warn(f"  Property address issues: {total_prop:,} rows flagged")
        labels = {
            "no_leading_house_number":                   "No leading number    ",
            "leading_zero_house_number":                 "Leading zero         ",
            "po_box_or_rural_route_as_property_address": "PO Box / Rural Route ",
            "number_only_no_street":                     "Number only          ",
            "empty":                                     "Empty                ",
        }
        for key, label in labels.items():
            if prop_issues.get(key, 0) > 0:
                ex = prop_examples.get(key, "")
                print(f"      {label}: {prop_issues[key]:,} rows  (e.g. \"{ex}\")")

    if total_mail > 0:
        print_warn(f"  Mailing address issues: {total_mail:,} rows flagged")
        if mail_issues.get("empty", 0):
            print(f"      Empty                : {mail_issues['empty']:,} rows")
        if mail_issues.get("no_leading_house_number_or_junk", 0):
            ex = mail_examples.get("no_leading_house_number_or_junk", "")
            print(f"      Junk / no number     : {mail_issues['no_leading_house_number_or_junk']:,} rows  (e.g. \"{ex}\")")

    return df


# ── Absentee correction ────────────────────────────────────────────────────────

def _correct_absentee(df: pd.DataFrame) -> pd.DataFrame:
    required = {"ABSENTEE", "STATE", "MAILING STATE"}
    if not required.issubset(df.columns):
        return df

    df["ABSENTEE ORIGINAL"] = df["ABSENTEE"]

    corrected_to_2 = 0
    corrected_to_1 = 0
    null_count     = 0

    for idx in df.index:
        absentee   = df.at[idx, "ABSENTEE"]
        prop_state = str(df.at[idx, "STATE"]).strip().upper()
        mail_state = str(df.at[idx, "MAILING STATE"]).strip().upper()

        if pd.isna(absentee) or str(absentee).strip() == "":
            null_count += 1
            df = _add_flag(df, pd.Series(df.index == idx, index=df.index), "absentee_null")
            continue

        absentee = int(absentee)

        if absentee == 0:
            continue

        if absentee == 1 and prop_state != mail_state:
            df.at[idx, "ABSENTEE"] = 2
            corrected_to_2 += 1
            df = _add_flag(df, pd.Series(df.index == idx, index=df.index), "absentee_corrected")

        elif absentee == 2 and prop_state == mail_state:
            df.at[idx, "ABSENTEE"] = 1
            corrected_to_1 += 1
            df = _add_flag(df, pd.Series(df.index == idx, index=df.index), "absentee_corrected")

    if corrected_to_2 or corrected_to_1 or null_count:
        print_step("  Absentee correction:")
        if corrected_to_2:
            print_done(f"    1→2 (out-of-state) : {corrected_to_2:,} rows")
        if corrected_to_1:
            print_done(f"    2→1 (same state)   : {corrected_to_1:,} rows")
        if null_count:
            print_warn(f"    Null absentee      : {null_count:,} rows flagged")

    return df


# ── Geographic distribution ────────────────────────────────────────────────────

def _capture_distribution(df: pd.DataFrame, col: str) -> dict[str, float]:
    return df[col].value_counts(normalize=True).to_dict()


def _apply_distribution(df: pd.DataFrame, col: str,
                         distribution: dict[str, float], goal: int) -> pd.DataFrame:
    score_cols    = [c for c in ["BUYBOX SCORE", "LIKELY DEAL SCORE", "SCORE"] if c in df.columns]
    result_frames = []
    allocated     = 0
    areas         = list(distribution.keys())

    for i, area in enumerate(areas):
        area_df = df[df[col] == area].copy()
        if area_df.empty:
            continue
        n = goal - allocated if i == len(areas) - 1 else round(distribution[area] * goal)
        n = min(n, len(area_df))
        if score_cols:
            for c in score_cols:
                area_df[c] = pd.to_numeric(area_df[c], errors="coerce")
            area_df = area_df.sort_values(by=score_cols, ascending=[False] * len(score_cols))
        result_frames.append(area_df.head(n))
        allocated += n

    return pd.concat(result_frames, ignore_index=True) if result_frames else df


# ── Filename K-count update ────────────────────────────────────────────────────

def _update_filename_k(name: str, row_count: int) -> str:
    k_val    = row_count / 1000
    k_str    = f"{int(k_val)}K" if k_val == int(k_val) else f"{round(k_val, 1)}K"
    new_name = re.sub(r'\d+(\.\d+)?K', k_str, name)
    return new_name if new_name != name else name


# ── Output folder cleanup ──────────────────────────────────────────────────────

def _prompt_clear_output_folders():
    from config import OUTPUT_DIR
    print("\n  ⚠  Starting a new clean process.")
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

def _process_file(file: Path, output_dir: Path,
                  rejected_all: list[pd.DataFrame],
                  flagged_all:  list[pd.DataFrame]) -> dict:
    t0      = time.time()
    df      = read_excel(file)
    cadence = _get_cadence(file.name)

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
    geo_col = None; geo_distribution = {}; preserve_geo = False
    if {"COUNTY", "ZIP"}.intersection(df.columns):
        preserve_geo = prompt_yes_no(
            f"\n  [{file.name}] Preserve geographic distribution after cleaning?",
            default=False
        )
        if preserve_geo:
            choices = [c for c in ["COUNTY", "ZIP"] if c in df.columns]
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

    # ── Filters ────────────────────────────────────────────────────────────────
    print_step("Filters")

    if "ACTION PLANS" in df.columns:
        before = len(df); _apply(_filter_empty_action_plans, df)
        print_done(f"  Empty Action Plans       : {before - len(df):,} removed")

    if {"MAILING ADDRESS", "MAILING ZIP"}.issubset(df.columns):
        before = len(df); _apply(_filter_duplicates, df, ["MAILING ADDRESS", "MAILING ZIP"], "Duplicate Address")
        print_done(f"  Duplicate Address        : {before - len(df):,} removed")

    if {"OWNER FULL NAME", "ADDRESS", "ZIP"}.issubset(df.columns):
        before = len(df); _apply(_filter_duplicates, df, ["OWNER FULL NAME", "ADDRESS", "ZIP"], "Duplicate Owner")
        print_done(f"  Duplicate Owner          : {before - len(df):,} removed")

    if "OWNER FULL NAME" in df.columns:
        before = len(df); _apply(_filter_empty_owner_name, df)
        print_done(f"  Empty Owner Full Name    : {before - len(df):,} removed")

        before = len(df); _apply(_filter_unwanted_names, df)
        print_done(f"  Unwanted Names           : {before - len(df):,} removed")

        before = len(df); _apply(_filter_institutional_owners, df)
        print_done(f"  Institutional Owners     : {before - len(df):,} removed")

    if "TAGS" in df.columns:
        before = len(df); _apply(_filter_tags, df, cadence)
        print_done(f"  Blacklisted Tags ({cadence.upper():<3})   : {before - len(df):,} removed")

    # ── Name logic validation ──────────────────────────────────────────────────
    if {"OWNER FULL NAME", "OWNER FIRST NAME", "OWNER LAST NAME"}.issubset(df.columns):
        df, rej = _filter_name_logic(df)
        if not rej.empty:
            rej["Source_File"] = file.name
            rejects.append(rej)

    # ── Absentee same address — DM only, automatic ────────────────────────────
    if cadence == "dm" and {"ABSENTEE", "ADDRESS", "MAILING ADDRESS"}.issubset(df.columns):
        absentee_count = ((df["ABSENTEE"] >= 1) & (df["ADDRESS"] == df["MAILING ADDRESS"])).sum()
        if absentee_count > 0:
            before = len(df); _apply(_filter_absentee_same_address, df)
            print_done(f"  Absentee Same Address    : {before - len(df):,} removed (DM only)")

    # ── Address validation (flags only, no removal) ────────────────────────────
    df = _run_address_validation(df)

    # ── Absentee correction (all cadences) ────────────────────────────────────
    df = _correct_absentee(df)

    cleaned_rows = len(df)
    print_done(f"\n  Cleaned: {cleaned_rows:,} rows (from {original_rows:,})")

    # ── Data quality summary ───────────────────────────────────────────────────
    if "data_quality_flags" in df.columns:
        flags_present = df["data_quality_flags"][df["data_quality_flags"].astype(str) != ""]
        if not flags_present.empty:
            all_flags  = [f for cell in flags_present for f in str(cell).split("|") if f]
            flag_counts = Counter(all_flags)
            print_step("Data Quality Summary (flagged but kept):")
            for flag, count in sorted(flag_counts.items(), key=lambda x: -x[1]):
                print(f"      {flag:<40}: {count:,}")

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
            for area, pct in geo_distribution.items():
                area_count = len(df[df[geo_col] == area])
                print(f"      {area:<20}: {area_count:,} rows  ({pct*100:.1f}%)")
        else:
            df = df.head(goal)
            print_done(f"  Trimmed to {goal:,} rows keeping highest scores.")

    # ── Auto-update filename K-count ───────────────────────────────────────────
    updated_name = _update_filename_k(file.stem, len(df))
    out_path     = output_dir / f"cleaned_{updated_name}.xlsx"
    save_excel(df, out_path)
    print_done(f"  Saved → {out_path.name}")

    # ── Accumulate rejects ─────────────────────────────────────────────────────
    non_empty = [r for r in rejects if isinstance(r, pd.DataFrame) and not r.empty]
    if non_empty:
        combined_rej = pd.concat(non_empty, ignore_index=True)
        rejected_all.append(combined_rej)

    # ── Accumulate flagged rows ────────────────────────────────────────────────
    if "data_quality_flags" in df.columns:
        flagged = df[df["data_quality_flags"].astype(str).str.strip() != ""].copy()
        if not flagged.empty:
            flagged["Source_File"] = file.name
            flagged_all.append(flagged)

    elapsed = time.time() - t0
    return {
        "file":     file.name,
        "cadence":  cadence,
        "status":   "ok",
        "original": original_rows,
        "cleaned":  len(df),
        "rejected": original_rows - len(df),
        "time":     round(elapsed, 2),
    }


# ── Output Reports ─────────────────────────────────────────────────────────────

def _save_reports(rejected_all: list[pd.DataFrame],
                  flagged_all:  list[pd.DataFrame],
                  output_dir:   Path):

    frames = []

    # ── Rejected rows ──────────────────────────────────────────────────────────
    if rejected_all:
        all_rej = pd.concat([r for r in rejected_all if not r.empty], ignore_index=True)
        if not all_rej.empty:
            all_rej["Status"] = "Rejected"
            all_rej["Reason"] = (all_rej["Rejection_Stage"].astype(str)
                                 + " — " + all_rej["Rejection_Value"].astype(str))
            frames.append(all_rej)

            # Rejection summary
            summary = (
                all_rej.groupby(["Source_File", "Rejection_Stage"])
                .size()
                .reset_index(name="Rejected_Count")
            )
            save_excel(summary, output_dir / "Rejection_Summary.xlsx")
            print_done("Rejection summary saved → Rejection_Summary.xlsx")

    # ── Flagged rows ───────────────────────────────────────────────────────────
    if flagged_all:
        all_flagged = pd.concat([f for f in flagged_all if not f.empty], ignore_index=True)
        if not all_flagged.empty:
            all_flagged["Status"] = "Flagged"
            all_flagged["Reason"] = all_flagged["data_quality_flags"].astype(str)
            frames.append(all_flagged)

    # ── Combined Quality Report (overwritten each run) ─────────────────────────
    if frames:
        combined     = pd.concat(frames, ignore_index=True)
        priority_cols = ["Status", "Reason", "Source_File"]
        other_cols    = [c for c in combined.columns if c not in priority_cols]
        combined      = combined[priority_cols + other_cols]
        save_excel(combined, output_dir / "Quality_Report.xlsx")
        rejected_count = (combined["Status"] == "Rejected").sum()
        flagged_count  = (combined["Status"] == "Flagged").sum()
        print_done(f"Quality report saved → Quality_Report.xlsx  "
                   f"({rejected_count:,} rejected, {flagged_count:,} flagged)")

    # ── Cumulative Run Log (appends across runs) ───────────────────────────────
    if rejected_all:
        all_rej = pd.concat([r for r in rejected_all if not r.empty], ignore_index=True)
        if not all_rej.empty:
            run_log_path = output_dir / "Rejection_Run_Log.xlsx"
            all_rej["Run_Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_cols = ["Run_Timestamp", "Source_File", "Rejection_Stage",
                        "Rejection_Value", "OWNER FULL NAME", "ADDRESS", "ZIP",
                        "MAILING ADDRESS", "MAILING ZIP", "FOLIO"]
            log_cols_present = [c for c in log_cols if c in all_rej.columns]
            run_entry = all_rej[log_cols_present].copy()

            if run_log_path.exists():
                existing = pd.read_excel(run_log_path, engine="openpyxl")
                run_entry = pd.concat([existing, run_entry], ignore_index=True)

            save_excel(run_entry, run_log_path)
            print_done(f"Run log updated → Rejection_Run_Log.xlsx  "
                       f"({len(run_entry):,} total entries across all runs)")


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
    flagged_all:  list[pd.DataFrame] = []
    results = []

    for f in files:
        print_step(f"Processing: {f.name}")
        try:
            result = _process_file(f, OUT_STEP1, rejected_all, flagged_all)
        except Exception as e:
            import traceback
            traceback.print_exc()
            results.append({"file": f.name, "cadence": "unknown", "status": "error",
                            "original": 0, "cleaned": 0, "rejected": 0, "time": 0})
            continue
        results.append(result)

        if result["status"] == "ok":
            print_done(
                f"{result['file']} — "
                f"{result['original']:,} in, "
                f"{result['cleaned']:,} cleaned, "
                f"{result['rejected']:,} rejected "
                f"({result['time']}s)"
            )
        else:
            print_error(f"Failed to process {result['file']}")

    _save_reports(rejected_all, flagged_all, OUT_STEP1)

    # ── Final summary by channel ───────────────────────────────────────────────
    by_cadence: dict = defaultdict(lambda: {"in": 0, "cleaned": 0, "rejected": 0})
    for r in results:
        c = r.get("cadence", "unknown")
        by_cadence[c]["in"]       += r.get("original", 0)
        by_cadence[c]["cleaned"]  += r.get("cleaned",  0)
        by_cadence[c]["rejected"] += r.get("rejected", 0)

    print("\n" + "-" * 60)
    print("  FINAL SUMMARY BY CHANNEL")
    print("-" * 60)
    for cadence, counts in by_cadence.items():
        label = {"dm": "Direct Mail", "cc": "Cold Calling",
                 "sms": "SMS"}.get(cadence, cadence.upper())
        print(f"  {label:<15}: {counts['in']:>8,} in  →  "
              f"{counts['cleaned']:>8,} cleaned  ({counts['rejected']:,} rejected)")

    total_in  = sum(r.get("original", 0) for r in results)
    total_out = sum(r.get("cleaned",  0) for r in results)
    total_rej = sum(r.get("rejected", 0) for r in results)
    print("-" * 60)
    print(f"  {'TOTAL':<15}: {total_in:>8,} in  →  "
          f"{total_out:>8,} cleaned  ({total_rej:,} rejected)")
    print(f"  Output folder   : {OUT_STEP1}")
    print("-" * 60)