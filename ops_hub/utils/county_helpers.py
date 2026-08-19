"""
County coverage helpers
=======================
Match a fulfillment file to its client in the AA county master file (by domain
subdomain — an exact, unique key) and verify that every county listed in the
client's "Active Counties" is actually present in the fulfillment.

The master file lives in a Google Drive Desktop mount. If that mount is not
reachable, a local fallback folder is checked. If neither is available the
caller is expected to skip the check gracefully.
"""

import re
import pandas as pd
from pathlib import Path

from config import (
    COUNTY_MASTER_DRIVE, COUNTY_MASTER_LOCAL,
    COUNTY_MASTER_NAME_COL, COUNTY_MASTER_DOMAIN_COL, COUNTY_MASTER_COUNTIES_COL,
)


# ── Normalization ──────────────────────────────────────────────────────────────

def _norm(s) -> str:
    """Lowercase and strip everything except letters and digits."""
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def _subdomain(url) -> str | None:
    """Extract the subdomain from a domain URL: https://kentuckyrealestate.8020rei.com → 'kentuckyrealestate'."""
    if not isinstance(url, str):
        return None
    m = re.search(r"https?://([^.]+)\.", url.strip())
    return m.group(1).lower() if m else None


# ── Filename → client token ────────────────────────────────────────────────────

def extract_client_token(filename: str) -> str:
    """
    Pull the client identifier out of a fulfillment filename and normalize it
    to match a domain subdomain.

    '2026-08-10 KENTUCKYREALESTATE 37K Sms.xlsx' → 'kentuckyrealestate'
    """
    stem = Path(filename).stem
    stem = re.sub(r"^\d{4}-\d{2}-\d{2}\s*", "", stem)        # strip leading date
    stem = re.sub(r"\d+(\.\d+)?\s*k\b", "", stem, flags=re.I)  # strip size token e.g. 37K
    for cad in ("direct mail", "cold calling", "sms"):        # strip cadence words
        stem = re.sub(cad, "", stem, flags=re.I)
    return _norm(stem)


# ── Master file loading ────────────────────────────────────────────────────────

def load_master() -> tuple[pd.DataFrame | None, str]:
    """
    Load the most-recently-modified CSV from the Drive folder, or the local
    fallback folder if Drive is not reachable.

    Returns (DataFrame, source_label). DataFrame is None if no master found.
    """
    for folder, label in [(COUNTY_MASTER_DRIVE, "Google Drive"),
                          (COUNTY_MASTER_LOCAL, "local fallback")]:
        try:
            if not folder.exists():
                continue
            csvs = sorted(folder.glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
            if not csvs:
                continue
            df = pd.read_csv(csvs[0])
            return df, f"{label} ({csvs[0].name})"
        except Exception:
            continue
    return None, ""


def build_domain_index(master_df: pd.DataFrame) -> dict[str, pd.Series]:
    """Map each client's domain subdomain → its master row."""
    index: dict[str, pd.Series] = {}
    if COUNTY_MASTER_DOMAIN_COL not in master_df.columns:
        return index
    for _, row in master_df.iterrows():
        sub = _subdomain(row[COUNTY_MASTER_DOMAIN_COL])
        if sub:
            index[sub] = row
    return index


# ── Active counties parsing ────────────────────────────────────────────────────

def parse_active_counties(cell) -> set[str]:
    """
    Parse an 'Active Counties' cell into a set of normalized county names.

    'FAYETTE, KY\\nJESSAMINE, KY\\nBOURBON, KY' → {'FAYETTE', 'JESSAMINE', 'BOURBON'}

    Handles both literal backslash-n and real newlines as separators, and
    drops the trailing state code after the comma.
    """
    if not isinstance(cell, str):
        return set()
    entries = re.split(r"\\n|\n", cell)
    counties = set()
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        name = entry.split(",")[0].strip().upper()  # county name before the state
        if name:
            counties.add(name)
    return counties


# ── Coverage check ─────────────────────────────────────────────────────────────

def check_coverage(df: pd.DataFrame, filename: str,
                   domain_index: dict[str, pd.Series]) -> dict:
    """
    Compare a fulfillment file's counties against its client's active counties.

    Returns a dict with:
      matched        — bool, whether the client was matched
      client_name    — display name from master (if matched)
      token          — the normalized token extracted from the filename
      active         — set of active counties from master
      present        — active counties found in the file
      missing        — active counties NOT in the file  (the problem to surface)
      extra          — counties in the file but not in the client's active list
    """
    token  = extract_client_token(filename)
    result = {"matched": False, "client_name": None, "token": token,
              "active": set(), "present": set(), "missing": set(), "extra": set(),
              "county_counts": {}, "total": 0}

    row = domain_index.get(token)
    if row is None:
        return result

    result["matched"]     = True
    result["client_name"] = row.get(COUNTY_MASTER_NAME_COL)
    active = parse_active_counties(row.get(COUNTY_MASTER_COUNTIES_COL))
    result["active"] = active

    if "COUNTY" in df.columns:
        county_series = df["COUNTY"].dropna().astype(str).str.strip().str.upper()
        file_counties = set(county_series)
        result["county_counts"] = county_series.value_counts().to_dict()
        result["total"]         = int(len(county_series))
    else:
        file_counties = set()

    result["present"] = active & file_counties
    result["missing"] = active - file_counties
    result["extra"]   = file_counties - active
    return result
