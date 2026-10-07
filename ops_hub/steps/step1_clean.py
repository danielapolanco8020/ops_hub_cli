import re
import time
import unicodedata
import pandas as pd
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict

from config import (
    INPUT_DIR, OUT_STEP1,
    UNWANTED_NAMES, INSTITUTIONAL_KEYWORDS,
    INSTITUTIONAL_QUALIFIED_KEYWORDS, INSTITUTIONAL_QUALIFIERS,
    STRONG_ENTITY_TOKENS, RESCUE_NAME_EXCEPTIONS, TRUSTEE_TOKENS, TRUST_NAME_TOKENS,
    ESTATE_OF_TOKENS,
    TAGS_ALL_CHANNELS, TAGS_DM_ONLY, TAGS_CC_SMS_ONLY, TAGS_CC_ONLY,
    TAGS_NEVER_FILTER,
    ADDRESS_VALIDATE_TYPES, ADDRESS_SKIP_TYPES, VALID_MAILING_PATTERNS,
    USPS_STATES, COUNTY_MASTER_LOCAL,
    OVERLAP_DAYS, OVERLAP_ALERT_PCT, OVERLAP_COLUMNS,
    STICKER_PRICE_COLUMNS, STICKER_PRICE_MIN_PCT,
)
from utils.file_helpers import (
    get_excel_files, read_excel, save_excel, find_column,
    prompt_yes_no, print_header, print_step, print_done,
    print_warn, print_error, detect_formula_and_hidden,
)
from utils.county_helpers import (
    load_master, build_domain_index, check_coverage,
)


# ── Console colors ─────────────────────────────────────────────────────────────
class C:
    RESET    = "\033[0m"
    GREEN    = "\033[92m"   # PASS / clean rows
    YELLOW   = "\033[93m"   # FLAGGED / warnings / kept with issue
    RED      = "\033[91m"   # REJECTED / removed
    BLUE     = "\033[94m"   # Info / neutral messages
    PURPLE   = "\033[95m"   # Case 3 name issues
    DARK_RED = "\033[31m"   # Case 4 name issues
    TEAL     = "\033[96m"   # 360 Fulfillment label
    BOLD     = "\033[1m"

def _color(text: str, color: str) -> str:
    return f"{color}{text}{C.RESET}"

def _pass(msg: str):
    print(f"  {_color('[PASS]', C.GREEN)} {msg}")

def _fail(msg: str):
    print(f"  {_color('[REMOVED]', C.RED)} {msg}")

def _flag(msg: str):
    print(f"  {_color('[FLAGGED]', C.YELLOW)} {msg}")


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
    """Append `flag` to data_quality_flags for every masked row in a single
    vectorized pass.

    Callers MUST pass one boolean mask covering the whole frame — never call this
    once per row in a loop. Building a full-length mask per row was the O(n²)
    hot spot that made Step 1 crawl on large files.
    """
    if "data_quality_flags" not in df.columns:
        df["data_quality_flags"] = ""
    if mask is None or not mask.any():
        return df
    existing = df.loc[mask, "data_quality_flags"].fillna("").astype(str)
    prefix   = existing.where(existing.eq(""), existing + "|")   # "" stays "", else "val|"
    df.loc[mask, "data_quality_flags"] = prefix + flag
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


# Vowels that make a name pronounceable. 'y' is included so real surnames whose
# only vowel is a 'y' (Lynn, Byrd, Flynn, Wynn) are never treated as junk.
_VOWELS = set("aeiouy")


def _is_consonant_only(value) -> bool:
    """True when a name has letters but not a single vowel — an unpronounceable,
    nonsensical string like 'N N V N' or 'Bcd Fgh'. Accents are stripped first
    (José → Jose) so accented vowels still count, and a name with no letters at
    all (blank / punctuation only) returns False — that is the empty-name filter's
    job, not this one."""
    norm    = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii").lower()
    letters = [c for c in norm if c.isalpha()]
    return bool(letters) and not any(c in _VOWELS for c in letters)


# Vowel-less entity suffixes are legitimate — a company abbreviated to initials plus
# such a suffix ("BCD LLC", "TRT LTD", "BB PLLC") is a real owner, not junk. These
# extend the shared STRONG_ENTITY_TOKENS (which lacks the vowel-less ones) purely for
# the consonant-only exemption below.
_CONSONANT_ENTITY_TOKENS = list(STRONG_ENTITY_TOKENS) + ["Ltd", "LLP", "PLLC", "PC", "PA", "LLLP"]


def _filter_consonant_only_names(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Drop rows whose OWNER FULL NAME is consonant-only (no vowels), e.g.
    'N N V N'. See _is_consonant_only for the exact rule.

    Legitimate entities are exempt: an initials-plus-suffix company name
    ("BCD LLC", "TRT LTD") is consonant-only but real, so a row is kept when its
    name carries an entity token OR OWNER TYPE is Company/Trust."""
    if "OWNER FULL NAME" not in df.columns:
        return df, pd.DataFrame()

    name = df["OWNER FULL NAME"]
    mask = name.apply(_is_consonant_only)
    if not mask.any():
        return df, pd.DataFrame()

    has_entity = name.str.contains(_wb_pattern(_CONSONANT_ENTITY_TOKENS), na=False)
    if "OWNER TYPE" in df.columns:
        owner_type     = df["OWNER TYPE"].astype(str).str.strip().str.lower()
        is_entity_type = owner_type.isin(["company", "trust"])
    else:
        is_entity_type = pd.Series(False, index=df.index)

    mask = mask & ~has_entity & ~is_entity_type
    if not mask.any():
        return df, pd.DataFrame()

    rej = df[mask].copy()
    rej["Rejection_Stage"] = "Consonant-Only Name"
    rej["Rejection_Value"] = rej["OWNER FULL NAME"]
    return df[~mask], rej


def _wb_pattern(tokens: list[str]) -> "re.Pattern":
    """Whole-word alternation, case-insensitive. A token matches only when it is
    delimited by whitespace, a string end, or punctuation OTHER THAN a hyphen —
    the hyphen counts as part of the word, so a keyword can never fire inside a
    larger token. This stops keywords colliding with surnames both when the letters
    are joined ("Gas" in "Vargas", "Bank" in "Banks") AND when they are hyphenated
    ("loan" in "Cam-loan", "Phuong-loan").

    Lookarounds are used instead of \\b because \\b treats a hyphen as a boundary,
    which would still wrongly match "loan" inside "Cam-loan"."""
    return re.compile(
        r"(?<![\w-])(?:" + "|".join(map(re.escape, tokens)) + r")(?![\w-])",
        re.IGNORECASE,
    )


def _filter_institutional_owners(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    name = df["OWNER FULL NAME"]

    # Strong keywords — a whole-word match alone flags the row as institutional.
    # This catches mislabeled non-sellers (school/church/utility) regardless of the
    # OWNER TYPE column, which is exactly what this filter is for.
    strong_mask = name.str.contains(_wb_pattern(INSTITUTIONAL_KEYWORDS), na=False)

    # Ambiguous keywords (Power/Temple/Church) are also common surnames, so they
    # only count as institutional when a supporting qualifier word is also present
    # ("Zion Temple", "Power Praise") — a bare "Betty J Temple" stays an individual.
    qualified_mask = (
        name.str.contains(_wb_pattern(INSTITUTIONAL_QUALIFIED_KEYWORDS), na=False)
        & name.str.contains(_wb_pattern(INSTITUTIONAL_QUALIFIERS), na=False)
    )

    mask = strong_mask | qualified_mask

    # ── Change 2 (last resort): OWNER TYPE tiebreaker ──────────────────────────
    # After whole-word matching, some genuine keywords still fire on real surnames
    # (e.g. "Parish", "Gas"). OWNER TYPE is only ~24% populated and is a weaker
    # signal, so it is consulted ONLY for rows still flagged above: keep the row
    # when OWNER TYPE is explicitly "Individual" AND the name carries no strong
    # entity token (LLC, Inc, Trust, Bank, …). Every other case — a blank/other
    # owner type, or an entity token present — stays rejected.
    if "OWNER TYPE" in df.columns and mask.any():
        owner_type    = df["OWNER TYPE"].astype(str).str.strip().str.lower()
        is_individual = owner_type == "individual"
        is_company    = owner_type == "company"
        has_entity    = name.str.contains(_wb_pattern(STRONG_ENTITY_TOKENS), na=False)

        # A religious/civic context word (e.g. "St", "Saint", "First", "Holy",
        # "Ministry") means the name is an institution even when OWNER TYPE says
        # Individual — "St Marcus Parish" is not a person's name. Common given names
        # that also live in the qualifier list (Grace, Faith, Jesus, …) are excluded
        # so a real person is not blocked from rescue by their own first name.
        context_tokens = [q for q in INSTITUTIONAL_QUALIFIERS
                          if q.lower() not in RESCUE_NAME_EXCEPTIONS]
        has_context    = name.str.contains(_wb_pattern(context_tokens), na=False)

        rescued_individual = mask & is_individual & ~has_entity & ~has_context

        # ── Company rescue — mirrors the Trustee/Trust rule ────────────────────
        # A whole-word institutional keyword ("Investments", "Capital", "Holdings"…)
        # often fires on a legitimate, correctly-typed company. Just as a trustee
        # token is kept only when OWNER TYPE == 'Trust', an institutional keyword is
        # overridden when OWNER TYPE == 'Company' AND the name carries a strong
        # company/entity token (LLC, Inc, Corp, Holdings, …): that is a real company
        # we still market to, not a mislabeled school/church/utility. A Company-typed
        # row WITHOUT any entity token (e.g. a bare "Riverside School") stays rejected.
        rescued_company = mask & is_company & has_entity

        rescued = rescued_individual | rescued_company
        if rescued.any():
            print_done(
                f"  Institutional Owner rescue: {rescued.sum():,} row(s) kept "
                f"({rescued_individual.sum():,} Individual, {rescued_company.sum():,} Company).")
        mask = mask & ~rescued

    rej  = df[mask].copy()
    rej["Rejection_Stage"] = "Institutional Owner"
    rej["Rejection_Value"] = rej["OWNER FULL NAME"]
    return df[~mask], rej


def _filter_trustee_tokens(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """A trustee-type token in OWNER FULL NAME (trustee, trustees, co-trustee, ttee,
    trs, successor trustee) — OR the bare word "trust" (e.g. "Smith Family Trust") —
    means the property is held in trust. The only valid reason to keep such a record
    is OWNER TYPE == 'Trust'; every other owner type — including a blank/unpopulated
    one — is dropped.

    This is the authoritative rule for trust/trustee tokens: "Trustee" is deliberately
    excluded from INSTITUTIONAL_KEYWORDS so this stage and the Institutional Owner
    stage cannot disagree. Tokens are matched as whole words (hyphen-aware), so
    "co-trustee" matches as a unit, "trs" never fires inside a larger word, and the
    bare "trust" never fires inside "trustee"."""
    if "OWNER FULL NAME" not in df.columns:
        return df, pd.DataFrame()

    name = df["OWNER FULL NAME"]
    has_trustee = name.str.contains(_wb_pattern(TRUSTEE_TOKENS), na=False)
    has_trust   = name.str.contains(_wb_pattern(TRUST_NAME_TOKENS), na=False)
    has_token = has_trustee | has_trust
    if not has_token.any():
        return df, pd.DataFrame()

    if "OWNER TYPE" in df.columns:
        is_trust = df["OWNER TYPE"].astype(str).str.strip().str.lower() == "trust"
    else:
        is_trust = pd.Series(False, index=df.index)

    drop_mask = has_token & ~is_trust
    if not drop_mask.any():
        return df, pd.DataFrame()

    rej = df[drop_mask].copy()
    # A trustee token takes precedence in the label; bare "trust" names (no trustee
    # token) get their own stage so the report shows the new rule's impact separately.
    stage = pd.Series("Trust (name) — non-Trust owner type", index=rej.index)
    stage[has_trustee[drop_mask]] = "Trustee — non-Trust owner type"
    rej["Rejection_Stage"] = stage
    rej["Rejection_Value"] = rej["OWNER FULL NAME"]
    return df[~drop_mask], rej


def _filter_company_with_first_name(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Company owner-type rule: a Company owner should have an EMPTY OWNER FIRST NAME
    (a company has a full name, not a first/last split). A Company-typed row with a
    populated OWNER FIRST NAME is mis-typed — usually an individual mislabeled as
    Company — and is DROPPED. Blank/whitespace/NaN counts as empty. No-op when the
    OWNER TYPE or OWNER FIRST NAME column is absent."""
    if not {"OWNER TYPE", "OWNER FIRST NAME"}.issubset(df.columns):
        return df, pd.DataFrame()

    is_company   = df["OWNER TYPE"].astype(str).str.strip().str.lower() == "company"
    first        = df["OWNER FIRST NAME"]
    first_filled = first.notna() & (first.astype(str).str.strip() != "")

    drop_mask = is_company & first_filled
    if not drop_mask.any():
        return df, pd.DataFrame()

    rej = df[drop_mask].copy()
    rej["Rejection_Stage"] = "Company owner with first name"
    rej["Rejection_Value"] = rej["OWNER FIRST NAME"]
    return df[~drop_mask], rej


def _filter_company_estate_of(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Company owner-type rule: "Estate of" / "Est of" in OWNER FULL NAME, OWNER LAST
    NAME, or OWNER FIRST NAME signals an estate-owned property mis-typed as Company,
    so the row is DROPPED. Phrases are matched whole-word (via _wb_pattern) across all
    three name columns. No-op when OWNER TYPE is absent."""
    if "OWNER TYPE" not in df.columns:
        return df, pd.DataFrame()

    name_cols = [c for c in ("OWNER FULL NAME", "OWNER LAST NAME", "OWNER FIRST NAME")
                 if c in df.columns]
    if not name_cols:
        return df, pd.DataFrame()

    is_company = df["OWNER TYPE"].astype(str).str.strip().str.lower() == "company"
    pattern    = _wb_pattern(ESTATE_OF_TOKENS)
    has_estate = pd.Series(False, index=df.index)
    for c in name_cols:
        has_estate |= df[c].astype(str).str.contains(pattern, na=False)

    drop_mask = is_company & has_estate
    if not drop_mask.any():
        return df, pd.DataFrame()

    rej = df[drop_mask].copy()
    rej["Rejection_Stage"] = "Estate of — Company owner type"
    rej["Rejection_Value"] = rej["OWNER FULL NAME"] if "OWNER FULL NAME" in rej.columns \
        else rej[name_cols[0]]
    return df[~drop_mask], rej


# Secondary unit / apartment designators. "#" is matched on its own (it is not a
# word character); the rest are matched as whole words so "Fl" won't hit a street
# name and "Lot" won't hit part of a word.
SFH_UNIT_PATTERN = re.compile(
    r"(?i)(?:#|\b(?:unit|apt|apartment|ste|suite|bldg|building|rm|room|"
    r"spc|space|trlr|trailer|lot|dept|fl|floor)\b)"
)


def _filter_sfh_with_unit(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """A single-family home (PROPERTY TYPE == "SFH") should not carry a unit or
    apartment designator in its address — such a row is usually a duplex or
    apartment mislabeled as SFH. Drop those rows."""
    if not {"PROPERTY TYPE", "ADDRESS"}.issubset(df.columns):
        return df, pd.DataFrame()

    is_sfh   = df["PROPERTY TYPE"].astype(str).str.strip().str.lower() == "sfh"
    has_unit = df["ADDRESS"].astype(str).str.contains(SFH_UNIT_PATTERN, na=False)
    mask     = is_sfh & has_unit

    rej = df[mask].copy()
    rej["Rejection_Stage"] = "SFH With Unit Number"
    rej["Rejection_Value"] = rej["ADDRESS"]
    return df[~mask], rej


# Street *prefix* words that must be followed by an actual street name. In these
# (mostly Spanish/Italian) formats the type word leads the street — "Via Del Mar",
# "Calle Ocho", "Camino Real" — so a house number followed by only the bare prefix
# ("1234 Via") is an incomplete address with the street name missing. Matched
# whole-word and case-insensitively; an optional trailing period is tolerated.
STREET_PREFIX_WORDS = [
    "via", "calle", "camino", "avenida", "paseo", "corte",
    "cerrada", "cuesta", "rancho", "plaza",
]
_STREET_PREFIX_ONLY_RE = re.compile(
    r'^\d+\s+(?:' + "|".join(STREET_PREFIX_WORDS) + r')\.?\s*$',
    re.IGNORECASE,
)


def _filter_street_prefix_only(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Drop rows whose ADDRESS is a house number followed by only a street-prefix
    word with no street name after it ("1234 Via", "890 Calle"). See
    STREET_PREFIX_WORDS / _STREET_PREFIX_ONLY_RE for the rule."""
    if "ADDRESS" not in df.columns:
        return df, pd.DataFrame()

    mask = df["ADDRESS"].astype(str).str.match(_STREET_PREFIX_ONLY_RE)
    if not mask.any():
        return df, pd.DataFrame()

    rej = df[mask].copy()
    rej["Rejection_Stage"] = "Street Prefix Without Name"
    rej["Rejection_Value"] = rej["ADDRESS"]
    return df[~mask], rej


def _filter_empty_action_plans(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # A blank-but-present cell ("" or whitespace) is just as empty as NaN, so strip
    # and test for emptiness — matching _filter_empty_owner_name. Checking only
    # notna() would let whitespace-only action plans survive a 360 run.
    mask = df["ACTION PLANS"].notna() & (df["ACTION PLANS"].astype(str).str.strip() != "")
    rej  = df[~mask].copy()
    rej["Rejection_Stage"] = "Empty Action Plans"
    rej["Rejection_Value"] = ""
    return df[mask], rej


def _filter_duplicates(df: pd.DataFrame, subset: list[str], stage: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_norm = df.copy()
    for col in subset:
        if col in df_norm.columns:
            df_norm[col] = df_norm[col].astype(str).str.strip().str.upper()

    duped_mask = df_norm.duplicated(subset=subset, keep="first")
    rej = df[duped_mask].copy()
    rej["Rejection_Stage"] = stage
    rej["Rejection_Value"] = rej.apply(
        lambda r: " | ".join(str(r[c]) for c in subset), axis=1
    )
    return df[~duped_mask], rej


def _filter_absentee_same_address(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Drop rows whose ABSENTEE value contradicts the property/mailing address:

      • ABSENTEE >= 1 (absentee owner) but ADDRESS == MAILING ADDRESS, and
      • ABSENTEE == 0 (non-absentee owner) but ADDRESS != MAILING ADDRESS.

    Both are the same kind of inconsistency and handled in one step. Addresses are
    compared normalized (trim + upper-case) so casing alone never counts as a
    difference. Blank mailing addresses were already backfilled from ADDRESS
    upstream, so a missing mailing address reads as "same" and never trips the
    ABSENTEE == 0 branch.
    """
    if not {"ABSENTEE", "ADDRESS", "MAILING ADDRESS"}.issubset(df.columns):
        return df, pd.DataFrame()

    absentee  = pd.to_numeric(df["ABSENTEE"], errors="coerce")
    addr      = df["ADDRESS"].astype(str).str.strip().str.upper()
    mail      = df["MAILING ADDRESS"].astype(str).str.strip().str.upper()
    same_addr = addr == mail

    same_mask = (absentee >= 1) & same_addr        # absentee but same address
    diff_mask = (absentee == 0) & ~same_addr       # non-absentee but different address
    drop_mask = same_mask | diff_mask
    if not drop_mask.any():
        return df, pd.DataFrame()

    rej = df[drop_mask].copy()
    rej["Rejection_Stage"] = same_mask[drop_mask].map(
        {True: "Absentee Same Address", False: "Non-Absentee Different Address"})
    rej["Rejection_Value"] = rej["ADDRESS"]
    return df[~drop_mask], rej


def _filter_invalid_state(df: pd.DataFrame, check_mailing: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    """V-01 — Discard records with an invalid USPS code.

    The property STATE is validated on every cadence (a bad property state is a
    genuine data-quality problem regardless of channel). The MAILING STATE is only
    validated when `check_mailing` is True — i.e. Direct Mail — because that is the
    only cadence we actually mail to, so an abroad/foreign mailing address should
    disqualify a row on DM but not on Cold Calling or SMS.
    """
    cols_to_check = [("STATE", "STATE")]
    if check_mailing:
        cols_to_check.append(("MAILING STATE", "MAILING STATE"))

    def _get_reason(row) -> str | None:
        for col, label in cols_to_check:
            if col not in row.index:
                continue
            val = str(row[col]).strip().upper()
            if val and val not in USPS_STATES:
                return f"{label} '{val}' is not a valid USPS state code"
        return None

    reasons = df.apply(_get_reason, axis=1)
    mask    = reasons.notna()
    rej     = df[mask].copy()
    rej["Rejection_Stage"] = "QUARANTINE-VALIDITY-INVALID_STATE"
    rej["Rejection_Value"] = reasons[mask]
    return df[~mask], rej


# ── Tag filtering ──────────────────────────────────────────────────────────────

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


# ── Invalid phone type (CC / SMS) ────────────────────────────────────────────────

# Phone types that can't be dialed/texted. SMS additionally rejects landlines
# (you can't text a landline). Kept in sync with the Step 3 audit's phone check.
INVALID_PHONE_TYPES_BASE = ["void", "null", "failed", "dnc"]
INVALID_PHONE_TYPES_SMS  = INVALID_PHONE_TYPES_BASE + ["landline"]


def _filter_invalid_phone_types(df: pd.DataFrame, cadence: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """CC/SMS only. Drop a row only when it has NO usable phone left — i.e. it has
    at least one phone whose TYPE is invalid (void/null/failed/dnc, plus landline
    for SMS) AND no phone with a valid type. Rows that still have a good number are
    kept (a single DNC number among valid ones does not disqualify the lead)."""
    if cadence not in ("cc", "sms"):
        return df, pd.DataFrame()

    type_cols = sorted(
        [c for c in df.columns if re.match(r"PHONE TYPE\s*\d+", c, re.IGNORECASE)],
        key=lambda x: int(re.search(r"\d+", x).group()),
    )
    if not type_cols:
        return df, pd.DataFrame()

    bad = INVALID_PHONE_TYPES_SMS if cadence == "sms" else INVALID_PHONE_TYPES_BASE
    bad_re = re.compile("|".join(bad), re.IGNORECASE)

    has_invalid = pd.Series(False, index=df.index)
    has_valid   = pd.Series(False, index=df.index)
    for col in type_cols:
        s        = df[col].astype(str).str.strip()
        nonempty = df[col].notna() & (s != "") & (s.str.lower() != "nan")
        is_bad   = nonempty & s.str.contains(bad_re, na=False)
        has_invalid |= is_bad
        has_valid   |= nonempty & ~is_bad

    drop_mask = has_invalid & ~has_valid
    if not drop_mask.any():
        return df, pd.DataFrame()

    rej = df[drop_mask].copy()
    rej["Rejection_Stage"] = "Invalid phone type"
    rej["Rejection_Value"] = rej[type_cols].apply(
        lambda row: ", ".join(str(v).strip() for v in row
                              if pd.notna(v) and str(v).strip()), axis=1)
    return df[~drop_mask], rej


# ── Name logic validation ──────────────────────────────────────────────────────

ROAD_KEYWORDS = {
    "rd", "ave", "blvd", "ln", "dr", "ct", "way", "pl", "tr", "rn", "ret",
    "hwy", "pkwy", "cir", "loop", "ter", "pass"
}

def _initial_matches_full_start(full: str, initial: str) -> bool:
    """Return True if `initial` (a single-letter first name) is the leading
    character of the full name, ignoring leading spaces and invisible characters.

    'R Thomas Navas' + 'R' → True    (initial leads the name → coherent split)
    'Ellen M Krause' + 'M' → False   (initial came from the middle → bad split)
    """
    letter = initial.strip().rstrip(".").lower()
    for ch in full:
        if ch.isspace() or not ch.isprintable():   # skip spaces + invisible chars
            continue
        return ch.lower() == letter
    return False


def _last_leads_full(full: str, last: str) -> bool:
    """Return True when OWNER LAST NAME is the leading run of tokens in
    OWNER FULL NAME — i.e. a coherent last-name-first record ("Smith W C" → last
    'Smith', "Reed A Louise" → last 'Reed'). In that layout a single-letter first
    name is a legitimate middle initial, not a bad mid-name split, so Case 2 should
    not fire. Genuine junk like "L W" (last 'W', full leads with 'L') still fails
    this test and stays flagged."""
    full_tokens = full.lower().split()
    last_tokens = last.lower().split()
    if not last_tokens or len(last_tokens) > len(full_tokens):
        return False
    return full_tokens[:len(last_tokens)] == last_tokens


def _name_logic_core(full_raw, first_raw, last_raw) -> tuple[int, str] | None:
    """Core name-logic check operating on three raw cell values. Called in a tight
    zip() loop by _filter_name_logic (~5x faster than df.apply(axis=1) on large
    files, byte-identical output) and via the _check_name_logic row wrapper below."""
    full  = str(full_raw  or "").strip()
    first = str(first_raw or "").strip()
    last  = str(last_raw  or "").strip()

    if not full or not first or not last:
        return None

    # Case 1 — First name is a number
    if re.match(r'^\d+', first):
        return (1, f"Case 1 — First name is a number: '{first}'")

    # Case 2 — First name is a single letter or initial.
    # A leading initial is a coherent split — "R Thomas Navas" → first 'R',
    # last 'Thomas Navas' — and is left alone. A last-name-first record is also
    # coherent — "Smith W C" → last 'Smith' leads the full name, so the 'W' is a
    # legitimate middle initial. It is only flagged when NEITHER the initial leads
    # the full name NOR the last name leads it, which means the split is genuinely
    # wrong (e.g. "Ellen M Krause" parsed to first 'M', or junk like "L W").
    if re.match(r'^[A-Za-z]\.?$', first):
        if not _initial_matches_full_start(full, first) and not _last_leads_full(full, last):
            return (2, f"Case 2 — Initial '{first}' is not the first name in '{full}'")

    # Case 3 — Last name contains road keywords (excluding St)
    last_words = {w.lower().rstrip(".") for w in last.split()}
    if last_words & ROAD_KEYWORDS:
        return (3, f"Case 3 — Last name contains road keyword: '{last}'")

    # Case 4 — No word from FIRST or LAST appears in FULL NAME
    full_words  = {w.lower().rstrip(".,") for w in full.split() if len(w) > 1}
    first_words = {w.lower().rstrip(".,") for w in first.split() if len(w) > 1}
    last_words2 = {w.lower().rstrip(".,") for w in last.split() if len(w) > 1}
    combined    = first_words | last_words2

    if combined and not (combined & full_words):
        return (4, f"Case 4 — No match between '{first} {last}' and full name '{full}'")

    return None


def _check_name_logic(row) -> tuple[int, str] | None:
    """Row-wise wrapper around _name_logic_core (kept for any row-based callers)."""
    return _name_logic_core(
        row.get("OWNER FULL NAME", ""),
        row.get("OWNER FIRST NAME", ""),
        row.get("OWNER LAST NAME", ""),
    )


def _filter_first_without_last(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Drop rows that have a first name but no last name — an incomplete owner
    split. Blank/whitespace counts as empty (not just NaN). MUST run before the
    OWNER LAST NAME ← OWNER FULL NAME backfill, which would otherwise hide the
    empty last name."""
    if not {"OWNER FIRST NAME", "OWNER LAST NAME"}.issubset(df.columns):
        return df, pd.DataFrame()
    first = df["OWNER FIRST NAME"]
    last  = df["OWNER LAST NAME"]
    first_filled = first.notna() & (first.astype(str).str.strip() != "")
    last_empty   = last.isna()  | (last.astype(str).str.strip() == "")
    mask = first_filled & last_empty
    if not mask.any():
        return df, pd.DataFrame()
    rej = df[mask].copy()
    rej["Rejection_Stage"] = "First Name Without Last Name"
    rej["Rejection_Value"] = rej["OWNER FIRST NAME"]
    return df[~mask], rej


def _filter_name_logic(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    required = {"OWNER FULL NAME", "OWNER FIRST NAME", "OWNER LAST NAME"}
    if not required.issubset(df.columns):
        return df, pd.DataFrame()

    df["OWNER FULL NAME ORIGINAL"] = df["OWNER FULL NAME"]

    results = pd.Series(
        [_name_logic_core(f, fi, la) for f, fi, la in
         zip(df["OWNER FULL NAME"], df["OWNER FIRST NAME"], df["OWNER LAST NAME"])],
        index=df.index,
    )

    case1 = results.apply(lambda x: x is not None and x[0] == 1)
    case2 = results.apply(lambda x: x is not None and x[0] == 2)
    case3 = results.apply(lambda x: x is not None and x[0] == 3)
    case4 = results.apply(lambda x: x is not None and x[0] == 4)

    if not (case1 | case2 | case3 | case4).any():
        return df, pd.DataFrame()

    drop_mask = pd.Series(False, index=df.index)

    def _show_samples(mask, color):
        for idx in df[mask].head(5).index:
            row = df.loc[idx]
            reason = results[idx][1]
            line = (f"    FULL: '{row['OWNER FULL NAME']}' → "
                    f"FIRST: '{row['OWNER FIRST NAME']}' / "
                    f"LAST: '{row['OWNER LAST NAME']}' — {reason}")
            print(f"  {_color(line, color)}")

    # Cases 1 and 4 — auto drop
    for mask, label in [
        (case1, "Case 1 — first name is a number"),
        (case4, "Case 4 — no name match in full name"),
    ]:
        if mask.any():
            print_warn(f"  {label}: {mask.sum():,} rows — auto-dropped. Sample:")
            _show_samples(mask, C.RED)
            drop_mask |= mask

    # Case 2 — single initial first name — optional prompt (unchanged)
    if case2.any():
        print_warn(f"  Case 2 — single initial first name: {case2.sum():,} rows. Sample:")
        _show_samples(case2, C.YELLOW)
        if prompt_yes_no(f"  Drop these {case2.sum():,} rows?", default=False):
            drop_mask |= case2
        else:
            df.loc[case2, "Name_Issue"] = results[case2].apply(lambda x: x[1])
            df = _add_flag(df, case2, "wrong_owner")
            print_done(f"  {case2.sum():,} rows flagged but kept.")

    # Case 3 — road keyword in last name — decided by OWNER TYPE.
    #   A last name containing a road/trust token (e.g. "Tr", "Ret", "Ave") is
    #   ambiguous: it can be a legitimate Trust ("Smith Family Ret") or Company
    #   ("Danbury Rd Realty Llc") whose entity name merely looks address-like, or a
    #   genuine street dumped into an individual's name field. We defer to OWNER TYPE:
    #     • OWNER TYPE in ("Trust", "Company") → legitimate owner, KEEP (not dropped/flagged)
    #     • any other owner type (Individual/Estate/…) → DROP as a Name Logic Issue
    if case3.any():
        # Rescue for real people whose surname is a road word ("William E Way",
        # "Jo Ann M Loop"). Runs before the rejection is finalized. Keep the row when
        # BOTH hold:
        #   1. OWNER FIRST NAME is present (not blank/NaN), and
        #   2. lowercase OWNER FIRST NAME equals the lowercase first word of
        #      OWNER FULL NAME — confirms the name starts with a real given name.
        # Otherwise the row is rejected exactly as before.
        first_raw     = df["OWNER FIRST NAME"]
        first_present = first_raw.notna() & (first_raw.astype(str).str.strip() != "")
        # Rescue when a real (2+ letter) word of OWNER FIRST NAME appears ANYWHERE in
        # OWNER FULL NAME — this confirms a genuine given name is present and covers
        # BOTH orderings: "Robert B Way" (given name leads) AND "Way Jeremy J"
        # (surname leads). The previous check only matched the leading token, so
        # last-first rows like "Way Jeremy J" were wrongly dropped even though
        # "Jeremy" is clearly a real first name.
        full_tokens   = df["OWNER FULL NAME"].astype(str).str.lower().str.findall(r"[a-z]{2,}")
        first_tokens  = first_raw.astype(str).str.lower().str.findall(r"[a-z]{2,}")
        # str.findall returns a list per string cell but a scalar NaN for any NA cell.
        # On pandas 3.0 the new default string dtype keeps blanks as NA through
        # astype(str) (2.x turned them into the literal "nan"), so NA reaches findall
        # and comes back as a float. Guard with _tokset so a non-list never reaches
        # set(), which would raise "TypeError: 'float' object is not iterable".
        def _tokset(v):
            return set(v) if isinstance(v, list) else set()
        first_in_full = pd.Series(
            [bool(_tokset(ft) & _tokset(fut)) for ft, fut in zip(first_tokens, full_tokens)],
            index=df.index,
        )
        case3_rescue  = case3 & first_present & first_in_full

        if "OWNER TYPE" in df.columns:
            owner_type   = df["OWNER TYPE"].astype(str).str.strip().str.lower()
            is_entity    = owner_type.isin(["trust", "company"])
            case3_keep   = case3 & (is_entity | case3_rescue)
            case3_drop   = case3 & ~case3_keep
            rescued_only = case3_rescue & ~is_entity
            print_warn(f"  Case 3 — road keyword in last name: {case3.sum():,} rows "
                       f"→ {(case3 & is_entity).sum():,} Trust/Company kept, "
                       f"{rescued_only.sum():,} rescued (real given name), "
                       f"{case3_drop.sum():,} dropped.")
            if case3_drop.any():
                _show_samples(case3_drop, C.PURPLE)
            drop_mask |= case3_drop
        else:
            # No OWNER TYPE column: previous behavior kept every Case 3 row, so nothing
            # is dropped here and the rescue is a no-op.
            print_warn(f"  Case 3 — road keyword in last name: {case3.sum():,} rows, "
                       f"but no OWNER TYPE column — keeping all (cannot classify).")

    if not drop_mask.any():
        return df, pd.DataFrame()

    rej = df[drop_mask].copy()
    rej["Name_Issue"] = results[drop_mask].apply(lambda x: x[1])
    rej["Rejection_Stage"] = "Name Logic Issue"
    rej["Rejection_Value"] = rej["Name_Issue"]
    return df[~drop_mask], rej


# ── Vacant lot filter ──────────────────────────────────────────────────────────

def _filter_vacant_lots(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "ADDRESS" not in df.columns:
        return df, pd.DataFrame()

    mask = df["ADDRESS"].astype(str).str.match(r"^0\s")

    if not mask.any():
        return df, pd.DataFrame()

    print_warn(f"  Vacant lot suspected (leading-zero address): {mask.sum():,} rows. Sample:")
    for idx in df[mask].head(5).index:
        addr = df.at[idx, "ADDRESS"]
        print(f"  {_color(f'    ADDRESS: {addr!r}', C.YELLOW)}")

    if prompt_yes_no(f"  Drop these {mask.sum():,} rows?", default=False):
        rej = df[mask].copy()
        rej["Rejection_Stage"] = "Vacant Lot"
        rej["Rejection_Value"] = rej["ADDRESS"]
        return df[~mask], rej

    df = _add_flag(df, mask, "vacant_lot_suspected")
    print_done(f"  {mask.sum():,} rows flagged but kept.")
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


def _filter_pobox_property_address(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Drop rows whose property ADDRESS is a mailing-only format — PO Box, Rural
    Route, HC/HCR, PSC/Unit/CMO, or General Delivery (the VALID_MAILING_PATTERNS).
    None of these is a physical property location. The identical formats stay
    VALID for MAILING ADDRESS, which this filter never touches. Applies on every
    cadence and regardless of property type (a PO box is never a property)."""
    if "ADDRESS" not in df.columns:
        return df, pd.DataFrame()
    addr = df["ADDRESS"].astype(str).str.strip()
    mask = addr.apply(lambda s: bool(s) and _is_valid_mailing_format(s))
    if not mask.any():
        return df, pd.DataFrame()
    rej = df[mask].copy()
    rej["Rejection_Stage"] = "PO Box / Rural Route as Property Address"
    rej["Rejection_Value"] = rej["ADDRESS"]
    return df[~mask], rej


def _run_address_validation(df: pd.DataFrame) -> pd.DataFrame:
    if "PROPERTY TYPE" in df.columns:
        prop_type_lower = df["PROPERTY TYPE"].astype(str).str.lower()
        skip_mask       = prop_type_lower.apply(
            lambda t: isinstance(t, str) and any(s in t for s in ADDRESS_SKIP_TYPES)
        )
        validate_mask = ~skip_mask
    else:
        validate_mask = pd.Series(True, index=df.index)

    def _validate_column(col: str, validator, flag: str) -> tuple[dict, dict]:
        """Validate one address column across all non-skipped rows in a single
        pass, flag every offending row at once, and return
        (issue_counts, first_example_per_issue) — matching the previous per-row
        tallies and 'first occurrence in row order' example selection."""
        nonlocal df
        issues = pd.Series(index=df.index, dtype=object)
        issues.loc[validate_mask] = df.loc[validate_mask, col].apply(validator)

        issue_mask = issues.notna()
        if issue_mask.any():
            df = _add_flag(df, issue_mask, flag)

        counts   = issues[issue_mask].value_counts().to_dict()
        examples = {}
        for key in counts:
            first_idx     = issues.index[issues == key][0]
            examples[key] = df.at[first_idx, col]
        return counts, examples

    prop_issues, prop_examples = ({}, {})
    mail_issues, mail_examples = ({}, {})

    if "ADDRESS" in df.columns:
        prop_issues, prop_examples = _validate_column(
            "ADDRESS", _validate_property_address, "incomplete_property_address")

    if "MAILING ADDRESS" in df.columns:
        mail_issues, mail_examples = _validate_column(
            "MAILING ADDRESS", _validate_mailing_address, "incomplete_mailing_address")

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

    prop_state    = df["STATE"].astype(str).str.strip().str.upper()
    mail_state    = df["MAILING STATE"].astype(str).str.strip().str.upper()
    states_differ = prop_state != mail_state

    absentee_num = pd.to_numeric(df["ABSENTEE"], errors="coerce")
    is_null      = df["ABSENTEE"].isna() | (df["ABSENTEE"].astype(str).str.strip() == "")

    # 1 → 2 : marked in-state but property/mailing states differ (really out of state)
    to_2 = (~is_null) & (absentee_num == 1) & states_differ
    # 2 → 1 : marked out-of-state but the states actually match
    to_1 = (~is_null) & (absentee_num == 2) & (~states_differ)

    df.loc[to_2, "ABSENTEE"] = 2
    df.loc[to_1, "ABSENTEE"] = 1

    corrected = to_2 | to_1
    if corrected.any():
        df = _add_flag(df, corrected, "absentee_corrected")
    if is_null.any():
        df = _add_flag(df, is_null, "absentee_null")

    corrected_to_2 = int(to_2.sum())
    corrected_to_1 = int(to_1.sum())
    null_count     = int(is_null.sum())

    if corrected_to_2 or corrected_to_1 or null_count:
        print_step("  Absentee correction:")
        if corrected_to_2:
            print_done(f"    1→2 (out-of-state) : {corrected_to_2:,} rows")
        if corrected_to_1:
            print_done(f"    2→1 (same state)   : {corrected_to_1:,} rows")
        if null_count:
            print_warn(f"    Null absentee      : {null_count:,} rows flagged")

    return df


# ── Pre-foreclosure correction ─────────────────────────────────────────────────

def _correct_preforeclosure(df: pd.DataFrame) -> pd.DataFrame:
    if "PRE-FORECLOSURE" not in df.columns:
        return df

    df["PRE-FORECLOSURE"] = pd.to_numeric(df["PRE-FORECLOSURE"], errors="coerce")
    vals = df["PRE-FORECLOSURE"]

    # 1) Validate FIRST, before any conversion: the only allowed values are
    #    0, 0.8 and 1. Flag any other non-blank value (e.g. 0.5, 0.9, 2). A
    #    blank / non-numeric cell coerces to NaN and is treated as missing, not
    #    invalid, so it is left alone.
    invalid   = vals.notna() & ~vals.isin([0, 0.8, 1])
    inv_count = int(invalid.sum())
    if inv_count:
        df = _add_flag(df, invalid, "preforeclosure_invalid_value")

    # 2) THEN convert 0.8 → 1 (the historical normalization). Valid 0.8 rows are
    #    not flagged by step 1, so the two rules coexist in one pass.
    mask  = df["PRE-FORECLOSURE"] == 0.8
    count = int(mask.sum())
    if count:
        df.loc[mask, "PRE-FORECLOSURE"] = 1
        df = _add_flag(df, mask, "preforeclosure_corrected")

    if inv_count or count:
        print_step("  Pre-foreclosure:")
        if inv_count:
            print_warn(f"    Invalid value (not 0/0.8/1): {inv_count:,} rows flagged")
        if count:
            print_done(f"    0.8→1 corrected            : {count:,} rows flagged")

    return df


# ── Duplicate phone numbers across owners ───────────────────────────────────────

def _flag_duplicate_phone_numbers(df: pd.DataFrame) -> pd.DataFrame:
    """Flag (keep) rows whose phone number is shared across MORE THAN ONE distinct
    owner — i.e. a number that is not unique to a single owner (often a bad
    skiptrace hit or a shared/relative line). The same number repeated across one
    owner's multiple properties is fine. Pools every PHONE NUMBER N column,
    normalized to digits only; numbers shorter than 7 digits are ignored as junk.
    Flag only — nothing is removed — and the count is printed for the user."""
    num_cols = sorted(
        [c for c in df.columns if re.match(r"PHONE NUMBER\s*\d+", c, re.IGNORECASE)],
        key=lambda x: int(re.search(r"\d+", x).group()),
    )
    if not num_cols or "OWNER FULL NAME" not in df.columns:
        return df

    owner = df["OWNER FULL NAME"].astype(str).str.strip().str.upper()

    # Long form: one (row index, owner, phone-digits) per non-empty phone cell.
    frames = []
    for col in num_cols:
        digits = df[col].astype(str).str.replace(r"\D+", "", regex=True)
        frames.append(pd.DataFrame({"idx": df.index, "owner": owner.values,
                                    "phone": digits.values}))
    long = pd.concat(frames, ignore_index=True)
    long = long[long["phone"].str.len() >= 7]
    if long.empty:
        return df

    # A phone is "not unique to an owner" when it maps to >1 distinct owner.
    owners_per_phone = long.groupby("phone")["owner"].nunique()
    shared_phones    = set(owners_per_phone[owners_per_phone > 1].index)

    print_step("  Duplicate phone number check:")
    if not shared_phones:
        print_done("    No phone numbers shared across owners.")
        return df

    rows_with_shared = set(long.loc[long["phone"].isin(shared_phones), "idx"])
    mask = df.index.to_series().isin(rows_with_shared)
    n    = int(mask.sum())

    df = _add_flag(df, mask, "duplicate_phone_number")
    print_warn(f"    {n:,} propert{'y' if n == 1 else 'ies'} share a phone number with "
               f"another owner ({len(shared_phones):,} distinct number(s)) — "
               "flagged 'duplicate_phone_number'.")
    return df


def _flag_shared_mailing_address(df: pd.DataFrame) -> pd.DataFrame:
    """Flag (keep) rows whose MAILING ADDRESS is shared across MORE THAN ONE distinct
    OWNER FULL NAME — the same mailbox tied to different owners (a mail house, family
    member, registered agent, or a data error). The same owner reusing one mailing
    address across their own properties is fine and never flagged. Grouped on MAILING
    ADDRESS + MAILING ZIP (normalized trim/upper) when the zip is present, else on
    MAILING ADDRESS alone; blank mailing addresses are ignored.

    CC/SMS only — these rows are KEPT and flagged 'Multiple Owner mailing address'
    (the contact channel is a phone, not the mailbox). DM drops them instead via the
    owner-agnostic Duplicate Address filter."""
    if "MAILING ADDRESS" not in df.columns or "OWNER FULL NAME" not in df.columns:
        return df

    mail  = df["MAILING ADDRESS"].astype(str).str.strip().str.upper()
    owner = df["OWNER FULL NAME"].astype(str).str.strip().str.upper()
    if "MAILING ZIP" in df.columns:
        key = mail + " | " + df["MAILING ZIP"].astype(str).str.strip().str.upper()
    else:
        key = mail

    valid = mail.ne("") & ~mail.isin(["NAN", "NONE", "<NA>"])
    grp   = pd.DataFrame({"key": key, "owner": owner})[valid]
    if grp.empty:
        return df

    owners_per_key = grp.groupby("key")["owner"].nunique()
    shared_keys    = set(owners_per_key[owners_per_key > 1].index)

    print_step("  Shared mailing address check:")
    if not shared_keys:
        print_done("    No mailing address shared across different owners.")
        return df

    mask = valid & key.isin(shared_keys)
    n    = int(mask.sum())
    df   = _add_flag(df, mask, "Multiple Owner mailing address")
    print_warn(f"    {n:,} propert{'y' if n == 1 else 'ies'} share a mailing address with "
               f"another owner ({len(shared_keys):,} distinct address(es)) — "
               "flagged 'Multiple Owner mailing address'.")
    return df


def _normalize_county(s: pd.Series) -> pd.Series:
    """Normalize a COUNTY column for comparison: lower-case, drop the administrative
    suffix (County/Parish/Borough/Census Area/Municipality), remove punctuation and
    apostrophes, collapse whitespace — so "Prince George's County" and
    "prince georges" compare equal."""
    s = s.astype(str).str.strip().str.lower()
    s = s.str.replace(r"\b(county|parish|borough|census area|municipality)\b", "", regex=True)
    s = s.str.replace(".", "", regex=False).str.replace("'", "", regex=False).str.replace("-", " ", regex=False)
    s = s.str.replace(r"\s+", " ", regex=True).str.strip()
    return s


def _flag_zip_county_review(df: pd.DataFrame) -> pd.DataFrame:
    """Flag-only "look here" check: flag every row whose ZIP maps to MORE THAN ONE
    distinct COUNTY within this file — a cue to eyeball possible ZIP/county data-entry
    errors. County names are normalized (see _normalize_county) so casing/suffix noise
    is not counted as a difference.

    IMPORTANT: a ZIP can legitimately straddle county lines, so this is a REVIEW
    signal, NOT a rejection — nothing is removed. No-op when ZIP or COUNTY is absent."""
    if not {"ZIP", "COUNTY"}.issubset(df.columns):
        return df

    z = df["ZIP"].astype(str).str.replace(r"\D", "", regex=True).str[:5]
    c = _normalize_county(df["COUNTY"])
    valid = z.str.len() == 5

    pairs = pd.DataFrame({"z": z, "c": c})[valid]
    if pairs.empty:
        return df

    counties_per_zip = pairs.groupby("z")["c"].nunique()
    multi_zips       = set(counties_per_zip[counties_per_zip > 1].index)

    print_step("  ZIP / county consistency check:")
    if not multi_zips:
        print_done("    Every ZIP maps to a single county.")
        return df

    mask = valid & z.isin(multi_zips)
    n    = int(mask.sum())
    df   = _add_flag(df, mask, "ZIP in multiple counties")
    print_warn(f"    {n:,} row(s) have a ZIP mapping to >1 county in this file "
               f"({len(multi_zips):,} such ZIP(s)) — flagged 'ZIP in multiple counties' "
               "for review (a ZIP can legitimately span counties; not a rejection).")
    return df


def _check_sticker_price(df: pd.DataFrame, cadence: str) -> pd.DataFrame:
    """DM-only report: what share of rows carry a NUMERIC sticker price (the cash-offer
    figure printed on the postcard). The percentage is ALWAYS printed; when it falls
    below STICKER_PRICE_MIN_PCT a red console alert is raised. Flag-only — nothing is
    removed. Currency formatting ($, commas, spaces) is stripped before the numeric
    test, so "$120,000" counts as numeric; blanks and non-numeric text do not. No-op
    on non-DM cadences or when no sticker-price column is found."""
    if cadence != "dm":
        return df

    col = find_column(df, STICKER_PRICE_COLUMNS)
    if col is None:
        print_warn(f"  Sticker price check skipped — none of {STICKER_PRICE_COLUMNS} found.")
        return df

    total = len(df)
    if total == 0:
        return df

    cleaned = df[col].astype(str).str.replace(r"[,$\s]", "", regex=True)
    numeric = pd.to_numeric(cleaned, errors="coerce").notna()
    n       = int(numeric.sum())
    pct     = n / total

    print_step(f"Sticker Price Check ({col})")
    print(f"    {n:,} of {total:,} rows have a numeric sticker price  ({pct*100:.1f}%)")
    if pct < STICKER_PRICE_MIN_PCT:
        alert = (f"⚠  STICKER PRICE ALERT: only {pct*100:.1f}% of rows have a numeric "
                 f"sticker price — below the {STICKER_PRICE_MIN_PCT*100:.0f}% threshold "
                 f"({total - n:,} row(s) missing a numeric value in '{col}').")
        print(f"  {_color(alert, C.RED)}")
    return df


# ── Overlap check ──────────────────────────────────────────────────────────────

def _check_overlap(df: pd.DataFrame, cadence: str) -> pd.DataFrame:
    """Flag-only, cadence-aware check.

    Using the cadence's "Last recommendation" column, count the properties whose
    last recommendation falls within OVERLAP_DAYS of the run date (day difference
    from today back to the recommendation date < OVERLAP_DAYS). That share of the
    file is the overlap. When it exceeds OVERLAP_ALERT_PCT, every overlapping
    property is flagged ("overlapping") and a console alert is printed.

    A blank/invalid date is treated as non-overlapping; a future date is ignored
    (not counted). If the cadence column is missing, the check notifies and skips.

    "30 DAYS" action-plan properties are exempt: they are meant to be in every
    list regardless of last-recommendation date, so they are never flagged and
    are excluded from the overlap percentage's denominator entirely.
    """
    col_name = OVERLAP_COLUMNS.get(cadence)
    col      = find_column(df, [col_name]) if col_name else None
    if col is None:
        label = f"'{col_name}'" if col_name else f"cadence '{cadence}'"
        print_warn(f"  Overlap check skipped — column {label} not found in this file.")
        return df

    total = len(df)
    if total == 0:
        return df

    today = pd.Timestamp.now().normalize()
    dates = pd.to_datetime(df[col], errors="coerce").dt.normalize()
    diff  = (today - dates).dt.days

    # "30 DAYS" action plans belong in every list by design, so they are never
    # overlap no matter when they were last recommended — exclude them from both
    # the flag and the overlap population (the percentage denominator).
    if "ACTION PLANS" in df.columns:
        exempt = df["ACTION PLANS"].astype(str).str.strip().str.upper().eq("30 DAYS")
    else:
        exempt = pd.Series(False, index=df.index)

    considered    = int((~exempt).sum())
    overlap_mask  = dates.notna() & (diff >= 0) & (diff < OVERLAP_DAYS) & ~exempt
    overlap_count = int(overlap_mask.sum())
    pct           = overlap_count / considered if considered else 0.0

    print_step(f"Overlap Check ({col})")
    print(f"    {overlap_count:,} of {considered:,} properties recommended within "
          f"{OVERLAP_DAYS} days  ({pct*100:.1f}% overlap)")
    exempt_count = int(exempt.sum())
    if exempt_count:
        print(f"    {exempt_count:,} '30 DAYS' action-plan properties excluded "
              "from the overlap check (always included).")

    if pct > OVERLAP_ALERT_PCT:
        df = _add_flag(df, overlap_mask, "overlapping")
        alert = (f"⚠  OVERLAP ALERT: {overlap_count:,} of {considered:,} properties "
                 f"({pct*100:.1f}%) recommended within {OVERLAP_DAYS} days — "
                 f"exceeds {OVERLAP_ALERT_PCT*100:.0f}% threshold. Properties flagged 'overlapping'.")
        print(f"  {_color(alert, C.RED)}")

    return df


# ── Geographic distribution ────────────────────────────────────────────────────

def _capture_distribution(df: pd.DataFrame, col: str) -> dict[str, float]:
    return df[col].value_counts(normalize=True).to_dict()


def _apply_distribution(df: pd.DataFrame, col: str,
                         distribution: dict[str, float], goal: int) -> pd.DataFrame:
    score_cols    = [c for c in ["SCORE", "LIKELY DEAL SCORE", "BUYBOX SCORE"] if c in df.columns]
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


# ── Run type prompt ────────────────────────────────────────────────────────────

def _prompt_run_type() -> str:
    """Ask user if this is a 360 Fulfillment or Manual Pull. Default is 360."""
    print(f"\n  {_color('Select run type:', C.BOLD)}")
    print(f"    1. {_color('360 Fulfillment', C.TEAL)} (default) — full cleaning including Action Plans filter")
    print(f"    2. {_color('Manual Pull', C.YELLOW)} — skips Action Plans filter, keeps properties without an action plan")
    while True:
        raw = input("  Enter choice [default: 1]: ").strip()
        if raw == "" or raw == "1":
            print_done(f"  Run type: {_color('360 Fulfillment', C.TEAL)}")
            return "360"
        elif raw == "2":
            print_done(f"  Run type: {_color('Manual Pull', C.YELLOW)}")
            return "manual"
        else:
            print("  Enter 1 or 2.")


# ── County coverage check ──────────────────────────────────────────────────────

def _report_county_coverage(df: pd.DataFrame, filename: str, domain_index: dict) -> dict:
    """Run the active-counties coverage check for one file and print the result."""
    result = check_coverage(df, filename, domain_index)

    print_step("County Coverage Check")
    if not result["matched"]:
        print_warn(f"  No master match for client token '{result['token']}' — county check skipped.")
        return result

    print(f"    Client        : {result['client_name']}")
    print(f"    Active counties: {len(result['active'])}  |  Present: {len(result['present'])}")

    if result["missing"]:
        print(f"  {_color('[FAIL]', C.RED)} Missing counties in fulfillment: "
              f"{', '.join(sorted(result['missing']))}")
    else:
        print(f"  {_color('[PASS]', C.GREEN)} All active counties present in fulfillment.")

    if result["extra"]:
        print(f"  {_color('[INFO]', C.BLUE)} Counties in file not on active list: "
              f"{', '.join(sorted(result['extra']))}")

    return result


# ── Output folder cleanup ──────────────────────────────────────────────────────

def _prompt_clear_output_folders():
    from config import OUTPUT_DIR
    print("\n  ⚠  Starting a new clean process.")
    folders = [f for f in OUTPUT_DIR.rglob("*.xlsx")
               if f.name not in ("Rejection_Run_Log.xlsx", "Rejection_Summary.xlsx")]
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
                  flagged_all:  list[pd.DataFrame],
                  run_type:     str = "360",
                  domain_index: dict | None = None) -> dict:
    t0      = time.time()
    df      = read_excel(file)
    cadence = _get_cadence(file.name)

    if df is None:
        return {"file": file.name, "status": "error", "rows": 0}

    original_rows = len(df)
    rejects: list[pd.DataFrame] = []

    # ── Integrity pre-check (formulas + hidden columns, single file load) ──────
    # The reader (calamine) only sees a formula's cached value and nothing about
    # hidden state, so both can feed stale/unintended data in silently. One
    # openpyxl pass detects both; flag them loudly so the user can fix the source
    # file, and record them in the Rejection Summary.
    formula_cols, hidden_cols = detect_formula_and_hidden(file)
    if formula_cols:
        total_cells = sum(formula_cols.values())
        col_list    = ", ".join(f"{c} ({n:,})" for c, n in formula_cols.items())
        print(f"  {_color('⚠  FORMULAS IN THE FILE', C.RED + C.BOLD)}")
        print(f"  {_color(f'   {total_cells:,} formula cell(s) across {len(formula_cols)} column(s): {col_list}', C.RED)}")
        print(f"  {_color('   These were read as cached values and may be stale/zero. Open the file, '
                          'convert formulas to values (Copy → Paste Special → Values), and re-run.', C.RED)}")

    # Hidden columns still carry data but are invisible in Excel — an easy way to
    # ship stale/unintended values or miss a column that should have been removed.
    if hidden_cols:
        hidden_list = ", ".join(hidden_cols)
        print(f"  {_color('⚠  HIDDEN COLUMNS IN THE FILE', C.RED + C.BOLD)}")
        print(f"  {_color(f'   {len(hidden_cols)} hidden column(s): {hidden_list}', C.RED)}")
        print(f"  {_color('   Unhide and review these columns in the source file, then re-run.', C.RED)}")

    # ── Drop first-name-without-last-name (BEFORE the backfill masks it) ───────
    if {"OWNER FIRST NAME", "OWNER LAST NAME"}.issubset(df.columns):
        before = len(df)
        df, rej = _filter_first_without_last(df)
        if not rej.empty:
            rej["Source_File"] = file.name
            rejects.append(rej)
        removed = before - len(df)
        if removed:
            print_done(f"  First w/o Last Name      : {removed:,} removed")

    # ── Fill owner last name fallback ──────────────────────────────────────────
    if "OWNER LAST NAME" in df.columns and "OWNER FULL NAME" in df.columns:
        df["OWNER LAST NAME"] = df["OWNER LAST NAME"].fillna(df["OWNER FULL NAME"])

    # ── Sort by scores ─────────────────────────────────────────────────────────
    # SCORE is the primary ranking key so the trim (df.head(goal)) keeps the
    # highest-SCORE properties. LIKELY DEAL SCORE / BUYBOX SCORE are tie-breakers
    # only. Ranking on BUYBOX first buried legitimate high-SCORE rows (notably
    # Company owners, whose BUYBOX is systematically low) at the bottom of the
    # file, so any trim dropped them first despite near-perfect SCORE values.
    score_cols = [c for c in ["SCORE", "LIKELY DEAL SCORE", "BUYBOX SCORE"] if c in df.columns]
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

    # ── Coerce ABSENTEE to numeric early ───────────────────────────────────────
    # Downstream absentee logic — the DM same-address block below and
    # _correct_absentee — compares ABSENTEE against numbers. A file that delivers
    # it as text would raise a TypeError on `>= 1`, failing the whole file. Coerce
    # once here so every later comparison is safe; blanks/junk become NaN (which
    # _correct_absentee already treats as null).
    if "ABSENTEE" in df.columns:
        df["ABSENTEE"] = pd.to_numeric(df["ABSENTEE"], errors="coerce")

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

    if "ACTION PLANS" in df.columns and run_type == "360":
        before = len(df); _apply(_filter_empty_action_plans, df)
        print_done(f"  Empty Action Plans       : {before - len(df):,} removed")
    elif run_type == "manual":
        print(f"  {_color('Empty Action Plans       : skipped (Manual Pull)', C.YELLOW)}")

    # Duplicate mailing address → DM only: keep the first row per mailing address and
    # drop the rest (owner-agnostic). CC/SMS instead FLAG the different-owner case via
    # _flag_shared_mailing_address below — their contact channel is a phone, so a
    # shared mailbox is reviewed, not removed.
    if {"MAILING ADDRESS", "MAILING ZIP"}.issubset(df.columns):
        if cadence == "dm":
            before = len(df); _apply(_filter_duplicates, df, ["MAILING ADDRESS", "MAILING ZIP"], "Duplicate Address")
            print_done(f"  Duplicate Address (DM)   : {before - len(df):,} removed")
        else:
            print(f"  {_color('Duplicate Address        : skipped (CC/SMS — flagged instead)', C.YELLOW)}")

    if {"OWNER FULL NAME", "ADDRESS", "ZIP"}.issubset(df.columns):
        before = len(df); _apply(_filter_duplicates, df, ["OWNER FULL NAME", "ADDRESS", "ZIP"], "Duplicate Owner")
        print_done(f"  Duplicate Owner          : {before - len(df):,} removed")

    if "OWNER FULL NAME" in df.columns:
        before = len(df); _apply(_filter_empty_owner_name, df)
        print_done(f"  Empty Owner Full Name    : {before - len(df):,} removed")

        before = len(df); _apply(_filter_unwanted_names, df)
        print_done(f"  Unwanted Names           : {before - len(df):,} removed")

        before = len(df); _apply(_filter_consonant_only_names, df)
        print_done(f"  Consonant-Only Names     : {before - len(df):,} removed")

        before = len(df); _apply(_filter_institutional_owners, df)
        print_done(f"  Institutional Owners     : {before - len(df):,} removed")

        before = len(df); _apply(_filter_trustee_tokens, df)
        print_done(f"  Trust/Trustee (non-Trust): {before - len(df):,} removed")

    if "OWNER TYPE" in df.columns:
        before = len(df); _apply(_filter_company_with_first_name, df)
        print_done(f"  Company w/ First Name    : {before - len(df):,} removed")

        before = len(df); _apply(_filter_company_estate_of, df)
        print_done(f"  Estate of (Company type) : {before - len(df):,} removed")

    if "TAGS" in df.columns:
        before = len(df); _apply(_filter_tags, df, cadence)
        print_done(f"  Blacklisted Tags ({cadence.upper():<3})   : {before - len(df):,} removed")

    # ── Invalid phone type — CC/SMS only; drops only when no valid phone remains ─
    if cadence in ("cc", "sms"):
        before = len(df); _apply(_filter_invalid_phone_types, df, cadence)
        print_done(f"  Invalid Phone Type       : {before - len(df):,} removed")

    if {"PROPERTY TYPE", "ADDRESS"}.issubset(df.columns):
        before = len(df); _apply(_filter_sfh_with_unit, df)
        print_done(f"  SFH With Unit Number     : {before - len(df):,} removed")

    if "ADDRESS" in df.columns:
        before = len(df); _apply(_filter_street_prefix_only, df)
        print_done(f"  Street Prefix Without Name: {before - len(df):,} removed")

    # ── Name logic validation ──────────────────────────────────────────────────
    if {"OWNER FULL NAME", "OWNER FIRST NAME", "OWNER LAST NAME"}.issubset(df.columns):
        df, rej = _filter_name_logic(df)
        if not rej.empty:
            rej["Source_File"] = file.name
            rejects.append(rej)

    # ── Vacant lot filter ──────────────────────────────────────────────────────
    if "ADDRESS" in df.columns:
        df, rej = _filter_vacant_lots(df)
        if not rej.empty:
            rej["Source_File"] = file.name
            rejects.append(rej)

    # ── PO Box / mailing-only format as property address ───────────────────────
    # A property ADDRESS in a mailing-only format (PO Box, Rural Route, HC/HCR,
    # PSC/Unit/CMO, General Delivery) is not a physical property — dropped on every
    # cadence. The same formats stay valid for MAILING ADDRESS.
    if "ADDRESS" in df.columns:
        before = len(df); _apply(_filter_pobox_property_address, df)
        print_done(f"  PO Box Property Address  : {before - len(df):,} removed")

    # ── Absentee address mismatch — DM only, automatic ─────────────────────────
    # Drops both contradictions: absentee (>=1) with the same address, and
    # non-absentee (0) with a different mailing address. The filter is a no-op
    # when neither is present, so it is safe to always run here.
    if cadence == "dm" and {"ABSENTEE", "ADDRESS", "MAILING ADDRESS"}.issubset(df.columns):
        before = len(df); _apply(_filter_absentee_same_address, df)
        removed = before - len(df)
        if removed:
            print_done(f"  Absentee address mismatch: {removed:,} removed (DM only)")

    # ── V-01 Invalid state code ────────────────────────────────────────────────
    # MAILING STATE is validated for DM only (foreign mailing address disqualifies
    # a mail row); property STATE is validated on every cadence.
    if "STATE" in df.columns or "MAILING STATE" in df.columns:
        before = len(df); _apply(_filter_invalid_state, df, cadence == "dm")
        print_done(f"  Invalid State Code (V-01): {before - len(df):,} removed")

    # ── Address validation (flags only, no removal) ────────────────────────────
    df = _run_address_validation(df)

    # ── Absentee correction (all cadences) ────────────────────────────────────
    df = _correct_absentee(df)

    # ── Pre-foreclosure correction (all cadences) ──────────────────────────────
    df = _correct_preforeclosure(df)

    # ── Duplicate phone numbers across owners (flag only) ──────────────────────
    df = _flag_duplicate_phone_numbers(df)

    # ── Shared mailing address across different owners (CC/SMS flag only) ───────
    # DM drops these via the Duplicate Address filter above; CC/SMS keep & flag.
    if cadence in ("cc", "sms"):
        df = _flag_shared_mailing_address(df)

    # ── ZIP / county consistency (flag only, all cadences; review signal) ───────
    df = _flag_zip_county_review(df)

    # ── Sticker price numeric coverage (DM only; always reports, alerts <90%) ───
    df = _check_sticker_price(df, cadence)

    # ── Overlap check (flag-only, cadence-aware) ───────────────────────────────
    try:
        df = _check_overlap(df, cadence)
    except Exception as e:
        print_warn(f"  Overlap check skipped: {e}")

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

    # ── County coverage check (mandatory) ──────────────────────────────────────
    county_result = None
    if domain_index is not None:
        county_result = _report_county_coverage(df, file.name, domain_index)
        if county_result is not None:
            county_result["file"]            = file.name
            county_result["total_file_rows"] = original_rows

    # ── Auto-update filename K-count ───────────────────────────────────────────
    updated_name = _update_filename_k(file.stem, len(df))
    out_path     = output_dir / f"cleaned_{updated_name}.xlsx"

    # Drop the overlap "Last recommendation" columns for every channel — they are
    # only used by the overlap check and must not appear in the cleaned output.
    overlap_cols = [find_column(df, [name]) for name in OVERLAP_COLUMNS.values()]
    overlap_cols = [c for c in overlap_cols if c]

    extra_cols = [
        "data_quality_flags", "OWNER FULL NAME ORIGINAL",
        "ABSENTEE ORIGINAL", "Name_Issue", "Source_File",
    ] + overlap_cols
    clean_df = df.drop(columns=[c for c in extra_cols if c in df.columns])

    # ── Final output ordering — always sort by SCORE (highest first) ────────────
    # The delivered file must be ranked by SCORE regardless of which trim path ran.
    # The simple trim already leaves the frame in SCORE order, but the geo-preserve
    # trim returns rows grouped by area, so without this the output would not be
    # globally score-sorted.
    final_sort_cols = [c for c in ["SCORE", "LIKELY DEAL SCORE", "BUYBOX SCORE"]
                       if c in clean_df.columns]
    if final_sort_cols:
        for c in final_sort_cols:
            clean_df[c] = pd.to_numeric(clean_df[c], errors="coerce")
        clean_df = clean_df.sort_values(
            by=final_sort_cols, ascending=[False] * len(final_sort_cols)
        ).reset_index(drop=True)

    save_excel(clean_df, out_path)
    print_done(f"  Saved → {out_path.name}")

    # ── Accumulate rejects ─────────────────────────────────────────────────────
    non_empty = [r for r in rejects if isinstance(r, pd.DataFrame) and not r.empty]
    if non_empty:
        combined_rej = pd.concat(non_empty, ignore_index=True)
        combined_rej["Total_File_Rows"] = original_rows
        rejected_all.append(combined_rej)

    # ── Accumulate flagged rows ────────────────────────────────────────────────
    if "data_quality_flags" in df.columns:
        flagged = df[df["data_quality_flags"].astype(str).str.strip() != ""].copy()
        if not flagged.empty:
            flagged["Source_File"] = file.name
            flagged["Total_File_Rows"] = original_rows
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
        "county":   county_result,
        "formulas": formula_cols,
        "hidden":   hidden_cols,
    }


# ── Output Reports ─────────────────────────────────────────────────────────────

def _save_reports(rejected_all: list[pd.DataFrame],
                  flagged_all:  list[pd.DataFrame],
                  output_dir:   Path,
                  county_all:   list | None = None,
                  formula_all:  list | None = None,
                  hidden_all:   list | None = None):

    frames = []

    # ── Rejected rows ──────────────────────────────────────────────────────────
    if rejected_all:
        all_rej = pd.concat([r for r in rejected_all if not r.empty], ignore_index=True)
        if not all_rej.empty:
            all_rej["Status"] = "Rejected"
            all_rej["Reason"] = (all_rej["Rejection_Stage"].astype(str)
                                 + " — " + all_rej["Rejection_Value"].astype(str))
            frames.append(all_rej)

    # ── Flagged rows ───────────────────────────────────────────────────────────
    if flagged_all:
        all_flagged = pd.concat([f for f in flagged_all if not f.empty], ignore_index=True)
        if not all_flagged.empty:
            all_flagged["Status"] = "Flagged"
            all_flagged["Reason"] = all_flagged["data_quality_flags"].astype(str)
            frames.append(all_flagged)

    # ── Rejection Summary (appends across runs) ────────────────────────────────
    summary_frames = []

    if rejected_all:
        all_rej_s = pd.concat([r for r in rejected_all if not r.empty], ignore_index=True)
        if not all_rej_s.empty:
            rej_grp = (
                all_rej_s.groupby(["Source_File", "Rejection_Stage"])
                .agg(Count=("Rejection_Stage", "size"),
                     Total_File_Rows=("Total_File_Rows", "first"))
                .reset_index()
                .rename(columns={"Rejection_Stage": "Stage_or_Flag"})
            )
            rej_grp["Status"] = "Rejected"
            summary_frames.append(rej_grp)

    if flagged_all:
        all_flag_s = pd.concat([f for f in flagged_all if not f.empty], ignore_index=True)
        if not all_flag_s.empty:
            # A kept row can carry several pipe-joined flags (e.g.
            # "incomplete_property_address|incomplete_mailing_address"). Count each flag
            # SEPARATELY so a row with N errors adds +1 to each error's tally, rather
            # than forming a single merged "a|b" bucket.
            exploded = all_flag_s.assign(
                _flag=all_flag_s["data_quality_flags"].astype(str).str.split("|")
            ).explode("_flag")
            exploded["_flag"] = exploded["_flag"].str.strip()
            exploded = exploded[exploded["_flag"] != ""]
            flag_grp = (
                exploded.groupby(["Source_File", "_flag"])
                .agg(Count=("_flag", "size"),
                     Total_File_Rows=("Total_File_Rows", "first"))
                .reset_index()
                .rename(columns={"_flag": "Stage_or_Flag"})
            )
            flag_grp["Status"] = "Flagged"
            summary_frames.append(flag_grp)

    # ── County coverage → summary rows ─────────────────────────────────────────
    if county_all:
        from config import COUNTY_LOW_COVERAGE_PCT
        county_rows = []
        for cr in county_all:
            if not cr or not cr.get("matched"):
                continue
            src        = cr.get("file")
            total_rows = cr.get("total_file_rows")
            total      = cr.get("total", 0)

            # Missing active counties → Rejected, count 1 each
            for county in sorted(cr.get("missing", [])):
                county_rows.append({
                    "Source_File":     src,
                    "Total_File_Rows": total_rows,
                    "Status":          "Rejected",
                    "Stage_or_Flag":   "Missing county",
                    "Count":           1,
                })

            # Under-represented counties (< threshold of fulfillment) → Flagged.
            # Count is 1 per low-coverage county (matching the "Missing county" rows
            # above), not the number of properties in that county.
            if total > 0:
                for county, cnt in cr.get("county_counts", {}).items():
                    if cnt / total < COUNTY_LOW_COVERAGE_PCT:
                        county_rows.append({
                            "Source_File":     src,
                            "Total_File_Rows": total_rows,
                            "Status":          "Flagged",
                            "Stage_or_Flag":   "County presence below 5%",
                            "Count":           1,
                        })

        if county_rows:
            summary_frames.append(pd.DataFrame(county_rows))

    # ── Formulas in the file → summary rows ────────────────────────────────────
    # One row per file that still contained formulas. Count = number of COLUMNS
    # that contain formulas (one per column, not per cell); the affected column
    # names are listed in the red on-screen alert during processing.
    if formula_all:
        formula_rows = []
        for fr in formula_all:
            if not fr or not fr.get("formulas"):
                continue
            formula_rows.append({
                "Source_File":     fr.get("file"),
                "Total_File_Rows": fr.get("total_file_rows"),
                "Status":          "Flagged",
                "Stage_or_Flag":   "Formulas in the file",
                "Count":           len(fr["formulas"]),
            })
        if formula_rows:
            summary_frames.append(pd.DataFrame(formula_rows))

    # ── Hidden columns in the file → summary rows ──────────────────────────────
    # One row per file that had hidden columns. Count = number of hidden COLUMNS
    # (one per column); the column names are listed in the red on-screen alert.
    if hidden_all:
        hidden_rows = []
        for hr in hidden_all:
            if not hr or not hr.get("hidden"):
                continue
            hidden_rows.append({
                "Source_File":     hr.get("file"),
                "Total_File_Rows": hr.get("total_file_rows"),
                "Status":          "Flagged",
                "Stage_or_Flag":   "Hidden columns in the file",
                "Count":           len(hr["hidden"]),
            })
        if hidden_rows:
            summary_frames.append(pd.DataFrame(hidden_rows))

    if summary_frames:
        summary = pd.concat(summary_frames, ignore_index=True)
        summary["Run_Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        col_order = ["Run_Timestamp", "Source_File", "Total_File_Rows",
                     "Status", "Stage_or_Flag", "Count"]
        summary = summary[[c for c in col_order if c in summary.columns]]
        summary_path = output_dir / "Rejection_Summary.xlsx"
        if summary_path.exists():
            existing_summary = read_excel(summary_path)
            if existing_summary is not None:
                summary = pd.concat([existing_summary, summary], ignore_index=True)
        save_excel(summary, summary_path)
        n_rej  = summary["Status"].eq("Rejected").sum()
        n_flag = summary["Status"].eq("Flagged").sum()
        print_done(f"Rejection summary updated → Rejection_Summary.xlsx  "
                   f"({n_rej:,} rejected rows, {n_flag:,} flagged rows across all runs)")

    # ── Combined Quality Report (overwritten each run) ─────────────────────────
    if frames:
        combined      = pd.concat(frames, ignore_index=True)
        priority_cols = ["Status", "Reason", "Source_File"]
        other_cols    = [c for c in combined.columns if c not in priority_cols]
        combined      = combined[priority_cols + other_cols]
        save_excel(combined, output_dir / "Quality_Report.xlsx")
        rejected_count = (combined["Status"] == "Rejected").sum()
        flagged_count  = (combined["Status"] == "Flagged").sum()
        print_done(f"Quality report saved → Quality_Report.xlsx  "
                   f"({rejected_count:,} rejected, {flagged_count:,} flagged)")

    # ── Cumulative Run Log (appends across runs) ───────────────────────────────
    log_frames = []

    if rejected_all:
        all_rej = pd.concat([r for r in rejected_all if not r.empty], ignore_index=True)
        if not all_rej.empty:
            all_rej["Status"] = "Rejected"
            log_frames.append(all_rej)

    if flagged_all:
        all_flagged = pd.concat([f for f in flagged_all if not f.empty], ignore_index=True)
        if not all_flagged.empty:
            all_flagged["Status"] = "Flagged"
            all_flagged["Rejection_Stage"] = all_flagged["data_quality_flags"]
            all_flagged["Rejection_Value"] = all_flagged["data_quality_flags"]
            log_frames.append(all_flagged)

    if log_frames:
        run_log_path = output_dir / "Rejection_Run_Log.xlsx"
        combined_log = pd.concat(log_frames, ignore_index=True)
        combined_log["Run_Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_cols = ["Run_Timestamp", "Status", "Source_File", "Total_File_Rows",
                    "Rejection_Stage", "Rejection_Value",
                    "OWNER FULL NAME", "OWNER FIRST NAME", "OWNER LAST NAME",
                    "OWNER TYPE", "PROPERTY TYPE",
                    "ADDRESS", "ZIP",
                    "MAILING ADDRESS", "MAILING ZIP", "FOLIO", "LINK PROPERTIES"]
        log_cols_present = [c for c in log_cols if c in combined_log.columns]
        run_entry = combined_log[log_cols_present].copy()

        if run_log_path.exists():
            existing = read_excel(run_log_path)
            if existing is not None:
                run_entry = pd.concat([existing, run_entry], ignore_index=True)

        save_excel(run_entry, run_log_path)
        n_rej  = (run_entry["Status"] == "Rejected").sum()
        n_flag = (run_entry["Status"] == "Flagged").sum()
        print_done(f"Run log updated → Rejection_Run_Log.xlsx  "
                   f"({n_rej:,} rejected, {n_flag:,} flagged across all runs)")


# ── Entry Point ────────────────────────────────────────────────────────────────

def run():
    print_header("STEP 1 — CLEAN")

    run_type = _prompt_run_type()
    _prompt_clear_output_folders()

    files = get_excel_files(INPUT_DIR)
    if not files:
        print_error(f"No Excel files found in {INPUT_DIR}")
        return

    print_step(f"Found {len(files)} file(s) in {INPUT_DIR.name}/")

    # ── Load county master file (mandatory county coverage check) ──────────────
    master_df, master_src = load_master()
    if master_df is not None:
        domain_index = build_domain_index(master_df)
        print_done(f"County master loaded from {master_src} — {len(domain_index):,} clients indexed.")
    else:
        domain_index = None
        print_warn("County master file not found (Google Drive not mounted and no local copy) — "
                   "county coverage check will be skipped.")
        print_warn(f"  To enable it, drop the master CSV into: {COUNTY_MASTER_LOCAL}")

    rejected_all: list[pd.DataFrame] = []
    flagged_all:  list[pd.DataFrame] = []
    results = []

    for f in files:
        print_step(f"Processing: {f.name}")
        try:
            result = _process_file(f, OUT_STEP1, rejected_all, flagged_all, run_type, domain_index)
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

    if run_type == "360":
        county_all  = [r.get("county") for r in results if r.get("county")]
        formula_all = [
            {"file": r.get("file"), "formulas": r.get("formulas"),
             "total_file_rows": r.get("original")}
            for r in results if r.get("formulas")
        ]
        hidden_all = [
            {"file": r.get("file"), "hidden": r.get("hidden"),
             "total_file_rows": r.get("original")}
            for r in results if r.get("hidden")
        ]
        _save_reports(rejected_all, flagged_all, OUT_STEP1,
                      county_all, formula_all, hidden_all)

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
