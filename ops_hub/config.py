import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pathlib import Path

# ── Root Paths ────────────────────────────────────────────────────────────────
ROOT_DIR    = Path(__file__).parent
INPUT_DIR   = ROOT_DIR / "input"
OUTPUT_DIR  = ROOT_DIR / "output"
MERGE_DIR   = ROOT_DIR / "merge"

# ── Output Subdirectories ─────────────────────────────────────────────────────
OUT_STEP1        = OUTPUT_DIR / "step1_clean"
OUT_STEP2        = OUTPUT_DIR / "step2_optional"
OUT_STEP3        = OUTPUT_DIR / "step3_audit"
OUT_STEP4        = OUTPUT_DIR / "step4_skiptrace" / "export"
OUT_ZESTIMATE_EX = OUTPUT_DIR / "zestimate" / "export"
OUT_ZESTIMATE_MG = OUTPUT_DIR / "zestimate" / "merged"

# ── Merge Drop Folders ────────────────────────────────────────────────────────
MERGE_ZESTIMATE  = MERGE_DIR / "zestimate"
MERGE_SKIPTRACE  = MERGE_DIR / "skiptrace"

# ── County Master File (Active Counties coverage check) ───────────────────────
# Primary source: Google Drive Desktop mount (machine-dependent).
# Fallback source: local folder where a user without Drive can drop the CSV.
COUNTY_MASTER_DRIVE = Path(
    r"G:\.shortcut-targets-by-id\1nJVflP2GIzXFjArMBwqzP7HVVHPdhTs1"
    r"\Client folders\AA county master file"
)
COUNTY_MASTER_LOCAL = MERGE_DIR / "county_master"

# Master file column names
COUNTY_MASTER_NAME_COL     = "Name"
COUNTY_MASTER_DOMAIN_COL   = "Domain (8020REI)"
COUNTY_MASTER_COUNTIES_COL = "Active Counties"

# A county whose share of the fulfillment is below this fraction is flagged as
# under-represented in the Rejection Summary.
COUNTY_LOW_COVERAGE_PCT = 0.05

# ── Auto-create all directories ───────────────────────────────────────────────
DIRS_TO_CREATE = [
    INPUT_DIR,
    OUT_STEP1,
    OUT_STEP2,
    OUT_STEP3,
    OUT_STEP4,
    OUT_ZESTIMATE_EX,
    OUT_ZESTIMATE_MG,
    MERGE_ZESTIMATE,
    MERGE_SKIPTRACE,
    COUNTY_MASTER_LOCAL,
]

for d in DIRS_TO_CREATE:
    d.mkdir(parents=True, exist_ok=True)

# ── Excel I/O engines ─────────────────────────────────────────────────────────
# Single source of truth for how the pipeline reads/writes .xlsx. EVERY step goes
# through utils.file_helpers (read_excel / read_many_parallel / save_excel /
# save_excel_multisheet), so changing these two flags changes the whole pipeline
# at once — no step touches an engine directly.
#
# READ_ENGINE  "calamine"   — Rust reader, ~6x faster than openpyxl, byte-for-byte
#                             identical output (verified on real files); releases
#                             the GIL so parallel reads scale.
# WRITE_ENGINE "xlsxwriter" — ~2.2x faster writes and ~3x smaller files than
#                             openpyxl, identical data. URL auto-linking is disabled
#                             centrally (see file_helpers._writer_kwargs) so
#                             LINK PROPERTIES stays plain text like before.
# Set either back to "openpyxl" to revert instantly.
READ_ENGINE  = "calamine"
WRITE_ENGINE = "xlsxwriter"

# ── Cadences ──────────────────────────────────────────────────────────────────
CADENCES = ["Direct Mail", "Cold Calling", "SMS"]

CADENCE_MAP = {
    "dm":  "Direct Mail",
    "cc":  "Cold Calling",
    "sms": "SMS",
}

# ── Required Columns per Cadence ──────────────────────────────────────────────
REQUIRED_COLUMNS = {
    "Direct Mail": [
        "FOLIO","APN","OWNER FULL NAME","OWNER FIRST NAME","OWNER LAST NAME",
        "ADDRESS","CITY","STATE","ZIP","COUNTY",
        "MAILING ADDRESS","MAILING CITY","MAILING STATE","MAILING ZIP",
        "GOLDEN ADDRESS","GOLDEN CITY","GOLDEN STATE","GOLDEN ZIP CODE",
        "ACTION PLANS","PROPERTY STATUS","SCORE","LIKELY DEAL SCORE","BUYBOX SCORE",
        "PROPERTY TYPE","VALUE","LINK PROPERTIES","TAGS","HIDDENGEMS","ABSENTEE",
        "HIGH EQUITY","DOWNSIZING","PRE-FORECLOSURE","VACANT","55+","ESTATE",
        "INTER FAMILY TRANSFER","DIVORCE","TAXES","PROBATE","LOW CREDIT",
        "CODE VIOLATIONS","BANKRUPTCY","LIENS CITY/COUNTY","LIENS OTHER",
        "LIENS UTILITY","LIENS HOA","LIENS MECHANIC","POOR CONDITION","EVICTION",
        "30-60 DAYS","JUDGEMENT","DEBT COLLECTION","DEFAULT RISK",
        "MARKETING DM COUNT","ESTIMATED CASH OFFER",
        "MAIN DISTRESS #1","MAIN DISTRESS #2","MAIN DISTRESS #3","MAIN DISTRESS #4",
        "TARGETED MESSAGE #1","TARGETED MESSAGE #2","TARGETED MESSAGE #3","TARGETED MESSAGE #4",
        "TARGETED GROUP NAME","TARGETED GROUP MESSAGE","TARGETED POSTCARD",
    ],
    "Cold Calling": [
        "FOLIO","APN","OWNER FULL NAME","OWNER FIRST NAME","OWNER LAST NAME",
        "SECOND OWNER FULL NAME",
        "ADDRESS","CITY","STATE","ZIP","COUNTY",
        "MAILING ADDRESS","MAILING CITY","MAILING STATE","MAILING ZIP",
        "GOLDEN ADDRESS","GOLDEN CITY","GOLDEN STATE","GOLDEN ZIP CODE",
        "ACTION PLANS","PROPERTY STATUS","SCORE","LIKELY DEAL SCORE","BUYBOX SCORE",
        "PROPERTY TYPE","VALUE","LINK PROPERTIES","TAGS","HIDDENGEMS","ABSENTEE",
        "HIGH EQUITY","DOWNSIZING","PRE-FORECLOSURE","VACANT","55+","ESTATE",
        "INTER FAMILY TRANSFER","DIVORCE","TAXES","PROBATE","LOW CREDIT",
        "CODE VIOLATIONS","BANKRUPTCY","LIENS CITY/COUNTY","LIENS OTHER",
        "LIENS UTILITY","LIENS HOA","LIENS MECHANIC","POOR CONDITION","EVICTION",
        "30-60 DAYS","JUDGEMENT","DEBT COLLECTION","DEFAULT RISK",
        "MARKETING CC COUNT",
    ],
    "SMS": [
        "FOLIO","APN","OWNER FULL NAME","OWNER FIRST NAME","OWNER LAST NAME",
        "ADDRESS","CITY","STATE","ZIP","COUNTY",
        "MAILING ADDRESS","MAILING CITY","MAILING STATE","MAILING ZIP",
        "GOLDEN ADDRESS","GOLDEN CITY","GOLDEN STATE","GOLDEN ZIP CODE",
        "ACTION PLANS","PROPERTY STATUS","SCORE","LIKELY DEAL SCORE","BUYBOX SCORE",
        "PROPERTY TYPE","VALUE","LINK PROPERTIES","TAGS","HIDDENGEMS","ABSENTEE",
        "HIGH EQUITY","DOWNSIZING","PRE-FORECLOSURE","VACANT","55+","ESTATE",
        "INTER FAMILY TRANSFER","DIVORCE","TAXES","PROBATE","LOW CREDIT",
        "CODE VIOLATIONS","BANKRUPTCY","LIENS CITY/COUNTY","LIENS OTHER",
        "LIENS UTILITY","LIENS HOA","LIENS MECHANIC","POOR CONDITION","EVICTION",
        "30-60 DAYS","JUDGEMENT","DEBT COLLECTION","DEFAULT RISK",
        "MARKETING SMS COUNT",
        "MAIN DISTRESS #1","MAIN DISTRESS #2","MAIN DISTRESS #3","MAIN DISTRESS #4",
        "TARGETED MESSAGE #1","TARGETED MESSAGE #2","TARGETED MESSAGE #3","TARGETED MESSAGE #4",
        "TARGETED GROUP NAME","TARGETED GROUP MESSAGE",
    ],
}

# ── Clean Step ────────────────────────────────────────────────────────────────
UNWANTED_NAMES = [
    "Given Not", "Record", "Available", "Bank ", "Church ", "School", "Cemetery",
    "Not given", "University", "College", "Owner", "Hospital", "County",
    "City of", "Not Provided Name", "Redacted Upon Request"
]

# Strong institutional keywords — a WHOLE-WORD match on any of these is enough to
# flag an owner as an institution (matched with \b…\b boundaries in the filter, so
# "Gas" no longer hits "Vargas", "Bank" no longer hits "Banks", etc.).
# NOTE: "HOA" was removed — the 3-letter token collided with names like "Hoa";
# real HOAs are still caught by "Homeowners" / "Association" / "Owners Association".
# "Power", "Temple" and "Church" were moved to INSTITUTIONAL_QUALIFIED_KEYWORDS
# below because they are also common surnames.
INSTITUTIONAL_KEYWORDS = [
    # Banks & financial
    "Bank", "Bancorp", "Bankers", "Mortgage", "Lending", "Loan", "Financial",
    "Capital", "Investment", "Investments", "Securities", "Asset", "Assets",
    "Fund", "Funds", "Credit Union", "Savings",
    # Government
    "City of", "County of", "State of", "Department", "Authority",
    "Commission", "Municipality", "Federal", "Housing Authority",
    # Religious
    "Ministry", "Ministries", "Diocese", "Parish",
    "Cathedral", "Mosque", "Synagogue", "Fellowship", "Assembly of God",
    # HOA / Condo associations
    "Association", "Condominium", "Homeowners", "Property Owners",
    "Community Association", "Owners Association",
    # Utilities
    "Electric", "Gas", "Water", "Energy", "Utility", "Utilities",
    "Telephone", "Telecom", "Pipeline",
    # Trust / IRA custodians
    # NOTE: "Trustee" is intentionally NOT here — trustee-type tokens are governed
    # solely by the dedicated trustee filter (see TRUSTEE_TOKENS below), which keeps
    # a record only when OWNER TYPE == 'Trust'.
    "Custodian", "FBO", "IRA Trust", "Trust Company",
    "Fiduciary", "Custodial",
    # Schools
    "School", "University", "College", "Academy", "Institute", "Education",
    # Other institutional
    "Cemetery", "Hospital", "Medical Center", "Clinic",
]

# Ambiguous keywords that are ALSO common surnames ("Michael Power", "Betty
# Temple", "John Church"). These are only treated as institutional when one of the
# supporting qualifier words below also appears in the same owner name — e.g.
# "Zion Temple" / "Power Praise" (kept as institutions) vs "Betty J Temple" (kept
# as an individual).
INSTITUTIONAL_QUALIFIED_KEYWORDS = ["Power", "Temple", "Church"]

INSTITUTIONAL_QUALIFIERS = [
    # Religious / church-name context
    "Baptist", "Methodist", "Pentecostal", "Catholic", "Christian", "Zion",
    "Israel", "Ministry", "Ministries", "Deliverance", "Praise", "Faith",
    "Gospel", "God", "Holy", "Grace", "Trinity", "Bible", "Worship",
    "Congregation", "Fellowship", "Assembly", "Sanctuary", "Chapel",
    "Apostolic", "Evangel", "Redeemer", "Salvation", "Mission", "Prophetic",
    "Saint", "St", "First", "Community", "Memorial", "Iglesia", "Christ",
    "Lord", "Jesus", "Spirit", "Kingdom", "Covenant", "Calvary",
    # Corporate / utility / civic context
    "Company", "Co", "Inc", "Corp", "Corporation", "Cooperative", "Coop",
    "Utility", "Utilities", "Authority", "District", "Electric", "Energy",
    "Municipal", "Public", "Light", "LLC",
]

# ── Strong entity tokens (Institutional Owner last-resort tiebreaker) ─────────
# The ONLY thing that can override an explicit OWNER TYPE == 'Individual' after a
# whole-word institutional keyword has still fired on a real-looking surname
# (e.g. "Parish"). If any of these appears as a whole word, the row is a genuine
# entity and stays rejected regardless of OWNER TYPE. Matched with the same
# whole-word (hyphen-aware) logic as the institutional keywords.
STRONG_ENTITY_TOKENS = [
    "LLC", "L.L.C.", "Inc", "Corp", "Company", "Co", "Trust", "Tr", "Trs",
    "Bank", "Mortgage", "Holdings", "Investments", "Realty", "Properties",
    "LP", "Partners", "Fund", "Association", "Assn", "HOA", "Condominium",
    "Custodian", "FBO",
]

# Common given names that also appear in INSTITUTIONAL_QUALIFIERS. When the
# Institutional Owner tiebreaker decides whether to rescue an 'Individual', these
# words do NOT count as institutional context — so a real person (e.g. "Grace
# Parish", "Jesus Rivera") is never blocked from rescue by their own first name.
RESCUE_NAME_EXCEPTIONS = {
    "christian", "faith", "grace", "trinity", "israel", "jesus", "christ", "zion",
}

# ── Trustee tokens (trust-held property) ──────────────────────────────────────
# A trustee-type token in OWNER FULL NAME means the property is held in trust. The
# ONLY valid reason to keep such a record is OWNER TYPE == 'Trust'; every other
# owner type — Individual, Company, Estate, or a blank one — is dropped. Handled by
# its own filter (authoritative for these tokens), matched as whole words.
TRUSTEE_TOKENS = [
    "trustee", "trustees", "co-trustee", "ttee", "trs", "successor trustee",
]

TAGS_BLACKLIST = [
    "Liti", "DNC", "donotmail", "Takeoff", "Undeli", "Return", "Dead",
    "Do Not Mail", "Dono", "Do no", "Available"
]

# ── Channel-specific tag suppression ──────────────────────────────────────────
TAGS_ALL_CHANNELS = [
    "opted out", "dead lead", "not interested", "sold",
    "undeliverable", "rts",
]

TAGS_DM_ONLY = [
    "do not mail", "dnm", "litigator", "possible litigator", "dead call",
]

TAGS_CC_SMS_ONLY = [
    "dnc", "do not call",
]

TAGS_CC_ONLY = [
    "wrong number",
]

TAGS_NEVER_FILTER = [
    "remove from marketing", "deceased", "probate", "bankruptcy",
]

# ── USPS Valid State Codes ─────────────────────────────────────────────────────
USPS_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC","PR","VI","GU","AS","MP","FM","MH","PW",
    "AA","AE","AP",
}
ADDRESS_VALIDATE_TYPES = ["sfh", "single family", "multi", "2-9 units", "condo"]
ADDRESS_SKIP_TYPES     = ["land", "townhouse", "mobile"]

VALID_MAILING_PATTERNS = [
    r'^\s*p\.?o\.?\s*box',
    r'^\s*rural\s+route',
    r'^\s*r\.?r\.?\s*\d',
    r'^\s*(hc|hcr)\s+\d',
    r'^\s*(psc|unit|cmo)\s+\d',
    r'^\s*general\s+delivery',
]

# ── Overlap Check ─────────────────────────────────────────────────────────────
# A property "overlaps" when its last recommendation for this cadence is within
# OVERLAP_DAYS of the run date. If more than OVERLAP_ALERT_PCT of a file overlaps,
# the overlapping properties are flagged ("overlapping") and a console alert is
# printed. The column used is chosen by cadence; a missing column is skipped.
OVERLAP_DAYS      = 30
OVERLAP_ALERT_PCT = 0.30
OVERLAP_COLUMNS   = {
    "dm":  "Last recommendation DM",
    "cc":  "Last recommendation CC",
    "sms": "Last recommendation SMS",
}

# ── Audit Step ────────────────────────────────────────────────────────────────
AUDIT_URGENT_PLAN      = "30 DAYS"
AUDIT_HIGH_PLAN        = "60 DAYS"
AUDIT_URGENT_MIN_SCORE = 746
AUDIT_HIGH_MIN_SCORE   = 545

AUDIT_OWNER_KEYWORDS = [
    "Given ", "Not ", "Record ", "Available ", "Bank ", "Church ", "School ",
    "Cemetery ", "Not given ", "University", "College", "Owner ", "Hospital ",
    "County ", "City of", "Unknown ", "Not Provided "
]

# ── BuyBox Step ───────────────────────────────────────────────────────────────
BUYBOX_LOW_RATE   = 0.60
BUYBOX_HIGH_RATE  = 0.65
BUYBOX_OFFER_RATE = 0.90
BUYBOX_LOW_LIMIT  = 2

# ── Skiptrace Export Columns ──────────────────────────────────────────────────
SKIPTRACE_EXPORT_COLUMNS = {
    "ADDRESS":          "Property Address",
    "ZIP":              "Property Zip Code",
    "OWNER FIRST NAME": "Owner First name",
    "OWNER LAST NAME":  "Owner Last name",
}

# ── Zestimate ─────────────────────────────────────────────────────────────────
ZESTIMATE_EXPORT_COLUMNS = ["ADDRESS", "CITY", "STATE", "ZIP", "FOLIO"]
ZESTIMATE_MERGE_KEYS     = ["FOLIO", "ADDRESS", "CITY", "STATE"]
ZESTIMATE_VALUE_COL      = "zestimate"
ZESTIMATE_OUTPUT_COL     = "STICKER PRICE"

# ── Dataflick ─────────────────────────────────────────────────────────────────
DATAFLICK_DEFAULT_CHUNK = 20_000

# ── Canadian Provinces ────────────────────────────────────────────────────────
CANADIAN_PROVINCES = {
    'AB','BC','MB','NB','NL','NS','NT','NU','ON','PE','QC','SK','YT'
}

# ── Split Step ────────────────────────────────────────────────────────────────
SPLIT_VALID_PLANS   = ['30 DAYS','60 DAYS','60 DAYS B','90 DAYS','90 DAYS B','90 DAYS C']
SPLIT_DEFAULT_WEEKS = 4