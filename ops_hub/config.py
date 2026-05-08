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
]

for d in DIRS_TO_CREATE:
    d.mkdir(parents=True, exist_ok=True)

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
    "City of", "Not Provided Name"
]

UNWANTED_OWNER_TYPES = []

TAGS_BLACKLIST = [
    "Liti", "DNC", "donotmail", "Takeoff", "Undeli", "Return", "Dead",
    "Do Not Mail", "Dono", "Do no", "Available"
]

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