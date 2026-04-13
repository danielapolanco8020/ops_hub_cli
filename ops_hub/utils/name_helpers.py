import re

# ── Name Lists ─────────────────────────────────────────────────────────────────

COMMON_FIRST_NAMES = {
    "James","John","Robert","Michael","William","David","Richard","Joseph","Thomas","Charles",
    "Patricia","Jennifer","Linda","Barbara","Elizabeth","Laura","Jessica","Sarah","Karen","Nancy",
    "Matthew","Daniel","Paul","Mark","Donald","George","Kenneth","Steven","Edward","Brian",
    "Ronald","Anthony","Kevin","Jason","Jeff","Frank","Timothy","Gary","Ryan","Nicholas",
    "Eric","Stephen","Andrew","Raymond","Gregory","Joshua","Jerry","Dennis","Walter","Patrick",
    "Peter","Harold","Douglas","Henry","Carl","Arthur","Roger","Keith","Jeremy","Terry",
    "Lawrence","Sean","Christian","Albert","Joe","Ethan","Austin","Jesse","Willie","Billy",
    "Bryan","Bruce","Ralph","Roy","Jordan","Eugene","Wayne","Alan","Juan","Louis","Russell",
    "Gabriel","Carol","Randy","Philip","Harry","Vincent","Noah","Bobby","Johnny","Logan",
    "Virginia","Jerrie","Danny",
}

COMMON_LAST_NAMES = {
    "Smith","White","Black","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
    "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Moore",
    "Jackson","Lee","Perez","Thompson","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson",
    "Walker","Young","Allen","King","Wright","Torres","Nguyen","Hill","Flores","Green","Adams",
    "Baker","Hall","Rivera","Campbell","Mitchell","Carter","Roberts","Gomez","Phillips","Evans",
    "Turner","Diaz","Parker","Cruz","Edwards","Collins","Reyes","Stewart","Morris","Morales",
    "Murphy","Cook","Rogers","Gutierrez","Ortiz","Morgan","Cooper","Peterson","Bailey","Reed",
    "Kelly","Howard","Ramos","Kim","Cox","Ward","Richardson","Watson","Brooks","Chavez","Wood",
    "James","Bennett","Gray","Mendoza","Ruiz","Hughes","Price","Alvarez","Castillo","Sanders",
    "Patel","Myers","Long","Ross","Foster","Jimenez",
}

BUSINESS_ENTITIES = {
    "trust","investment","properties","prop","capital","acquisitions","association",
    "inc","incorporated","and","council","rental","llc",
}

NAME_TERMS = {
    "jr","sr","tr","ii","iii","iv","esq","phd","md","aka","fka","tod","dba","mba","cpa",
}


# ── Core Logic ─────────────────────────────────────────────────────────────────

def clean_and_split_name(full_name: str) -> tuple[str, str]:
    """
    Clean a full name string and split into (first_name, last_name).

    Steps:
      1. Detect and handle LLC / business entities
      2. Handle 'tr' trustee names
      3. Strip punctuation, remove name suffix terms
      4. Correct order using common name lists (swap if last detected first)
      5. Return (first_name, last_name)
    """
    full_name = str(full_name).strip(" ,-.")
    words     = full_name.lower().split()

    # ── LLC ────────────────────────────────────────────────────────────────────
    if "llc" in words:
        words_no_llc = [w for w in words if w != "llc"]
        return " ".join(words_no_llc).title(), "LLC"

    # ── Other business entities ────────────────────────────────────────────────
    if any(e in words for e in BUSINESS_ENTITIES):
        return full_name, full_name

    # ── Trustee (tr) ───────────────────────────────────────────────────────────
    orig_words  = full_name.split()
    lower_words = [w.lower() for w in orig_words]
    if "tr" in lower_words:
        last_name  = orig_words[0]
        first_name = " ".join(orig_words[1:-1])
        return _clean_name_part(first_name), _clean_name_part(last_name)

    # ── Standard split ─────────────────────────────────────────────────────────
    cleaned = re.sub(r"[^\w\s]", "", full_name)
    parts   = cleaned.split()
    parts   = [w for w in parts if len(w) > 1 or len(parts) == 2]
    parts   = [w for w in parts if w.lower() not in NAME_TERMS]

    if not parts:
        return full_name, ""
    if len(parts) == 1:
        return parts[0], ""

    first_name = parts[0]
    last_name  = " ".join(parts[1:])

    # ── Order correction ───────────────────────────────────────────────────────
    fn_title = first_name.title()
    ln_title = last_name.split()[0].title() if last_name else ""

    fn_is_first = fn_title in COMMON_FIRST_NAMES
    fn_is_last  = fn_title in COMMON_LAST_NAMES
    ln_is_first = ln_title in COMMON_FIRST_NAMES
    ln_is_last  = ln_title in COMMON_LAST_NAMES

    should_swap = (
        (fn_is_last  and not fn_is_first and not ln_is_last) or
        (ln_is_first and not ln_is_last  and not fn_is_first)
    )

    if should_swap:
        first_name, last_name = last_name, first_name

    return first_name.strip().title(), last_name.strip().title()


def _clean_name_part(s: str) -> str:
    """Remove name terms and single-character fragments from a name part."""
    parts = s.split()
    parts = [w for w in parts if w.lower() not in NAME_TERMS and len(w) > 1]
    return " ".join(parts).title()


def needs_name_split(row) -> bool:
    """Return True if OWNER FIRST NAME or OWNER LAST NAME is empty/NaN."""
    import pandas as pd
    fn = row.get("OWNER FIRST NAME", None)
    ln = row.get("OWNER LAST NAME",  None)
    return pd.isna(fn) or str(fn).strip() == "" or pd.isna(ln) or str(ln).strip() == ""
