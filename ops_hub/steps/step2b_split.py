import warnings
import numpy as np
import pandas as pd
from pathlib import Path

from config import OUT_STEP2, OUT_STEP1, SPLIT_VALID_PLANS, SPLIT_DEFAULT_WEEKS
from utils.file_helpers import (
    get_files_by_cadence, read_excel, save_excel_multisheet,
    prompt_cadence_or_all, prompt_yes_no, prompt_int,
    print_header, print_step, print_done, print_warn, print_error,
    make_output_path,
)

# ── Helpers ────────────────────────────────────────────────────────────────────

def _validate(df, cols, label):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{label}: missing columns: {', '.join(missing)}")


def _prompt_proportions() -> list[tuple[str, float]]:
    """
    Prompt user for number of templates and their proportions.
    Returns list of (label, proportion) tuples e.g. [('template_a', 50.0), ('template_b', 30.0), ('template_c', 20.0)]
    """
    while True:
        try:
            n = int(input("  How many templates do you want to split into? (min 2): ").strip())
            if n >= 2:
                break
            print("  Must be at least 2.")
        except ValueError:
            print("  Enter a valid number.")

    use_default = n == 2 and prompt_yes_no("  Use default 50/50 split?", default=True)
    if use_default:
        return [("template_a", 50.0), ("template_b", 50.0)]

    labels = [f"template_{chr(97+i)}" for i in range(n)]  # template_a, template_b, template_c...

    while True:
        print(f"  Enter proportions for {n} templates separated by commas (must sum to 100).")
        print(f"  Example for {n} templates: {', '.join(['33.3']*( n-1) + [str(round(100 - 33.3*(n-1), 1))])}")
        raw = input("  Proportions: ").strip()
        try:
            parts = [float(x.strip()) for x in raw.split(",")]
            if len(parts) != n:
                print(f"  Enter exactly {n} values.")
                continue
            if any(p <= 0 for p in parts):
                print("  All values must be positive.")
                continue
            if abs(sum(parts) - 100) > 0.01:
                print(f"  Values must sum to 100. Current sum: {sum(parts)}")
                continue
            return list(zip(labels, parts))
        except ValueError:
            print("  Enter valid numbers separated by commas.")


def _split_balanced(df, proportions: list[tuple[str, float]]) -> dict[str, pd.DataFrame]:
    """
    Split df into N templates with balanced ACTION PLANS distribution.
    proportions = [('template_a', 50.0), ('template_b', 30.0), ('template_c', 20.0)]
    """
    total      = len(df)
    n          = len(proportions)
    labels     = [p[0] for p in proportions]
    pcts       = [p[1] for p in proportions]

    # Calculate target counts per template
    targets = [int(total * pct / 100) for pct in pcts]
    # Distribute rounding remainder to first template
    targets[0] += total - sum(targets)

    plan_counts = df["ACTION PLANS"].value_counts().to_dict()

    # Calculate per-plan counts per template
    plan_alloc: dict[str, list[int]] = {}
    for plan, count in plan_counts.items():
        alloc = [int(count * pct / 100) for pct in pcts]
        alloc[0] += count - sum(alloc)
        plan_alloc[plan] = alloc

    # Build output DataFrames
    outputs = {label: pd.DataFrame() for label in labels}
    remaining_df = df.copy()

    for plan in SPLIT_VALID_PLANS:
        plan_df = remaining_df[remaining_df["ACTION PLANS"] == plan].copy()
        if plan_df.empty:
            continue
        alloc = plan_alloc.get(plan, [0] * n)
        for i, label in enumerate(labels):
            n_rows = alloc[i]
            if n_rows <= 0:
                continue
            sampled = plan_df.sample(n=min(n_rows, len(plan_df)), random_state=42+i)
            plan_df = plan_df.drop(sampled.index)
            outputs[label] = pd.concat([outputs[label], sampled])

    return {label: outputs[label].sort_index() for label in labels}


def _prompt_goal(df: pd.DataFrame) -> pd.DataFrame:
    """Ask user for a row goal and trim df to that size keeping highest scores."""
    total = len(df)
    print(f"\n  Total rows available: {total:,}")
    while True:
        raw = input(f"  Enter goal (or press Enter to use all {total:,} rows): ").strip()
        if raw == "":
            return df
        try:
            goal = int(raw)
            if 1 <= goal <= total:
                break
            print(f"  Enter a number between 1 and {total:,}.")
        except ValueError:
            print("  Enter a valid number.")

    # Trim keeping highest scores
    score_cols = [c for c in ["BUYBOX SCORE", "LIKELY DEAL SCORE", "SCORE"] if c in df.columns]
    if score_cols:
        for c in score_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.sort_values(by=score_cols, ascending=[False] * len(score_cols))

    trimmed = df.head(goal).copy()
    dropped = total - len(trimmed)
    print_done(f"  Goal set to {goal:,} rows — {dropped:,} rows dropped before splitting.")
    return trimmed



def _split_1_proportional(df):
    _validate(df, ["FOLIO", "ACTION PLANS", "PROPERTY TYPE"], "Client 1")
    if len(df) < 1000:
        raise ValueError("Need at least 1,000 records.")
    proportions = _prompt_proportions()
    print(f"\n  Splitting into {len(proportions)} templates:")
    for label, pct in proportions:
        print(f"    {label}: {pct}%")
    return _split_balanced(df, proportions)


def _split_2_odd_even(df):
    _validate(df, ["FOLIO", "PROPERTY TYPE"], "Client 2")
    if len(df) < 1000:
        raise ValueError("Need at least 1,000 records.")
    even = df.iloc[::2].copy()
    odd  = df.iloc[1::2].copy()
    return {"template_even": even, "template_odd": odd}


def _split_3_top_x(df):
    _validate(df, ["FOLIO", "ACTION PLANS", "PROPERTY TYPE", "SCORE"], "Client 3")
    if len(df) < 1000:
        raise ValueError("Need at least 1,000 records.")
    top_x       = prompt_int("  Number of top records to include in all templates", 100, 1, len(df))
    top_df      = df.nlargest(top_x, "SCORE")
    remaining   = df.drop(top_df.index)
    proportions = _prompt_proportions()
    splits      = _split_balanced(remaining, proportions)
    # Add top_x records to every template
    for label in splits:
        splits[label] = pd.concat([top_df, splits[label]]).sort_index()
    return splits


def _split_4_property_type(df):
    _validate(df, ["FOLIO", "PROPERTY TYPE"], "Client 4")
    if len(df) < 1000:
        raise ValueError("Need at least 1,000 records.")
    valid   = sorted(df["PROPERTY TYPE"].dropna().unique())
    print(f"  Available property types: {', '.join(valid)}")
    grouped = prompt_yes_no("  Group property types together?", default=False)
    groups  = _parse_groups(valid, "PROPERTY TYPE") if grouped else [(v, [v]) for v in valid]
    out = {}
    for name, types in groups:
        sub = df[df["PROPERTY TYPE"].isin(types)]
        if len(sub) < 10:
            print_warn(f"  Group '{name}' has {len(sub)} records — skipping (min 10).")
            continue
        out[f"type_{name.lower().replace(' ','_')[:25]}"] = sub.sort_index()
    return out


def _split_5_county(df):
    _validate(df, ["FOLIO", "COUNTY", "PROPERTY TYPE"], "Client 5")
    if len(df) < 1000:
        raise ValueError("Need at least 1,000 records.")
    valid   = sorted(df["COUNTY"].dropna().unique())
    print(f"  Available counties: {', '.join(valid)}")
    grouped = prompt_yes_no("  Group counties together?", default=False)
    groups  = _parse_groups(valid, "COUNTY") if grouped else [(v, [v]) for v in valid]
    out = {}
    for name, counties in groups:
        sub = df[df["COUNTY"].isin(counties)]
        if len(sub) < 10:
            print_warn(f"  Group '{name}' has {len(sub)} records — skipping (min 10).")
            continue
        out[f"county_{name.lower().replace(' ','_')[:23]}"] = sub.sort_index()
    return out


def _split_6_action_plan(df):
    _validate(df, ["FOLIO", "ACTION PLANS", "PROPERTY TYPE"], "Client 6")
    out = {}
    for kw in ["30 DAYS", "60 DAYS", "90 DAYS"]:
        sub = df[df["ACTION PLANS"].str.contains(kw, na=False)]
        if sub.empty:
            print_warn(f"  No records for ACTION PLANS containing '{kw}'")
            continue
        out[kw.lower().replace(" ", "_")] = sub.sort_index()
    return out


def _split_7_weekly(df):
    weeks  = prompt_int("  Number of weeks to split into", SPLIT_DEFAULT_WEEKS, 2, 52)
    chunks = np.array_split(df, weeks)
    return {f"week_{i+1}": chunk for i, chunk in enumerate(chunks)}


def _parse_groups(valid_values: list, label: str) -> list[tuple[str, list]]:
    raw    = input(f"  Enter groups (e.g. Group 1: {valid_values[0]}; Group 2: {valid_values[-1]}): ").strip()
    groups = []
    seen   = set()
    for i, part in enumerate(raw.split(";"), 1):
        values  = [v.strip() for v in part.split(":")[-1].split(",") if v.strip()]
        invalid = [v for v in values if v not in valid_values]
        if invalid:
            raise ValueError(f"Invalid {label} values: {', '.join(invalid)}")
        dupes = [v for v in values if v in seen]
        if dupes:
            raise ValueError(f"Duplicate {label} values: {', '.join(dupes)}")
        seen.update(values)
        groups.append((f"group_{i}", values))
    return groups


# ── Mode Menu ──────────────────────────────────────────────────────────────────

MODES = {
    "1": ("Proportional Split (A/B)",         _split_1_proportional),
    "2": ("Odd / Even Split",                 _split_2_odd_even),
    "3": ("Top-X + Balanced Split",           _split_3_top_x),
    "4": ("By Property Type",                 _split_4_property_type),
    "5": ("By County",                        _split_5_county),
    "6": ("By Action Plan (30/60/90 days)",   _split_6_action_plan),
    "7": ("Weekly Split",                     _split_7_weekly),
}


def _prompt_mode() -> str:
    print("\n  Split modes:")
    for k, (desc, _) in MODES.items():
        print(f"    {k}. {desc}")
    while True:
        choice = input("  Select mode (1-7): ").strip()
        if choice in MODES:
            return choice
        print("  Invalid choice.")


# ── Entry Point ────────────────────────────────────────────────────────────────

def run():
    print_header("STEP 2B — SPLIT")

    cadences = prompt_cadence_or_all("cadence to split")
    mode_key = _prompt_mode()
    mode_label, split_fn = MODES[mode_key]

    for cadence in cadences:
        # Resolve input dir per cadence
        input_dir = None
        for folder in [OUT_STEP2, OUT_STEP1]:
            if get_files_by_cadence(folder, cadence):
                input_dir = folder
                break

        if not input_dir:
            print_warn(f"No files found for cadence '{cadence}' in any output folder.")
            continue

        files = get_files_by_cadence(input_dir, cadence)
        if not files:
            print_warn(f"No files found for '{cadence}'")
            continue

        for f in files:
            print_step(f"{f.name}  [{mode_label}]")
            df = read_excel(f)
            if df is None:
                continue
            try:
                # Re-sort by scores to ensure correct order
                score_cols = [c for c in ["BUYBOX SCORE", "LIKELY DEAL SCORE", "SCORE"] if c in df.columns]
                if score_cols:
                    for c in score_cols:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                    df = df.sort_values(by=score_cols, ascending=[False] * len(score_cols))

                sheets = split_fn(df)
                if not sheets:
                    print_warn(f"  No output produced for {f.name}")
                    continue
                out_path = make_output_path(OUT_STEP2, f.name, prefix=f"split_{mode_key}_")
                save_excel_multisheet(sheets, out_path)
                for sheet, sdf in sheets.items():
                    print_done(f"  Sheet '{sheet}': {len(sdf):,} rows")
                print_done(f"Saved → {out_path.name}")
            except ValueError as e:
                print_error(f"  {e}")
            except Exception as e:
                print_error(f"  Unexpected error: {e}")