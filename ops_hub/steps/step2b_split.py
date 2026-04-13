import warnings
import numpy as np
import pandas as pd
from pathlib import Path

from config import OUT_STEP2, OUT_STEP1, SPLIT_VALID_PLANS, SPLIT_DEFAULT_WEEKS
from utils.file_helpers import (
    get_files_by_cadence, read_excel, save_excel_multisheet,
    prompt_cadence_or_all, prompt_yes_no, prompt_int,
    resolve_input_dir,
    print_header, print_step, print_done, print_warn, print_error,
    make_output_path,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _validate(df, cols, label):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{label}: missing columns: {', '.join(missing)}")


def _prompt_proportions() -> tuple[float, float]:
    use_default = prompt_yes_no("  Use default 50/50 split?", default=True)
    if use_default:
        return 50.0, 50.0
    while True:
        raw = input("  Enter proportions A, B (must sum to 100, e.g. 60,40): ").strip()
        try:
            a, b = map(float, raw.split(","))
            if a <= 0 or b <= 0:
                print("  Both values must be positive.")
                continue
            if abs(a + b - 100) > 0.001:
                print("  Values must sum to 100.")
                continue
            return a, b
        except ValueError:
            print("  Enter two numbers separated by a comma.")


def _split_balanced(df, prop_a, prop_b, label_a, label_b):
    total    = len(df)
    target_a = int(total * prop_a / 100)
    target_b = total - target_a

    plan_counts = df["ACTION PLANS"].value_counts().to_dict()
    exp_a = {p: int(c * prop_a / 100) for p, c in plan_counts.items()}
    exp_b = {p: c - exp_a[p] for p, c in plan_counts.items()}

    diff = target_a - sum(exp_a.values())
    for p, c in sorted(plan_counts.items(), key=lambda x: -x[1]):
        if diff == 0: break
        if diff > 0:
            add = min(diff, c - exp_a[p])
            exp_a[p] += add; exp_b[p] -= add; diff -= add
        else:
            rem = min(-diff, exp_a[p])
            exp_a[p] -= rem; exp_b[p] += rem; diff += rem

    out_a, out_b = pd.DataFrame(), pd.DataFrame()
    for plan in SPLIT_VALID_PLANS:
        plan_df = df[df["ACTION PLANS"] == plan]
        n_a = exp_a.get(plan, 0)
        n_b = exp_b.get(plan, 0)
        s_a = plan_df.sample(n=n_a, random_state=42) if n_a > 0 else pd.DataFrame()
        s_b = plan_df.drop(s_a.index).head(n_b) if n_b > 0 else pd.DataFrame()
        out_a = pd.concat([out_a, s_a])
        out_b = pd.concat([out_b, s_b])

    return {label_a: out_a.sort_index(), label_b: out_b.sort_index()}


# ── Split Modes ────────────────────────────────────────────────────────────────

def _split_1_proportional(df):
    _validate(df, ["FOLIO", "ACTION PLANS", "PROPERTY TYPE"], "Client 1")
    if len(df) < 1000:
        raise ValueError("Need at least 1,000 records.")
    prop_a, prop_b = _prompt_proportions()
    return _split_balanced(df, prop_a, prop_b, "template_a", "template_b")


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
    top_x     = prompt_int("  Number of top records to include in both templates", 100, 1, len(df))
    top_df    = df.nlargest(top_x, "SCORE")
    remaining = df.drop(top_df.index)
    splits    = _split_balanced(remaining, 50, 50, "template_a", "template_b")
    splits["template_a"] = pd.concat([top_df, splits["template_a"]]).sort_index()
    splits["template_b"] = pd.concat([top_df, splits["template_b"]]).sort_index()
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
        input_dir = resolve_input_dir([OUT_STEP2, OUT_STEP1])
        if not input_dir:
            print_warn(f"No processed files found for '{cadence}'")
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