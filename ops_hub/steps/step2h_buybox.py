import pandas as pd
from pathlib import Path

from config import (
    OUT_STEP2, OUT_STEP1,
    BUYBOX_LOW_RATE, BUYBOX_HIGH_RATE, BUYBOX_OFFER_RATE, BUYBOX_LOW_LIMIT,
)
from utils.file_helpers import (
    get_excel_files, read_excel, save_excel_multisheet,
    prompt_file_selection, prompt_yes_no, prompt_float, prompt_int,
    resolve_input_dir,
    print_header, print_step, print_done, print_warn, print_error,
    make_output_path,
)


def _prompt_rates() -> tuple[float, float, float, int]:
    print(f"\n  Current cash offer settings:")
    print(f"    DM Count 0–{BUYBOX_LOW_LIMIT}  → {int(BUYBOX_OFFER_RATE*100)}% offer + {int(BUYBOX_LOW_RATE*100)}% offer")
    print(f"    DM Count {BUYBOX_LOW_LIMIT+1}+   → {int(BUYBOX_OFFER_RATE*100)}% offer + {int(BUYBOX_HIGH_RATE*100)}% offer")

    use_defaults = prompt_yes_no("  Use these default rates?", default=True)
    if use_defaults:
        return BUYBOX_OFFER_RATE, BUYBOX_LOW_RATE, BUYBOX_HIGH_RATE, BUYBOX_LOW_LIMIT

    offer_rate = prompt_float("  Primary offer rate (e.g. 0.90 for 90%)", BUYBOX_OFFER_RATE)
    low_rate   = prompt_float(f"  Secondary rate for DM Count 0–N (e.g. {BUYBOX_LOW_RATE})", BUYBOX_LOW_RATE)
    high_rate  = prompt_float(f"  Secondary rate for DM Count N+1+ (e.g. {BUYBOX_HIGH_RATE})", BUYBOX_HIGH_RATE)
    low_limit  = prompt_int(
        "  DM Count threshold — counts 0 to N use low rate, N+1+ use high rate",
        BUYBOX_LOW_LIMIT, min_val=0
    )
    return offer_rate, low_rate, high_rate, low_limit


def _process(
    df: pd.DataFrame,
    offer_rate: float,
    low_rate: float,
    high_rate: float,
    low_limit: int,
    decrement: bool,
) -> dict[str, pd.DataFrame]:

    dm_col    = "MARKETING DM COUNT"
    value_col = "VALUE"

    if dm_col not in df.columns:
        raise ValueError(f"Missing column: '{dm_col}'")
    if value_col not in df.columns:
        raise ValueError(f"Missing column: '{value_col}'")

    df = df.copy()
    df.drop(columns=["ESTIMATED CASH OFFER"], errors="ignore", inplace=True)

    if decrement:
        df[dm_col] = df[dm_col] - 1

    sheets: dict[str, pd.DataFrame] = {}

    for count in range(low_limit + 1):
        rate       = low_rate if count <= low_limit else high_rate
        second_col = f"CASH OFFER {int(rate*100)}%"
        filtered   = df[df[dm_col] == count].copy()
        if filtered.empty:
            continue
        filtered[f"CASH OFFER {int(offer_rate*100)}%"] = filtered[value_col] * offer_rate
        filtered[second_col]                            = filtered[value_col] * rate
        cols     = filtered.columns.tolist()
        dm_idx   = cols.index(dm_col)
        offer1   = f"CASH OFFER {int(offer_rate*100)}%"
        new_cols = (
            cols[:dm_idx + 1] +
            [offer1, second_col] +
            [c for c in cols if c not in (cols[:dm_idx + 1] + [offer1, second_col])]
        )
        sheets[f"DM Count {count}"] = filtered[new_cols]

    above_df = df[df[dm_col] >= low_limit + 1].copy()
    if not above_df.empty:
        rate       = high_rate
        second_col = f"CASH OFFER {int(rate*100)}%"
        above_df[f"CASH OFFER {int(offer_rate*100)}%"] = above_df[value_col] * offer_rate
        above_df[second_col]                            = above_df[value_col] * rate
        cols     = above_df.columns.tolist()
        dm_idx   = cols.index(dm_col)
        offer1   = f"CASH OFFER {int(offer_rate*100)}%"
        new_cols = (
            cols[:dm_idx + 1] +
            [offer1, second_col] +
            [c for c in cols if c not in (cols[:dm_idx + 1] + [offer1, second_col])]
        )
        sheets[f"DM Count {low_limit+1} or more"] = above_df[new_cols]

    return sheets


def run():
    print_header("STEP 2H — BUYBOX HQ")

    input_dir = resolve_input_dir([OUT_STEP2, OUT_STEP1])
    if not input_dir:
        print_error("No processed files found in any output folder.")
        return
    f = prompt_file_selection(input_dir, "file to process")
    if f is None:
        return

    df = read_excel(f)
    if df is None:
        return

    offer_rate, low_rate, high_rate, low_limit = _prompt_rates()

    decrement = prompt_yes_no("\n  Decrement MARKETING DM COUNT by 1?", default=False)
    if decrement:
        print_done("  DM Count will be decremented by 1.")

    try:
        sheets = _process(df, offer_rate, low_rate, high_rate, low_limit, decrement)
    except ValueError as e:
        print_error(str(e))
        return

    if not sheets:
        print_warn("No output sheets produced — check DM Count values in the file.")
        return

    out_path = make_output_path(OUT_STEP2, f.name, prefix="buybox_")
    save_excel_multisheet(sheets, out_path)

    for sheet_name, sdf in sheets.items():
        print_done(f"  Sheet '{sheet_name}': {len(sdf):,} rows")

    print_done(f"BuyBox HQ complete → {out_path.name}")