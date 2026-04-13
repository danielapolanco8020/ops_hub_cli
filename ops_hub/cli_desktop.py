"""
Real Estate Data Pipeline CLI
==============================
Entry point. Runs the full pipeline interactively.

Usage:
    python main.py
"""

from utils.file_helpers import print_header, print_step, print_done, prompt_yes_no

from steps import (
    step1_clean,
    step2a_merge,
    step2b_split,
    step2c_tagcheck,
    step2d_zestimate,
    step2e_dataflick,
    step2f_canadian,
    step2g_namesplit,
    step2h_buybox,
    step3_audit,
    step4_skiptrace,
)

OPTIONAL_STEPS = {
    "a": ("Merge (consolidate by cadence)",         step2a_merge.run),
    "b": ("Split (7 modes incl. weekly)",           step2b_split.run),
    "c": ("Tag Check (Skiptrace date analysis)",    step2c_tagcheck.run),
    "d": ("Zestimate Tool (standalone)",            step2d_zestimate.run),
    "e": ("Dataflick Format",                       step2e_dataflick.run),
    "f": ("Canadian Mail Filter",                   step2f_canadian.run),
    "g": ("Name Cleaner & Splitter",                step2g_namesplit.run),
    "h": ("BuyBox HQ (DM Count + Cash Offers)",     step2h_buybox.run),
}


def _prompt_optional_steps() -> list[str]:
    print("\n  Optional steps (select all that apply):")
    for key, (label, _) in OPTIONAL_STEPS.items():
        print(f"    {key.upper()}. {label}")
    print("\n  Enter step letters in the order you want them to run.")
    print("  Example: a b g   or   b f   or just press Enter to skip all.\n")
    while True:
        raw     = input("  Your selection: ").strip().lower()
        if raw == "":
            return []
        keys    = raw.split()
        invalid = [k for k in keys if k not in OPTIONAL_STEPS]
        if invalid:
            print(f"  Invalid option(s): {', '.join(invalid)}. Choose from: {', '.join(OPTIONAL_STEPS.keys())}")
            continue
        return keys


def _run_optional_steps(keys: list[str]):
    for key in keys:
        label, fn = OPTIONAL_STEPS[key]
        print_step(f"Running optional step {key.upper()}: {label}")
        try:
            fn()
        except KeyboardInterrupt:
            print("\n  Step interrupted by user. Continuing to next step...")
        except Exception as e:
            print(f"\n  [ERROR] Step {key.upper()} failed: {e}")
            if not prompt_yes_no("  Continue with remaining steps?", default=True):
                raise


def main():
    print_header("REAL ESTATE DATA PIPELINE")
    print("  Welcome. This tool will guide you through the full pipeline.")
    print("  Press Ctrl+C at any time to cancel the current step.\n")

    # ── Step 1: Clean (mandatory) ──────────────────────────────────────────────
    print_step("Starting Step 1: Clean (mandatory)")
    try:
        step1_clean.run()
    except Exception as e:
        print(f"\n  [FATAL] Step 1 failed: {e}")
        print("  Cannot continue without clean data. Exiting.")
        return

    # ── Step 2: Optional steps ─────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  STEP 2 — OPTIONAL OPERATIONS")
    print("=" * 60)
    selected = _prompt_optional_steps()

    if not selected:
        print_done("  No optional steps selected — skipping Step 2.")
    else:
        print_step(f"Running {len(selected)} optional step(s): {', '.join(s.upper() for s in selected)}")
        try:
            _run_optional_steps(selected)
        except Exception as e:
            print(f"\n  [ERROR] Pipeline stopped during optional steps: {e}")
            if not prompt_yes_no("  Continue to Step 3 anyway?", default=True):
                return

    # ── Step 3: Audit (mandatory) ──────────────────────────────────────────────
    print("\n" + "=" * 60)
    print_step("Starting Step 3: Audit & Phone Count (mandatory)")
    try:
        step3_audit.run()
    except Exception as e:
        print(f"\n  [ERROR] Step 3 failed: {e}")

    # ── Step 4: Skiptrace pre-export (optional) ────────────────────────────────
    print("\n" + "=" * 60)
    run_skip = prompt_yes_no("  Run Step 4: Skiptrace Pre-Export?", default=False)
    if run_skip:
        try:
            step4_skiptrace.run()
        except Exception as e:
            print(f"\n  [ERROR] Step 4 failed: {e}")

    # ── Done ───────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)
    print("  Check the output/ folder for all generated files.")
    print("  Check merge/zestimate/ and merge/skiptrace/ for provider result drops.\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Pipeline cancelled by user.")
