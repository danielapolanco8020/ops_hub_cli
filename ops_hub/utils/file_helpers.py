import os
import sys
import time
import threading
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

os.system("")  # enable ANSI escape codes on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

_RESET  = "\033[0m"
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_BLUE   = "\033[94m"
_BOLD   = "\033[1m"


# ── File Discovery ─────────────────────────────────────────────────────────────

def get_excel_files(folder: Path) -> list[Path]:
    """Return all .xlsx files in a folder."""
    return sorted(folder.glob("*.xlsx"))


def get_files_by_cadence(folder: Path, cadence: str) -> list[Path]:
    """Return .xlsx files in folder whose name contains the cadence keyword."""
    return sorted(f for f in folder.glob("*.xlsx") if cadence.lower() in f.name.lower())


def get_latest_file(folder: Path) -> Path | None:
    """Return the most recently modified .xlsx file in a folder."""
    files = list(folder.glob("*.xlsx"))
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def get_latest_file_by_cadence(folder: Path, cadence: str) -> Path | None:
    """Return the most recently modified .xlsx file matching the cadence keyword."""
    files = [f for f in folder.glob("*.xlsx") if cadence.lower() in f.name.lower()]
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def resolve_input_dir(folders: list[Path]) -> Path | None:
    """
    Given an ordered list of folders (most processed first),
    return the first one that contains at least one .xlsx file.
    """
    for folder in folders:
        if list(folder.glob("*.xlsx")):
            return folder
    return None


def prompt_cadence_or_all(label: str = "cadence") -> list[str]:
    """
    Prompt user to select one, several, or all cadences.

    Accepts a single number (e.g. '1'), several space-separated numbers for a
    subset (e.g. '1 3' → Direct Mail + SMS), or '4' for all three. Returns the
    selected cadence names in the order the user entered them, deduped.
    """
    from config import CADENCES
    options = {"1": "Direct Mail", "2": "Cold Calling", "3": "SMS"}
    print(f"\n  Select {label}:")
    print("    1. Direct Mail")
    print("    2. Cold Calling")
    print("    3. SMS")
    print("    4. All (separate files per cadence)")
    print("  Tip: enter one or more numbers separated by spaces (e.g. '1 3').")
    while True:
        keys = input("  Enter choice(s) (1-4): ").strip().split()
        if not keys:
            print("  Select at least one option.")
            continue
        if "4" in keys:
            return list(CADENCES)
        invalid = [k for k in keys if k not in options]
        if invalid:
            print(f"  Invalid option(s): {', '.join(invalid)}. Enter numbers from 1-4.")
            continue
        seen, selected = set(), []
        for k in keys:
            name = options[k]
            if name not in seen:
                seen.add(name)
                selected.append(name)
        return selected


def prompt_file_selection(folder: Path, label: str = "file") -> Path | None:
    """
    List all .xlsx files in folder and prompt user to select one.
    Returns the selected Path or None if folder is empty.
    """
    files = get_excel_files(folder)
    if not files:
        print(f"  No Excel files found in {folder}")
        return None
    print(f"\n  Available files in {folder.name}:")
    for i, f in enumerate(files, 1):
        print(f"    {i}. {f.name}")
    while True:
        try:
            idx = int(input(f"  Select {label} (1-{len(files)}): ").strip()) - 1
            if 0 <= idx < len(files):
                return files[idx]
            print(f"  Enter a number between 1 and {len(files)}.")
        except ValueError:
            print("  Enter a valid number.")


def prompt_yes_no(question: str, default: bool = True) -> bool:
    """Prompt a yes/no question. Returns bool."""
    default_str = "Y/n" if default else "y/N"
    while True:
        ans = input(f"{question} [{default_str}]: ").strip().lower()
        if ans == "":       return default
        if ans in ("y", "yes"): return True
        if ans in ("n", "no"):  return False
        print("  Please enter y or n.")


def prompt_int(question: str, default: int, min_val: int = 1, max_val: int = None) -> int:
    """Prompt for an integer with a default value and optional bounds."""
    bounds = f"min {min_val}" + (f", max {max_val}" if max_val else "")
    while True:
        raw = input(f"{question} [default: {default}] ({bounds}): ").strip()
        if raw == "": return default
        try:
            val = int(raw)
            if val < min_val or (max_val and val > max_val):
                print(f"  Enter a value between {min_val} and {max_val or '∞'}.")
            else:
                return val
        except ValueError:
            print("  Enter a valid integer.")


def prompt_float(question: str, default: float) -> float:
    """Prompt for a float with a default value."""
    while True:
        raw = input(f"{question} [default: {default}]: ").strip()
        if raw == "": return default
        try:
            return float(raw)
        except ValueError:
            print("  Enter a valid number (e.g. 0.65).")


# ── File I/O ───────────────────────────────────────────────────────────────────

def _pad_zip(val) -> str:
    """Zero-pad a ZIP value to 5 digits if it was read as a number (e.g. 1234 → '01234')."""
    if pd.isna(val):
        return val
    s = str(val).strip()
    if '.' in s:
        s = s.split('.')[0]
    return s.zfill(5) if s.isdigit() and len(s) < 5 else s


def _fix_zip_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Restore leading zeros in any column whose name contains 'ZIP'."""
    for col in [c for c in df.columns if "ZIP" in c.upper()]:
        df[col] = df[col].apply(_pad_zip)
    return df


# ── Excel engine resolution (single source of truth) ────────────────────────────
# I/O CONVENTION — keep every step (current and future) consistent:
#   • READ  xlsx via  read_excel()  /  read_many_parallel()   — never pd.read_excel
#   • WRITE xlsx via  save_excel()  /  save_excel_multisheet() — never df.to_excel
# Engines come from config (READ_ENGINE / WRITE_ENGINE). The three resolvers below
# are the ONLY place an engine or its options are applied, so any new writer, sheet
# type, or step inherits the same behavior for free — add engine-specific tweaks
# here, not in the steps.

def _resolve_read_engine() -> str:
    try:
        from config import READ_ENGINE
        return READ_ENGINE
    except Exception:
        return "openpyxl"


def _resolve_write_engine() -> str:
    try:
        from config import WRITE_ENGINE
        return WRITE_ENGINE
    except Exception:
        return "openpyxl"


def _writer_kwargs(engine: str) -> dict:
    """Per-engine ExcelWriter options, centralized so single- and multi-sheet
    writes (and any future writer) stay identical. xlsxwriter: disable auto-URL
    conversion so URL-like cells (e.g. LINK PROPERTIES) are written as plain text —
    matching openpyxl and dodging Excel's 65,530-hyperlinks-per-sheet limit."""
    if engine == "xlsxwriter":
        return {"engine_kwargs": {"options": {"strings_to_urls": False}}}
    return {}


def read_excel(path: Path) -> pd.DataFrame | None:
    """Read an Excel file safely. Returns None on failure.

    Engine is config.READ_ENGINE (default "calamine" — a Rust reader ~6x faster
    than openpyxl with identical output). Falls back to openpyxl if unavailable.
    """
    engine = _resolve_read_engine()
    try:
        df = pd.read_excel(path, engine=engine)
        return _fix_zip_columns(df)
    except Exception as e:
        print(f"  [ERROR] Could not read {path.name}: {e}")
        return None


def read_many_parallel(paths: list[Path], max_workers: int | None = None) -> "dict[Path, pd.DataFrame | None]":
    """Read several Excel files concurrently and return {path: DataFrame or None}.

    Only the *reading* is parallelized. The returned dict preserves the input
    order, so a caller's existing sequential, prompt-driven loop stays exactly
    the same — it just pulls each already-loaded frame out of the dict instead of
    waiting on disk between files. A value is None when that file failed to read,
    identical to what read_excel() returns.

    Threads (not processes) are used on purpose: xlsx reads spend their time in
    file I/O and zlib decompression — both release the GIL — so threads overlap
    that wait without the pickle/spawn cost of a process pool. The speedup is
    largest once reads move to a GIL-free engine (e.g. calamine); with openpyxl
    the gain comes mainly from overlapping I/O and decompression.
    """
    if not paths:
        return {}
    if max_workers is None:
        max_workers = min(len(paths), (os.cpu_count() or 4))
    results: "dict[Path, pd.DataFrame | None]" = {p: None for p in paths}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(read_excel, p): p for p in paths}
        for future, p in futures.items():
            try:
                results[p] = future.result()
            except Exception as e:
                print(f"  [ERROR] Could not read {p.name}: {e}")
                results[p] = None
    return results


# ── Write progress heartbeat ────────────────────────────────────────────────
# A single df.to_excel() is one blocking call with no per-row callback, so a true
# percentage bar is impossible. Instead the write runs on a worker thread and this
# helper emits an indeterminate progress line every WRITE_HEARTBEAT_INTERVAL
# seconds, so long writes (the big xlsx files) visibly show they are alive. Writes
# that finish before the first interval print nothing, keeping small writes quiet.
WRITE_HEARTBEAT_INTERVAL = 10.0  # seconds between progress updates


def _write_with_heartbeat(label: str, n_rows: int, write_fn,
                          interval: float = WRITE_HEARTBEAT_INTERVAL):
    """Run write_fn() while printing a progress line every `interval` seconds.
    Returns write_fn()'s result; exceptions propagate to the caller unchanged."""
    done  = threading.Event()
    beats = [0]
    t0    = time.time()

    def _beat():
        # done.wait() returns True once the write finishes, False on each timeout.
        while not done.wait(interval):
            beats[0] += 1
            elapsed  = time.time() - t0
            filled   = min(beats[0], 20)
            bar      = "█" * filled + "░" * (20 - filled)
            print(f"\r  {_BLUE}⏳ writing {label} [{bar}] {elapsed:4.0f}s "
                  f"({n_rows:,} rows){_RESET}", end="", flush=True)

    th = threading.Thread(target=_beat, daemon=True)
    th.start()
    ok = False
    try:
        result = write_fn()
        ok = True
        return result
    finally:
        done.set()
        th.join(timeout=2)
        if beats[0] > 0:                       # only if at least one heartbeat showed
            elapsed = time.time() - t0
            if ok:
                print(f"\r  {_GREEN}✓ wrote {label} in {elapsed:.0f}s "
                      f"({n_rows:,} rows){_RESET}" + " " * 24)
            else:
                print()                        # newline so a following error prints cleanly


def save_excel(df: pd.DataFrame, path: Path, index: bool = False) -> bool:
    """Save a DataFrame to a single-sheet Excel file. Engine: config.WRITE_ENGINE.
    Long writes show a progress heartbeat every WRITE_HEARTBEAT_INTERVAL seconds."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        engine = _resolve_write_engine()

        def _do():
            with pd.ExcelWriter(path, engine=engine, **_writer_kwargs(engine)) as writer:
                df.to_excel(writer, index=index)
            return True

        return _write_with_heartbeat(path.name, len(df), _do)
    except Exception as e:
        print(f"  [ERROR] Could not save {path.name}: {e}")
        return False


def save_excel_multisheet(sheets: dict[str, pd.DataFrame], path: Path) -> bool:
    """Save multiple DataFrames as sheets in one Excel file. Engine: config.WRITE_ENGINE.
    Long writes show a progress heartbeat every WRITE_HEARTBEAT_INTERVAL seconds."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        engine = _resolve_write_engine()
        n_rows = sum(len(df) for df in sheets.values())

        def _do():
            with pd.ExcelWriter(path, engine=engine, **_writer_kwargs(engine)) as writer:
                for name, df in sheets.items():
                    df.to_excel(writer, sheet_name=name[:31], index=False)
            return True

        return _write_with_heartbeat(path.name, n_rows, _do)
    except Exception as e:
        print(f"  [ERROR] Could not save {path.name}: {e}")
        return False


# ── Output Path Helpers ────────────────────────────────────────────────────────

def make_output_path(output_dir: Path, original_name: str, prefix: str = "", suffix: str = "") -> Path:
    """Build an output file path from an original filename with optional prefix/suffix."""
    stem = Path(original_name).stem
    name = f"{prefix}{stem}{suffix}.xlsx"
    return output_dir / name


def format_k(count: int) -> str:
    """Format a row count as a K string e.g. 5500 → '5.5K'."""
    k = count / 1000
    return f"{int(k)}K" if k == int(k) else f"{round(k, 1)}K"


# ── Column Helpers ─────────────────────────────────────────────────────────────

def find_column(df: pd.DataFrame, possible_names: list[str]) -> str | None:
    """Find a column in df matching any of the possible names (case-insensitive)."""
    def normalize(s): return s.lower().replace(" ", "").replace("_", "").replace("-", "")
    targets = [normalize(n) for n in possible_names]
    for col in df.columns:
        if normalize(col) in targets:
            return col
    return None


def check_missing_columns(df: pd.DataFrame, required: list[str], label: str = "") -> list[str]:
    """Return list of required columns missing from df. Prints warnings."""
    missing = [c for c in required if c not in df.columns]
    if missing and label:
        print(f"  [WARN] {label} missing columns: {', '.join(missing)}")
    return missing


# ── Progress Helpers ───────────────────────────────────────────────────────────

def print_header(title: str):
    print("\n" + f"{_BOLD}" + "=" * 60 + f"{_RESET}")
    print(f"  {_BOLD}{title}{_RESET}")
    print(f"{_BOLD}" + "=" * 60 + f"{_RESET}")


def print_step(msg: str):
    print(f"\n  {_BLUE}→ {msg}{_RESET}")


def print_done(msg: str):
    print(f"  {_GREEN}✓ {msg}{_RESET}")


def print_warn(msg: str):
    print(f"  {_YELLOW}⚠  {msg}{_RESET}")


def print_error(msg: str):
    print(f"  {_RED}✗ {msg}{_RESET}")