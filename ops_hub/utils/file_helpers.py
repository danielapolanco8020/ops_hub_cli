import os
import pandas as pd
from pathlib import Path


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
    Prompt user to select a cadence or all.
    Returns a list of cadence strings e.g. ['Direct Mail'] or all three.
    """
    from config import CADENCES
    print(f"\n  Select {label}:")
    print("    1. Direct Mail")
    print("    2. Cold Calling")
    print("    3. SMS")
    print("    4. All (separate files per cadence)")
    while True:
        choice = input("  Enter choice (1-4): ").strip()
        if choice == "1":   return ["Direct Mail"]
        elif choice == "2": return ["Cold Calling"]
        elif choice == "3": return ["SMS"]
        elif choice == "4": return CADENCES
        else: print("  Invalid choice. Enter 1, 2, 3 or 4.")


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

def read_excel(path: Path) -> pd.DataFrame | None:
    """Read an Excel file safely. Returns None on failure."""
    try:
        return pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print(f"  [ERROR] Could not read {path.name}: {e}")
        return None


def save_excel(df: pd.DataFrame, path: Path, index: bool = False) -> bool:
    """Save a DataFrame to Excel. Returns True on success."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(path, index=index, engine="openpyxl")
        return True
    except Exception as e:
        print(f"  [ERROR] Could not save {path.name}: {e}")
        return False


def save_excel_multisheet(sheets: dict[str, pd.DataFrame], path: Path) -> bool:
    """Save multiple DataFrames to a single Excel file as separate sheets."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for name, df in sheets.items():
                df.to_excel(writer, sheet_name=name[:31], index=False)
        return True
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
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def print_step(msg: str):
    print(f"\n  → {msg}")


def print_done(msg: str):
    print(f"  ✓ {msg}")


def print_warn(msg: str):
    print(f"  ⚠  {msg}")


def print_error(msg: str):
    print(f"  ✗ {msg}")