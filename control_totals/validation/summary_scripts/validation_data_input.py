"""Cached loaders for validation-dashboard chapter notebooks.

The dashboard is pointed at a particular pipeline run via
`control_totals/validation/config.yaml`. This module resolves that config
and exposes one function per artifact:

    load_config()             -> dict
    load_settings()           -> dict (the run's configs/settings.yaml)
    load_controls(sheet=...)  -> DataFrame from Control-Totals-LUVit.xlsx
    load_unrolled()           -> long-format yearly controls by subreg
    load_unrolled_regional()  -> long-format yearly regional totals
    load_targets(sheet=...)   -> DataFrame from TargetsRebasedOutput.xlsx
    load_capacity()           -> DataFrame from CapacityPclNoSampling*.csv
    load_legacy_unrolled()    -> long-format legacy controls (control_id, year, ...) or None
    load_legacy_pop()         -> regional legacy population by year, or None
    load_legacy_units()       -> regional legacy households by year, or None
    load_legacy_emp()         -> regional legacy employment by year, or None
    available_years()         -> sorted list of stepped years in the controls
"""

from __future__ import annotations

import functools
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

# This file lives at:
#   control_totals/validation/summary_scripts/validation_data_input.py
# config.yaml lives at:
#   control_totals/validation/config.yaml
_VALIDATION_DIR = Path(__file__).resolve().parent.parent
_CONFIG_PATH = _VALIDATION_DIR / "config.yaml"


# ---------------------------------------------------------------------------
# Config / paths
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=1)
def load_config() -> dict[str, Any]:
    """Load and resolve the dashboard's config.yaml."""
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Validation config not found: {_CONFIG_PATH}. "
            "Create it from the template in index.qmd."
        )
    with _CONFIG_PATH.open() as f:
        cfg = yaml.safe_load(f) or {}

    example_path = cfg.get("example_path")
    if not example_path:
        raise ValueError("config.yaml must set 'example_path'.")
    cfg["example_dir"] = (_VALIDATION_DIR / example_path).resolve()

    legacy_path = cfg.get("legacy_path")
    cfg["legacy_dir"] = (_VALIDATION_DIR / legacy_path).resolve() if legacy_path else None

    cfg["output_dir"] = cfg["example_dir"] / "output"
    cfg["configs_dir"] = cfg["example_dir"] / "configs"
    return cfg


@functools.lru_cache(maxsize=1)
def load_settings() -> dict[str, Any]:
    """Load the example run's configs/settings.yaml."""
    cfg = load_config()
    settings_path = cfg["configs_dir"] / "settings.yaml"
    with settings_path.open() as f:
        return yaml.safe_load(f) or {}


# ---------------------------------------------------------------------------
# Output workbooks
# ---------------------------------------------------------------------------

def _controls_path() -> Path:
    settings = load_settings()
    fname = settings.get("rebased_targets", {}).get(
        "output_controls_file", "Control-Totals-LUVit.xlsx"
    )
    return load_config()["output_dir"] / fname


def _targets_path() -> Path:
    settings = load_settings()
    fname = settings.get("rebased_targets", {}).get(
        "output_targets_file", "TargetsRebasedOutput.xlsx"
    )
    return load_config()["output_dir"] / fname


@functools.lru_cache(maxsize=16)
def load_controls(sheet: str = "unrolled") -> pd.DataFrame:
    """Load a sheet from Control-Totals-LUVit.xlsx.

    Common sheets:
      - 'HHPop', 'HH', 'Emp', 'Pop'  : wide format (one column per stepped year)
      - 'unrolled'                   : long format by subreg_id and year
      - 'unrolled_regional'          : long format, regional totals by year
    """
    path = _controls_path()
    if not path.exists():
        raise FileNotFoundError(f"Control totals file not found: {path}")
    return pd.read_excel(path, sheet_name=sheet)


@functools.lru_cache(maxsize=1)
def load_unrolled() -> pd.DataFrame:
    """Long-format yearly controls by subregion (control_id)."""
    df = load_controls("unrolled").copy()
    # Standardize column name; some pipelines use 'subreg_id', some 'control_id'.
    if "subreg_id" in df.columns and "control_id" not in df.columns:
        df = df.rename(columns={"subreg_id": "control_id"})
    return df


@functools.lru_cache(maxsize=1)
def load_unrolled_regional() -> pd.DataFrame:
    """Long-format yearly regional totals."""
    return load_controls("unrolled_regional").copy()


@functools.lru_cache(maxsize=16)
def load_targets(sheet: str = "CityPop") -> pd.DataFrame:
    """Load a sheet from TargetsRebasedOutput.xlsx.

    Common sheets:
      - 'RGs'      : RG-level targets (Pop2350, HH50, Emp50, ...)
      - 'CityPop'  : city / control_id population targets
      - 'CityHH'   : city / control_id household targets
      - 'CityEmp'  : city / control_id employment targets
    """
    path = _targets_path()
    if not path.exists():
        raise FileNotFoundError(f"Rebased targets file not found: {path}")
    return pd.read_excel(path, sheet_name=sheet)


@functools.lru_cache(maxsize=1)
def control_id_lookup() -> pd.DataFrame:
    """Map control_id -> (RGID, county_id, Juris) using the targets workbook."""
    df = load_targets("CityPop")
    return df[["control_id", "RGID", "county_id", "Juris"]].drop_duplicates()


def available_years() -> list[int]:
    """Stepped years present in the unrolled controls."""
    return sorted(load_unrolled()["year"].unique().tolist())


# ---------------------------------------------------------------------------
# Other artifacts
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=1)
def load_capacity() -> pd.DataFrame | None:
    """Parcel capacity CSV; returns None if not present."""
    settings = load_settings()
    fname = settings.get("split_hct", {}).get(
        "capacity_file", "CapacityPclNoSampling_res50.csv"
    )
    path = load_config()["output_dir"] / fname
    if not path.exists():
        return None
    return pd.read_csv(path)


def _find_legacy_workbook() -> Path | None:
    """Locate the legacy LUVit control-totals workbook in the configured legacy dir.

    Looks for any file matching ``LUVit_ct_by_tod_generator-*.xlsx`` (the
    legacy R pipeline names them with a date stamp), then falls back to
    ``Control-Totals-LUVit.xlsx``. Returns the most recently modified match
    or None if nothing is found.
    """
    cfg = load_config()
    legacy_dir = cfg["legacy_dir"]
    if legacy_dir is None:
        return None
    out = legacy_dir / "output"
    if not out.exists():
        return None
    candidates = sorted(out.glob("LUVit_ct_by_tod_generator-*.xlsx"))
    if not candidates:
        fallback = out / "Control-Totals-LUVit.xlsx"
        return fallback if fallback.exists() else None
    return max(candidates, key=lambda p: p.stat().st_mtime)


@functools.lru_cache(maxsize=1)
def _load_legacy_unrolled() -> pd.DataFrame | None:
    """Load the legacy 'unrolled' (long, by subreg/control_id) sheet."""
    path = _find_legacy_workbook()
    if path is None:
        return None
    try:
        xl = pd.ExcelFile(path)
    except Exception:
        return None

    # Prefer 'unrolled' / 'unrolled regional' if present; else build from wide
    # per-indicator sheets.
    if "unrolled" in xl.sheet_names:
        df = pd.read_excel(xl, "unrolled").copy()
        if "subreg_id" in df.columns and "control_id" not in df.columns:
            df = df.rename(columns={"subreg_id": "control_id"})
        return df

    # Fall back to wide sheets ('HHPop', 'HH', 'Emp', optionally 'Pop').
    sheet_to_col = {
        "HHPop": "total_hhpop",
        "HH": "total_hh",
        "Emp": "total_emp",
        "Pop": "total_pop",
    }
    pieces = []
    for sheet, col in sheet_to_col.items():
        if sheet not in xl.sheet_names:
            continue
        wide = pd.read_excel(xl, sheet)
        id_col = "control_id" if "control_id" in wide.columns else "subreg_id"
        long = wide.melt(id_vars=[id_col], var_name="year", value_name=col)
        long["year"] = pd.to_numeric(long["year"], errors="coerce").astype("Int64")
        long = long.rename(columns={id_col: "control_id"})
        pieces.append(long.set_index(["control_id", "year"]))
    if not pieces:
        return None
    out = pd.concat(pieces, axis=1).reset_index()
    return out


@functools.lru_cache(maxsize=1)
def load_legacy_unrolled() -> pd.DataFrame | None:
    """Public accessor: legacy long-format controls by control_id and year.

    Columns: ``control_id``, ``year``, and any of ``total_pop``,
    ``total_hh``, ``total_hhpop``, ``total_emp`` that the legacy workbook
    contained. Returns ``None`` if no legacy workbook is configured.
    """
    df = _load_legacy_unrolled()
    return None if df is None else df.copy()


def _legacy_indicator_by_year(col: str) -> pd.DataFrame | None:
    df = _load_legacy_unrolled()
    if df is None or col not in df.columns:
        return None
    return (
        df[["year", col]]
        .dropna()
        .groupby("year", as_index=False)
        .sum()
    )


@functools.lru_cache(maxsize=1)
def load_legacy_pop() -> pd.DataFrame | None:
    """Legacy total population by year (regional). Columns: year, total_pop."""
    return _legacy_indicator_by_year("total_pop")


@functools.lru_cache(maxsize=1)
def load_legacy_units() -> pd.DataFrame | None:
    """Legacy total households by year (regional). Columns: year, total_hh."""
    return _legacy_indicator_by_year("total_hh")


@functools.lru_cache(maxsize=1)
def load_legacy_emp() -> pd.DataFrame | None:
    """Legacy total employment by year (regional). Columns: year, total_emp."""
    return _legacy_indicator_by_year("total_emp")


__all__ = [
    "load_config",
    "load_settings",
    "load_controls",
    "load_unrolled",
    "load_unrolled_regional",
    "load_targets",
    "control_id_lookup",
    "load_capacity",
    "load_legacy_unrolled",
    "load_legacy_pop",
    "load_legacy_units",
    "load_legacy_emp",
    "available_years",
]
