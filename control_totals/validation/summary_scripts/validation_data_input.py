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
    load_legacy_pop()         -> DataFrame (or None) from legacy total_pop_estimates_all_years.csv
    load_legacy_units()       -> DataFrame (or None) from legacy units_estimates_all_years.csv
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


def _legacy_csv(name: str) -> pd.DataFrame | None:
    cfg = load_config()
    if cfg["legacy_dir"] is None:
        return None
    path = cfg["legacy_dir"] / "output" / name
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return None
    return df if not df.empty else None


@functools.lru_cache(maxsize=1)
def load_legacy_pop() -> pd.DataFrame | None:
    """Legacy pipeline total_pop_estimates_all_years.csv (or None)."""
    return _legacy_csv("total_pop_estimates_all_years.csv")


@functools.lru_cache(maxsize=1)
def load_legacy_units() -> pd.DataFrame | None:
    """Legacy pipeline units_estimates_all_years.csv (or None)."""
    return _legacy_csv("units_estimates_all_years.csv")


__all__ = [
    "load_config",
    "load_settings",
    "load_controls",
    "load_unrolled",
    "load_unrolled_regional",
    "load_targets",
    "control_id_lookup",
    "load_capacity",
    "load_legacy_pop",
    "load_legacy_units",
    "available_years",
]
