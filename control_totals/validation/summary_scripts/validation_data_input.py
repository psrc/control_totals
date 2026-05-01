"""Multi-forecast data loaders for the validation dashboard.

Forecasts are listed in ``control_totals/validation/config.yaml`` under
``forecasts:``. Two backend types are supported:

- ``type: pipeline`` — reads ``rebased_control_totals_*`` tables from the
  example's ``data/pipeline.h5`` via :class:`control_totals.util.Pipeline`.
- ``type: spreadsheet`` — reads sheet-per-indicator from a legacy
  Control-Totals workbook. Year columns are detected automatically.

Each forecast carries its own ``[base_year, targets_end_year, end_year]``
year set. Helpers gracefully handle missing indicators by returning a
"Not available" placeholder DataFrame, so a chapter still renders all
tabs even when one forecast (e.g. the legacy spreadsheet) lacks
``rebased_control_totals_units``.
"""

from __future__ import annotations

import functools
import sys
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

# This file lives at:
#   control_totals/validation/summary_scripts/validation_data_input.py
_THIS_FILE = Path(__file__).resolve()
_VALIDATION_DIR = _THIS_FILE.parent.parent
_CONFIG_PATH = _VALIDATION_DIR / "config.yaml"
_PROJECT_ROOT = _VALIDATION_DIR.parent  # control_totals/

# Make `control_totals.util` importable when the notebook is executed from any cwd.
if str(_PROJECT_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT.parent))
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from util import Pipeline  # noqa: E402


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Counties in display order: King, Pierce, Snohomish, Kitsap.
COUNTIES: list[tuple[int, str]] = [
    (53033, "King"),
    (53053, "Pierce"),
    (53061, "Snohomish"),
    (53035, "Kitsap"),
]

# Map indicator key -> pipeline table name.
_PIPELINE_TABLES = {
    "pop": "rebased_control_totals_pop",
    "hhpop": "rebased_control_totals_hhpop",
    "hh": "rebased_control_totals_hh",
    "units": "rebased_control_totals_units",
}

# Map indicator key -> spreadsheet sheet name. The legacy Control-Totals
# workbooks ship a ``Pop`` sheet that is all zeros and a ``HHPop`` sheet
# carrying total population, so we read population out of HHPop. Housing
# units are not stored in the legacy workbooks at all and therefore omit
# from this map; the loader returns "Not available" for those.
_SPREADSHEET_SHEETS = {
    "pop": "HHPop",
    "hhpop": "HHPop",
    "hh": "HH",
}

INDICATOR_KEYS = ("pop", "hhpop", "hh", "units")


# ---------------------------------------------------------------------------
# Year-column helpers
# ---------------------------------------------------------------------------

def _year_columns(df: pd.DataFrame) -> list:
    """Return columns of *df* whose label is a 4-digit year (str or int)."""
    out = []
    for col in df.columns:
        s = str(col)
        if s.isdigit() and len(s) == 4:
            out.append(col)
    return out


def _normalize_year_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce 4-digit year column labels to strings, in place-safe copy."""
    rename = {c: str(c) for c in _year_columns(df)}
    return df.rename(columns=rename)


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------

@dataclass
class _PipelineBackend:
    """Backend that reads ``rebased_control_totals_*`` tables from pipeline.h5."""

    path: Path

    @functools.cached_property
    def _pipeline(self) -> Pipeline:
        return Pipeline(settings_path=str(self.path / "configs"))

    @functools.cached_property
    def settings(self) -> dict[str, Any]:
        return self._pipeline.settings or {}

    @functools.cached_property
    def _xwalk(self) -> pd.DataFrame | None:
        cols = ["target_id", "target_name", "control_id", "county_id"]
        try:
            return self._pipeline.get_table("control_target_xwalk")[cols].drop_duplicates()
        except KeyError:
            return None

    def load_indicator(self, indicator: str) -> pd.DataFrame | None:
        table = _PIPELINE_TABLES.get(indicator)
        if table is None:
            return None
        if self._xwalk is None:
            return None
        try:
            df = self._pipeline.get_table(table).copy()
        except KeyError:
            return None
        df = _normalize_year_columns(df)
        df = df.merge(self._xwalk, on="control_id", how="left")
        year_cols = _year_columns(df)
        grouped = (
            df.groupby(["target_id", "target_name", "county_id"], as_index=False)[year_cols]
            .sum()
            .sort_values("target_id")
            .reset_index(drop=True)
        )
        return grouped


@dataclass
class _SpreadsheetBackend:
    """Backend that reads HH/HHPop/Pop sheets from a legacy workbook."""

    workbook_path: Path
    xwalk_path: Path

    @functools.cached_property
    def _xwalk(self) -> pd.DataFrame:
        cols = ["target_id", "target_name", "control_id", "county_id"]
        df = pd.read_csv(self.xwalk_path)[cols].drop_duplicates()
        return df

    def load_indicator(self, indicator: str) -> pd.DataFrame | None:
        sheet = _SPREADSHEET_SHEETS.get(indicator)
        if sheet is None:
            return None
        try:
            df = pd.read_excel(self.workbook_path, sheet_name=sheet)
        except (FileNotFoundError, ValueError):
            return None
        if "control_id" not in df.columns:
            return None
        df = _normalize_year_columns(df)
        year_cols = _year_columns(df)
        if not year_cols:
            return None
        # Merge on xwalk and aggregate to target_id.
        merged = df[["control_id"] + year_cols].merge(
            self._xwalk, on="control_id", how="inner"
        )
        if merged.empty:
            warnings.warn(
                f"Spreadsheet {self.workbook_path.name!r} sheet {sheet!r}: "
                "no control_ids matched the xwalk."
            )
            return None
        grouped = (
            merged.groupby(["target_id", "target_name", "county_id"], as_index=False)[year_cols]
            .sum()
            .sort_values("target_id")
            .reset_index(drop=True)
        )
        return grouped


# ---------------------------------------------------------------------------
# Forecast
# ---------------------------------------------------------------------------

@dataclass
class Forecast:
    """One forecast available to the dashboard."""

    id: str
    name: str
    years: list[str]
    backend: Any  # _PipelineBackend | _SpreadsheetBackend
    type: str = "pipeline"

    def load_indicator(self, indicator: str) -> pd.DataFrame | None:
        return self.backend.load_indicator(indicator)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def _years_from_settings(settings: dict[str, Any]) -> list[str]:
    keys = ["base_year", "targets_end_year", "end_year"]
    out: list[str] = []
    for k in keys:
        v = settings.get(k)
        if v is not None and str(v) not in out:
            out.append(str(v))
    return out


def _build_forecast(spec: dict[str, Any], idx: int) -> Forecast:
    """Construct a :class:`Forecast` from a config-yaml entry."""
    name = spec.get("name") or spec.get("path") or f"forecast_{idx}"
    fid = spec.get("id") or _slugify(name)
    ftype = spec.get("type", "pipeline").lower()
    raw_path = spec.get("path")
    if not raw_path:
        raise ValueError(f"Forecast {name!r} missing 'path'.")
    path = (_VALIDATION_DIR / raw_path).resolve()

    if ftype == "pipeline":
        backend = _PipelineBackend(path=path)
        years = _years_from_settings(backend.settings)
        if not years:
            years = _years_from_settings(spec)
    elif ftype == "spreadsheet":
        wb_rel = spec.get("spreadsheet_file")
        if not wb_rel:
            raise ValueError(
                f"Forecast {name!r}: spreadsheet type requires 'spreadsheet_file'."
            )
        wb_path = (path / wb_rel).resolve()
        xwalk_rel = spec.get("xwalk_path")
        if xwalk_rel:
            xwalk_path = (_VALIDATION_DIR / xwalk_rel).resolve()
        else:
            xwalk_path = (path / "data" / "control_target_xwalk.csv").resolve()
        backend = _SpreadsheetBackend(workbook_path=wb_path, xwalk_path=xwalk_path)
        years = _years_from_settings(spec)
    else:
        raise ValueError(f"Forecast {name!r}: unknown type {ftype!r}.")

    return Forecast(id=fid, name=name, years=years, backend=backend, type=ftype)


def _slugify(s: str) -> str:
    out = []
    for ch in s.lower():
        if ch.isalnum():
            out.append(ch)
        elif ch in (" ", "-", "_", "/", "."):
            out.append("_")
    slug = "".join(out).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug or "forecast"


@functools.lru_cache(maxsize=1)
def load_config() -> dict[str, Any]:
    """Load and validate ``validation/config.yaml``."""
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(f"Validation config not found: {_CONFIG_PATH}")
    with _CONFIG_PATH.open() as f:
        cfg = yaml.safe_load(f) or {}
    return cfg


@functools.lru_cache(maxsize=1)
def load_forecasts() -> dict[str, Forecast]:
    """Build the ordered ``id -> Forecast`` mapping from config.yaml.

    Falls back to a single forecast built from the legacy
    ``example_path`` key if ``forecasts:`` is not defined.
    """
    cfg = load_config()
    specs: list[dict[str, Any]]
    if cfg.get("forecasts"):
        specs = list(cfg["forecasts"])
    elif cfg.get("example_path"):
        specs = [{
            "name": "Forecast",
            "path": cfg["example_path"],
            "type": "pipeline",
        }]
    else:
        raise ValueError(
            "validation/config.yaml must define either 'forecasts:' or "
            "'example_path:' (legacy single-forecast mode)."
        )
    out: dict[str, Forecast] = {}
    for i, spec in enumerate(specs):
        fc = _build_forecast(spec, i)
        if fc.id in out:
            fc.id = f"{fc.id}_{i}"
        out[fc.id] = fc
    return out


def forecast_ids() -> list[str]:
    """Ordered forecast ids matching config.yaml order."""
    return list(load_forecasts())


def forecast_name(forecast_id: str) -> str:
    return load_forecasts()[forecast_id].name


def forecast_years(forecast_id: str) -> list[str]:
    return list(load_forecasts()[forecast_id].years)


# ---------------------------------------------------------------------------
# Crosswalk + indicators (cached per forecast/indicator)
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=64)
def load_indicator(forecast_id: str, indicator: str) -> pd.DataFrame | None:
    """Cached aggregated indicator DataFrame for *forecast_id*.

    Returns ``None`` if the forecast's backend cannot supply the
    indicator (e.g. spreadsheet forecast with no ``units`` sheet).
    Result columns: ``target_id, target_name, county_id``, plus one
    string-labelled year column per available year.
    """
    if indicator not in _PIPELINE_TABLES:
        raise KeyError(f"Unknown indicator {indicator!r}.")
    return load_forecasts()[forecast_id].load_indicator(indicator)


# ---------------------------------------------------------------------------
# Placeholder DataFrame for "not available"
# ---------------------------------------------------------------------------

_NOT_AVAILABLE_LABEL = "Not available for this forecast"


def _placeholder(years: list[str]) -> pd.DataFrame:
    cols = years if years else ["—"]
    df = pd.DataFrame([[_NOT_AVAILABLE_LABEL] + [""] * (len(cols) - 1)], columns=cols)
    df.index = [""]
    return df


# ---------------------------------------------------------------------------
# Table builders
# ---------------------------------------------------------------------------

_AVG_ANNUAL_CHANGE_COL = "Avg. annual change"


def _restrict_years(df: pd.DataFrame, years: list[str]) -> list[str]:
    """Return year labels from *years* that are present in *df*."""
    return [y for y in years if y in df.columns]


def _append_avg_annual_change(
    df: pd.DataFrame, years: list[str], col: str = _AVG_ANNUAL_CHANGE_COL
) -> pd.DataFrame:
    """Append an average-annual-change column based on the first and last years.

    The value is ``(df[end_year] - df[base_year]) / (end_year - base_year)``.
    Returns *df* unchanged if fewer than two years are available or if the
    span is zero.
    """
    if len(years) < 2:
        return df
    base, end = years[0], years[-1]
    try:
        span = int(end) - int(base)
    except (TypeError, ValueError):
        return df
    if span <= 0:
        return df
    df = df.copy()
    df[col] = (df[end] - df[base]) / span
    return df


def _county_slice(df: pd.DataFrame, county_id: int, years: list[str]) -> pd.DataFrame:
    sub = df.loc[df["county_id"] == county_id].copy()
    sub = sub.sort_values("target_id").set_index("target_name")
    return sub[years]


def target_area_table(
    forecast_id: str,
    indicator: str,
    county_id: int,
    total_label: str = "Total",
) -> pd.DataFrame:
    """Wide target-area-by-year counts for one forecast/indicator/county."""
    fc = load_forecasts()[forecast_id]
    df = load_indicator(forecast_id, indicator)
    if df is None:
        return _placeholder(fc.years)
    years = _restrict_years(df, fc.years)
    if not years:
        return _placeholder(fc.years)
    sub = _county_slice(df, county_id, years)
    sub.loc[total_label] = sub.sum(axis=0)
    return _append_avg_annual_change(sub, years)


def ratio_table(
    forecast_id: str,
    numerator: str,
    denominator: str,
    county_id: int,
    formula: str = "ratio",
    total_label: str = "Total",
) -> pd.DataFrame:
    """Wide target-area ratio table (e.g. HH size, vacancy)."""
    fc = load_forecasts()[forecast_id]
    num_df = load_indicator(forecast_id, numerator)
    den_df = load_indicator(forecast_id, denominator)
    if num_df is None or den_df is None:
        return _placeholder(fc.years)
    years = _restrict_years(num_df, fc.years)
    years = [y for y in years if y in den_df.columns]
    if not years:
        return _placeholder(fc.years)
    num = _county_slice(num_df, county_id, years)
    den = _county_slice(den_df, county_id, years)
    if formula == "ratio":
        ratio = num / den.replace(0, pd.NA)
        total = num.sum(axis=0) / den.sum(axis=0).replace(0, pd.NA)
    elif formula == "vacancy":
        ratio = 1 - num / den.replace(0, pd.NA)
        total = 1 - num.sum(axis=0) / den.sum(axis=0).replace(0, pd.NA)
    else:
        raise ValueError(f"Unknown formula {formula!r}; use 'ratio' or 'vacancy'.")
    ratio.loc[total_label] = total
    return _append_avg_annual_change(ratio, years)


def _county_year_matrix(df: pd.DataFrame, years: list[str]) -> pd.DataFrame:
    grouped = df.groupby("county_id", as_index=True)[years].sum()
    name_map = dict(COUNTIES)
    grouped = grouped.reindex([cid for cid, _ in COUNTIES])
    grouped.index = [name_map[cid] for cid in grouped.index]
    grouped.index.name = "County"
    return grouped


def region_table(
    forecast_id: str,
    indicator: str,
    total_label: str = "Region",
) -> pd.DataFrame:
    """County-by-year totals for one forecast, with a region row."""
    fc = load_forecasts()[forecast_id]
    df = load_indicator(forecast_id, indicator)
    if df is None:
        return _placeholder(fc.years)
    years = _restrict_years(df, fc.years)
    if not years:
        return _placeholder(fc.years)
    counties = _county_year_matrix(df, years)
    counties.loc[total_label] = counties.sum(axis=0)
    return _append_avg_annual_change(counties, years)


def region_ratio_table(
    forecast_id: str,
    numerator: str,
    denominator: str,
    formula: str = "ratio",
    total_label: str = "Region",
) -> pd.DataFrame:
    fc = load_forecasts()[forecast_id]
    num_df = load_indicator(forecast_id, numerator)
    den_df = load_indicator(forecast_id, denominator)
    if num_df is None or den_df is None:
        return _placeholder(fc.years)
    years = _restrict_years(num_df, fc.years)
    years = [y for y in years if y in den_df.columns]
    if not years:
        return _placeholder(fc.years)
    num = _county_year_matrix(num_df, years)
    den = _county_year_matrix(den_df, years)
    if formula == "ratio":
        ratio = num / den.replace(0, pd.NA)
        total = num.sum(axis=0) / den.sum(axis=0).replace(0, pd.NA)
    elif formula == "vacancy":
        ratio = 1 - num / den.replace(0, pd.NA)
        total = 1 - num.sum(axis=0) / den.sum(axis=0).replace(0, pd.NA)
    else:
        raise ValueError(f"Unknown formula {formula!r}; use 'ratio' or 'vacancy'.")
    ratio.loc[total_label] = total
    return _append_avg_annual_change(ratio, years)


# ---------------------------------------------------------------------------
# Display formatting
# ---------------------------------------------------------------------------

def _is_placeholder(df: pd.DataFrame) -> bool:
    return (
        df.shape[0] == 1
        and df.iloc[0, 0] == _NOT_AVAILABLE_LABEL
    )


def _fmt_fixed(decimals: int):
    """Return a formatter that avoids '-0.00' for values rounding to zero."""
    def _f(v):
        if pd.isna(v):
            return "-"
        rounded = round(float(v), decimals)
        if rounded == 0:
            rounded = 0.0
        return f"{rounded:,.{decimals}f}"
    return _f


def format_counts(df: pd.DataFrame):
    """Render counts with thousands separators and no decimals."""
    if _is_placeholder(df):
        return df.style
    return df.style.format(_fmt_fixed(0), na_rep="-")


def format_ratio(df: pd.DataFrame, decimals: int = 2):
    """Render ratios with fixed decimal places."""
    if _is_placeholder(df):
        return df.style
    return df.style.format(_fmt_fixed(decimals), na_rep="-")


# ---------------------------------------------------------------------------
# Tabset rendering (Quarto panel-tabset)
# ---------------------------------------------------------------------------

def display_tabset(builder, ids: list[str] | None = None) -> None:
    """Render a Quarto panel-tabset, one tab per forecast.

    Args:
        builder: Callable taking a forecast id and returning a value
            suitable for ``IPython.display.display`` (typically a
            pandas Styler or DataFrame).
        ids: Optional list of forecast ids to display; defaults to all
            configured forecasts in config.yaml order.
    """
    from IPython.display import Markdown, display

    ids = ids or forecast_ids()
    parts: list[str] = ["::: {.panel-tabset}", ""]
    for fid in ids:
        parts.append(f"### {forecast_name(fid)}")
        parts.append("")
        try:
            obj = builder(fid)
        except Exception as err:  # pragma: no cover - defensive
            parts.append(f"*Error rendering forecast {fid!r}: {err}*")
            parts.append("")
            continue
        # Render Styler / DataFrame to HTML so the whole tabset can be
        # emitted as a single Markdown block (Quarto requires the tab
        # headings to live inside the same markdown output as the
        # ``:::`` fence markers).
        if hasattr(obj, "to_html"):
            html = obj.to_html()
        else:
            html = str(obj)
        parts.append(html)
        parts.append("")
    parts.append(":::")
    display(Markdown("\n".join(parts)))


def show_indicator_county(indicator: str, county_id: int) -> None:
    """Display a per-county panel-tabset of count tables for *indicator*."""
    display_tabset(lambda fid: format_counts(target_area_table(fid, indicator, county_id)))


def show_ratio_county(
    numerator: str,
    denominator: str,
    county_id: int,
    formula: str = "ratio",
    decimals: int = 2,
) -> None:
    """Display a per-county panel-tabset of ratio tables."""
    display_tabset(
        lambda fid: format_ratio(
            ratio_table(fid, numerator, denominator, county_id, formula=formula),
            decimals=decimals,
        )
    )


def show_region_indicator(indicator: str) -> None:
    """Display a region-level panel-tabset of count tables."""
    display_tabset(lambda fid: format_counts(region_table(fid, indicator)))


def show_region_ratio(
    numerator: str,
    denominator: str,
    formula: str = "ratio",
    decimals: int = 2,
) -> None:
    """Display a region-level panel-tabset of ratio tables."""
    display_tabset(
        lambda fid: format_ratio(
            region_ratio_table(fid, numerator, denominator, formula=formula),
            decimals=decimals,
        )
    )


__all__ = [
    "COUNTIES",
    "Forecast",
    "load_config",
    "load_forecasts",
    "forecast_ids",
    "forecast_name",
    "forecast_years",
    "load_indicator",
    "target_area_table",
    "ratio_table",
    "region_table",
    "region_ratio_table",
    "format_counts",
    "format_ratio",
    "display_tabset",
    "show_indicator_county",
    "show_ratio_county",
    "show_region_indicator",
    "show_region_ratio",
]
