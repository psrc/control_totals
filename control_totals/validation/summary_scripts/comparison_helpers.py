"""Pairwise forecast comparison helpers for the validation dashboard.

Used by the ``households_compare_*`` notebooks to compute and render
the % difference in households between two forecasts at a single
forecast year, broken out by control/target area and grouped by county.

Public API:
    compare_hh_target_area(forecast_a_id, forecast_b_id, year, county_id)
    compare_hh_region(forecast_a_id, forecast_b_id, year)

Both return a pandas Styler with cell color-coding on the % Diff
column. When either forecast lacks the ``hh`` indicator or the
requested year, a placeholder Styler is returned instead of raising.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from validation_data_input import (
    COUNTIES,
    forecast_name,
    load_indicator,
)

# Thresholds (percent) for cell coloring on the "% Diff" column.
_PCT_RED = 5.0
_PCT_AMBER = 2.5

_DIFF_COL = "Diff"
_PCT_COL = "% Diff"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _pct_diff_series(a: pd.Series, b: pd.Series) -> pd.Series:
    """Vectorised (a - b) / b * 100; NaN when b is 0/NaN."""
    b_safe = b.replace(0, np.nan)
    return (a - b_safe) / b_safe * 100.0


def _placeholder_styler(message: str) -> "pd.io.formats.style.Styler":
    df = pd.DataFrame({"": [message]})
    return df.style.hide(axis="index")


def _color_pct(val: float) -> str:
    if pd.isna(val):
        return ""
    mag = abs(val)
    if mag >= _PCT_RED:
        return "color: #b00020; font-weight: 600;"
    if mag >= _PCT_AMBER:
        return "color: #d97706;"
    return "color: #2f7a2f;"


def _fmt_int(v) -> str:
    if pd.isna(v):
        return "—"
    return f"{int(round(float(v))):,}"


def _fmt_signed_int(v) -> str:
    if pd.isna(v):
        return "—"
    return f"{int(round(float(v))):+,}"


def _fmt_signed_pct(v) -> str:
    if pd.isna(v):
        return "—"
    return f"{v:+.1f}%"


def _style_compare(
    df: pd.DataFrame,
    a_label: str,
    b_label: str,
) -> "pd.io.formats.style.Styler":
    fmt = {
        a_label: _fmt_int,
        b_label: _fmt_int,
        _DIFF_COL: _fmt_signed_int,
        _PCT_COL: _fmt_signed_pct,
    }
    styler = df.style.format(fmt, na_rep="—")
    styler = styler.map(_color_pct, subset=[_PCT_COL])
    styler = styler.set_table_styles([
        {"selector": "th",
         "props": [("background-color", "rgba(0,85,140,0.08)"),
                   ("font-weight", "600"),
                   ("text-align", "left")]},
        {"selector": "td", "props": [("padding", "4px 10px")]},
    ])
    return styler


def _load_hh_year(forecast_id: str, year: str) -> pd.DataFrame | None:
    """Return ``hh`` indicator restricted to a single year column, or None.

    Output columns: ``target_id, target_name, county_id, hh`` where the
    ``hh`` column carries the value at *year*.
    """
    df = load_indicator(forecast_id, "hh")
    if df is None:
        return None
    if year not in df.columns:
        return None
    out = df[["target_id", "target_name", "county_id", year]].copy()
    out = out.rename(columns={year: "hh"})
    return out


# ---------------------------------------------------------------------------
# Per-county target-area comparison
# ---------------------------------------------------------------------------

def compare_hh_target_area(
    forecast_a_id: str,
    forecast_b_id: str,
    year: str,
    county_id: int,
    total_label: str = "Total",
) -> "pd.io.formats.style.Styler":
    """Per-target-area HH comparison table for one county at *year*.

    Rows are target areas (with a ``Total`` row); columns are
    ``<forecast A name>``, ``<forecast B name>``, ``Diff`` (A − B),
    ``% Diff`` ((A − B) / B × 100). Target areas present in only one
    forecast are kept (outer merge); missing values render as ``—``.
    """
    a = _load_hh_year(forecast_a_id, year)
    b = _load_hh_year(forecast_b_id, year)
    if a is None or b is None:
        return _placeholder_styler(
            f"Comparison not available — forecast missing 'hh' at year {year}."
        )

    a_label = forecast_name(forecast_a_id)
    b_label = forecast_name(forecast_b_id)

    a_co = a.loc[a["county_id"] == county_id, ["target_id", "target_name", "hh"]]
    b_co = b.loc[b["county_id"] == county_id, ["target_id", "target_name", "hh"]]

    merged = a_co.merge(
        b_co,
        on=["target_id", "target_name"],
        how="outer",
        suffixes=("_a", "_b"),
    )
    merged = merged.sort_values("target_id").reset_index(drop=True)
    merged = merged.set_index("target_name")
    merged.index.name = None

    out = pd.DataFrame({
        a_label: merged["hh_a"],
        b_label: merged["hh_b"],
    })
    # Total row: sum (NaN-aware so partial coverage is still summed).
    total_a = out[a_label].sum(skipna=True)
    total_b = out[b_label].sum(skipna=True)
    out.loc[total_label] = [total_a, total_b]

    out[_DIFF_COL] = out[a_label] - out[b_label]
    out[_PCT_COL] = _pct_diff_series(out[a_label], out[b_label])

    return _style_compare(out, a_label, b_label)


# ---------------------------------------------------------------------------
# Regional comparison (county rows + Region total)
# ---------------------------------------------------------------------------

def compare_hh_region(
    forecast_a_id: str,
    forecast_b_id: str,
    year: str,
    total_label: str = "Region",
) -> "pd.io.formats.style.Styler":
    """County-level HH comparison table at *year* with a Region row."""
    a = _load_hh_year(forecast_a_id, year)
    b = _load_hh_year(forecast_b_id, year)
    if a is None or b is None:
        return _placeholder_styler(
            f"Comparison not available — forecast missing 'hh' at year {year}."
        )

    a_label = forecast_name(forecast_a_id)
    b_label = forecast_name(forecast_b_id)

    a_by_co = a.groupby("county_id")["hh"].sum()
    b_by_co = b.groupby("county_id")["hh"].sum()

    rows: list[tuple[str, float, float]] = []
    for cid, cname in COUNTIES:
        rows.append((cname, a_by_co.get(cid, np.nan), b_by_co.get(cid, np.nan)))

    out = pd.DataFrame(rows, columns=["County", a_label, b_label]).set_index("County")
    out.index.name = None

    out.loc[total_label] = [
        out[a_label].sum(skipna=True),
        out[b_label].sum(skipna=True),
    ]
    out[_DIFF_COL] = out[a_label] - out[b_label]
    out[_PCT_COL] = _pct_diff_series(out[a_label], out[b_label])

    return _style_compare(out, a_label, b_label)


__all__ = [
    "compare_hh_target_area",
    "compare_hh_region",
]
