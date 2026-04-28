"""Shared helpers for validation dashboard notebooks.

Keep this module dependency-light (pandas, plotly, numpy) so every chapter
notebook can import it without pulling in the full pipeline stack.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio


# ---------------------------------------------------------------------------
# County / region labels
# ---------------------------------------------------------------------------

# State+county FIPS used in settings.yaml -> human-readable name.
COUNTY_FIPS_TO_NAME: dict[int, str] = {
    53033: "King",
    53035: "Kitsap",
    53053: "Pierce",
    53061: "Snohomish",
}

# Short county_id used inside output workbooks (CityPop / RGs sheets).
COUNTY_ID_TO_NAME: dict[int, str] = {
    33: "King",
    35: "Kitsap",
    53: "Pierce",
    61: "Snohomish",
}

# Display order across the dashboard.
COUNTY_ORDER: list[str] = ["King", "Kitsap", "Pierce", "Snohomish"]

# Plotly colors — keep stable across charts.
COUNTY_COLORS: dict[str, str] = {
    "King":      "#00558C",
    "Kitsap":    "#7AB800",
    "Pierce":    "#E37222",
    "Snohomish": "#5C2D91",
    "Region":    "#444444",
}

INDICATORS: dict[str, str] = {
    "total_pop":   "Population",
    "total_hh":    "Households",
    "total_hhpop": "Household population",
    "total_emp":   "Employment",
}


# ---------------------------------------------------------------------------
# Plotly theme
# ---------------------------------------------------------------------------

def _register_psrc_template() -> None:
    """Register a 'psrc' plotly template once."""
    if "psrc" in pio.templates:
        return
    tmpl = go.layout.Template()
    tmpl.layout = go.Layout(
        font=dict(family="Poppins, Segoe UI, sans-serif", size=12, color="#222"),
        title=dict(font=dict(size=16, color="#00558C")),
        plot_bgcolor="white",
        paper_bgcolor="white",
        colorway=[
            "#00558C", "#7AB800", "#E37222", "#5C2D91",
            "#0099A8", "#D62728", "#8C564B", "#E377C2",
        ],
        xaxis=dict(showgrid=False, linecolor="#cccccc", ticks="outside"),
        yaxis=dict(gridcolor="#eeeeee", linecolor="#cccccc", zerolinecolor="#cccccc"),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        margin=dict(l=60, r=20, t=60, b=50),
    )
    pio.templates["psrc"] = tmpl


def apply_plotly_theme(fig: go.Figure) -> go.Figure:
    """Apply the shared PSRC plotly template to a figure."""
    _register_psrc_template()
    fig.update_layout(template="psrc")
    return fig


# Register on import so notebooks can also do
#   pio.templates.default = "psrc"
_register_psrc_template()
pio.templates.default = "psrc"

# Use a renderer that produces inline HTML/JS so Quarto's HTML output can
# display the figures. The default 'plotly_mimetype' emits JSON only, which
# Quarto/nbconvert renders as "Unable to display output for mime type(s):
# application/vnd.plotly.v1+json".
pio.renderers.default = "plotly_mimetype+notebook_connected"


# ---------------------------------------------------------------------------
# Pandas / formatting helpers
# ---------------------------------------------------------------------------

def format_int(n: float | int | None) -> str:
    """Format a number with thousands separators; '—' for NaN/None."""
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "—"
    return f"{int(round(n)):,}"


def pct_diff(a: float, b: float) -> float:
    """Percent difference (a - b) / b * 100. Returns NaN when b is 0/NaN."""
    if b is None or b == 0 or (isinstance(b, float) and np.isnan(b)):
        return float("nan")
    return (a - b) / b * 100.0


def style_diff_table(
    df: pd.DataFrame,
    int_cols: list[str] | None = None,
    pct_cols: list[str] | None = None,
    pct_threshold: float = 5.0,
) -> "pd.io.formats.style.Styler":
    """Pretty-print a comparison table.

    int_cols   : columns formatted as integers with thousands separators.
    pct_cols   : columns formatted as percent (red/green > pct_threshold).
    """
    int_cols = int_cols or []
    pct_cols = pct_cols or []
    fmt = {c: format_int for c in int_cols}
    fmt.update({c: lambda v: "—" if pd.isna(v) else f"{v:+.1f}%" for c in pct_cols})

    styler = df.style.format(fmt, na_rep="—")

    def _color(val):
        if pd.isna(val):
            return ""
        if abs(val) >= pct_threshold:
            return "color: #b00020; font-weight: 600;"
        if abs(val) >= pct_threshold / 2:
            return "color: #d97706;"
        return "color: #2f7a2f;"

    for c in pct_cols:
        styler = styler.map(_color, subset=[c])

    styler = styler.set_table_styles([
        {"selector": "th",
         "props": [("background-color", "rgba(0,85,140,0.08)"),
                   ("font-weight", "600"),
                   ("text-align", "left")]},
        {"selector": "td", "props": [("padding", "4px 10px")]},
    ])
    return styler


def passfail_badge(ok: bool) -> str:
    """Return an HTML badge — used by the consistency-checks notebook."""
    if ok:
        return "<span style='color:#2f7a2f;font-weight:600;'>✓ pass</span>"
    return "<span style='color:#b00020;font-weight:600;'>✗ fail</span>"


__all__ = [
    "COUNTY_FIPS_TO_NAME",
    "COUNTY_ID_TO_NAME",
    "COUNTY_ORDER",
    "COUNTY_COLORS",
    "INDICATORS",
    "apply_plotly_theme",
    "format_int",
    "pct_diff",
    "style_diff_table",
    "passfail_badge",
]
