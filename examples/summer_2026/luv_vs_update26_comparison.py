"""Build a workbook comparing the LUV.3 and update26 control totals (HHwork / EMPwork).

Tabs:
  luv_HHwork, luv_EMPwork, update26_HHwork, update26_EMPwork  unchanged source tables
  HH_by_subreg, EMP_by_subreg      outer join on subreg_id (= control_hct_id)
  HH_by_control, EMP_by_control    outer join on control_id (= geo_id = nosplit_geo_id)
  HH_by_county, EMP_by_county      county and region totals (base year, 2044, 2050, differences)
  geo_changes                      geographies added in / dropped from update26

Run from examples/summer_2026:  python luv_vs_update26_comparison.py
"""
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent

LUV_PATH = "J:/Projects/LandUseVision/LUV.3/Control_totals/SubJuris_ConTots/LUVit_ct_by_tod_generator_90-90-90_2024-10-18_6575_LUVit_Match.xlsx"
UPDATE26_PATH = HERE / 'output/LUVit_ct_by_tod_generator-2026-11-18_90-90-90.xlsx'
LUV_XWALK_PATH = "J:/Projects/LandUseVision/LUV.3/Final_Products/LUV-it Dataset.xlsx"
UPDATE26_XWALK_PATH = HERE / 'data/control_target_xwalk.csv'
OUT_FILE = HERE / 'output/ct_luv_vs_update26_comparison.xlsx'

COUNTY_NAMES = {53033: 'King', 53035: 'Kitsap', 53053: 'Pierce', 53061: 'Snohomish'}
LUV_BASE, UPDATE_BASE = 2020, 2023
COMPARE_YEARS = [2044, 2050]
BASE_YEARS = {'luv': LUV_BASE, 'update26': UPDATE_BASE}
PATHS = {'luv': LUV_PATH, 'update26': UPDATE26_PATH}
# label: (work sheet, sheet with one column per year)
MEASURES = {'HH': ('HHwork', 'HH'), 'EMP': ('EMPwork', 'Emp')}
GEO_COLS = ['county', 'control_id', 'target_id', 'control_name']
STATUS = {'both': 'in both', 'left_only': 'dropped in update26', 'right_only': 'added in update26'}


def load_control_lookup():
    """Control-level county/target_id/control_name; the update26 xwalk is preferred and the
    luv xwalk fills controls that no longer exist."""
    cols = ['control_id', 'target_id', 'control_name', 'county']
    target_xwalk_luv = pd.read_excel(LUV_XWALK_PATH, sheet_name='Control + HCT Geographies')[
        ['control_hct_id', 'control_id', 'target_id', 'control_hct_name', 'County']
    ]
    luv = (target_xwalk_luv.rename(columns={'control_hct_name': 'control_name', 'County': 'county'})
           .drop_duplicates('control_id')[cols])
    target_xwalk26 = pd.read_csv(UPDATE26_XWALK_PATH)
    upd = target_xwalk26.assign(county=target_xwalk26['county_id'].map(COUNTY_NAMES))[cols]
    return (upd.set_index('control_id').combine_first(luv.set_index('control_id'))
            .reset_index().astype({'target_id': 'Int64'}))


def run_data(run, label, raw_tables):
    """subreg_id -> control_id/is_tod ids and the base, 2044 and 2050 values of one run and measure."""
    _, year_sheet = MEASURES[label]
    work, base = raw_tables[(run, label)], BASE_YEARS[run]
    geo_col = 'geo_id' if 'geo_id' in work.columns else 'nosplit_geo_id'
    ids = work[['subreg_id', geo_col, 'is_tod']].rename(columns={geo_col: 'control_id'})
    vals = pd.read_excel(PATHS[run], sheet_name=year_sheet)
    vals.columns = ['subreg_id'] + [int(c) for c in vals.columns[1:]]
    years = [base, *COMPARE_YEARS]
    vals = vals[['subreg_id', *years]].rename(columns={y: f'{run}_{y}' for y in years})
    return ids, vals


def add_metrics(df):
    for y in COMPARE_YEARS:
        df[f'diff_{y}'] = df[f'update26_{y}'] - df[f'luv_{y}']
        df[f'pct_diff_{y}'] = (df[f'diff_{y}'] / df[f'luv_{y}'].replace(0, np.nan) * 100).round(1)
    return df


def value_cols():
    return [f'{run}_{y}' for run, base in BASE_YEARS.items() for y in [base, *COMPARE_YEARS]]


def order_columns(df, lead):
    cols = [f'luv_{LUV_BASE}', f'update26_{UPDATE_BASE}']
    for y in COMPARE_YEARS:
        cols += [f'luv_{y}', f'update26_{y}', f'diff_{y}', f'pct_diff_{y}']
    return df[lead + cols].reset_index(drop=True)


def per_control(ids, vals):
    """Sum a run's subreg values to the control level."""
    return (vals.merge(ids[['subreg_id', 'control_id']], on='subreg_id').drop(columns='subreg_id')
            .groupby('control_id', as_index=False).sum(min_count=1))


def build_comparisons(label, raw_tables, control_lookup):
    luv_ids, luv_vals = run_data('luv', label, raw_tables)
    upd_ids, upd_vals = run_data('update26', label, raw_tables)
    ids = pd.concat([upd_ids, luv_ids]).drop_duplicates('subreg_id')  # update26 wins where both have the subreg

    sub = luv_vals.merge(upd_vals, on='subreg_id', how='outer', indicator='status')
    sub['status'] = sub['status'].map(STATUS)
    sub = sub.merge(ids, on='subreg_id', how='left').merge(control_lookup, on='control_id', how='left')
    sub = add_metrics(sub).sort_values(['county', 'target_id', 'control_id', 'subreg_id'])
    by_subreg = order_columns(sub, ['subreg_id', *GEO_COLS, 'is_tod', 'status'])

    ctl = per_control(luv_ids, luv_vals).merge(per_control(upd_ids, upd_vals), on='control_id', how='outer',
                                               indicator='status')
    ctl['status'] = ctl['status'].map(STATUS)
    ctl = ctl.merge(control_lookup, on='control_id', how='left')
    ctl_totals = ctl.copy()
    ctl = add_metrics(ctl).sort_values(['county', 'target_id', 'control_id'])
    by_control = order_columns(ctl, [*GEO_COLS, 'status'])

    cty = ctl_totals.groupby('county', as_index=False)[value_cols()].sum(min_count=1)
    reg = pd.DataFrame([{'county': 'Region', **ctl_totals[value_cols()].sum(min_count=1).to_dict()}])
    by_county = order_columns(add_metrics(pd.concat([cty, reg], ignore_index=True)), ['county'])
    return by_subreg, by_control, by_county


def build_geo_changes(comparisons):
    """Added / dropped geographies with the 2050 value from whichever run has the geography."""
    keys = ['level', 'id', *GEO_COLS, 'status']
    by_measure = []
    for label, (by_subreg, by_control, _) in comparisons.items():
        frames = []
        for level, df in [('subreg_id', by_subreg), ('control_id', by_control)]:
            ch = df[df['status'] != 'in both'].copy()
            ch['level'] = level
            ch['id'] = ch[level]
            ch[f'{label}_2050'] = ch['update26_2050'].fillna(ch['luv_2050'])
            frames.append(ch[[*keys, f'{label}_2050']])
        by_measure.append(pd.concat(frames))
    out = by_measure[0]
    for df in by_measure[1:]:
        out = out.merge(df, on=keys, how='outer')
    return out.sort_values(['level', 'status', 'county', 'id'], ascending=[False, True, True, True]).reset_index(drop=True)


def pretty_columns(df):
    """Readable headers for the comparison tabs; source tabs keep their original column names."""
    fixed = {'subreg_id': 'Subreg ID', 'control_id': 'Control ID', 'target_id': 'Target ID',
             'control_name': 'Control Name', 'is_tod': 'Is TOD', 'county': 'County', 'status': 'Status',
             'level': 'Level', 'id': 'ID'}
    prefixes = {'luv_': 'LUV ', 'update26_': 'Update26 ', 'diff_': 'Diff ', 'pct_diff_': '% Diff ',
                'HH_': 'HH ', 'EMP_': 'EMP '}

    def rename(col):
        if col in fixed:
            return fixed[col]
        for prefix, label in prefixes.items():
            if col.startswith(prefix):
                return label + col[len(prefix):]
        return col

    return df.rename(columns=rename)


def format_sheet(ws):
    """Header styling, filter, frozen header row and readable column widths."""
    from openpyxl.styles import Alignment, Font
    from openpyxl.utils import get_column_letter

    ws.freeze_panes = 'A2'
    ws.auto_filter.ref = ws.dimensions
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(wrap_text=True, vertical='top')
    for i, col in enumerate(ws.iter_cols(min_row=1, max_row=min(ws.max_row, 200)), start=1):
        longest = max(len(str(c.value)) if c.value is not None else 0 for c in col[1:] or col)
        ws.column_dimensions[get_column_letter(i)].width = min(max(longest + 2, 10), 32)
    ws.row_dimensions[1].height = 45


def main():
    # unchanged work tables (RGid/RGID, geo_id/nosplit_geo_id are left as they are in each source file)
    raw_tables = {
        (run, label): pd.read_excel(PATHS[run], sheet_name=work_sheet)
        for run in PATHS for label, (work_sheet, _) in MEASURES.items()
    }
    control_lookup = load_control_lookup()
    comparisons = {label: build_comparisons(label, raw_tables, control_lookup) for label in MEASURES}
    geo_changes = build_geo_changes(comparisons)

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        for (run, label), df in raw_tables.items():
            df.to_excel(writer, sheet_name=f'{run}_{MEASURES[label][0]}', index=False)
        for suffix, i in [('by_subreg', 0), ('by_control', 1), ('by_county', 2)]:
            for label, frames in comparisons.items():
                pretty_columns(frames[i]).to_excel(writer, sheet_name=f'{label}_{suffix}', index=False)
        pretty_columns(geo_changes).to_excel(writer, sheet_name='geo_changes', index=False)
        for ws in writer.book.worksheets:
            format_sheet(ws)
    print(f'wrote {OUT_FILE}')
    return comparisons, geo_changes


if __name__ == '__main__':
    main()
