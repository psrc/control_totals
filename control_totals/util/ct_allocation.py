"""Shared control-totals allocation and rebalancing algorithms.

Generalized implementations of the household-by-PPH allocation used by both the
regional (:mod:`steps.regionalCTs`) and subregional (:mod:`steps.subregionalCTs`)
control-totals steps. The two steps differ only in:

* the grouping geography -- regional operates on a single global group while
  subregional operates per ``subreg_id`` (passed via ``group_keys``); and
* the names of the aggregate-control columns (``hh_pop``/``household_count`` for
  the regional forecast vs ``total_hhpop``/``total_hh`` for the subregional
  controls), passed via ``hh_col`` / ``hhpop_col``.

A ``group_keys`` of ``[]`` selects the regional (single-group) behaviour; when
empty, a temporary constant key is used internally so a single code path serves
both cases.

Note: the rebalancing step uses weighted random sampling. Results are
reproducible run-to-run for a fixed RNG seed but are not bit-for-bit identical
to the original R scripts.
"""
import numpy as np


# ---------------------------------------------------------------------------
# Shared bin definitions
# ---------------------------------------------------------------------------
PPH_BINS = np.arange(1, 8, dtype=int)        # persons-per-household bins 1..7
WORKER_BINS = np.arange(0, 5, dtype=int)     # worker bins 0..4

# Default income brackets (lower bound of each bracket). Overridable via the
# ``regional_cts.income_bins`` / ``regional_cts.income_labels`` settings.
INCOME_BINS = [0, 56000, 106000, 180000]
INCOME_LABELS = [
    "Under $56,000",
    "$56,000-$105,999",
    "$106,000-$179,999",
    "$180,000 or more",
]


def _with_dummy_key(group_keys):
    """Return ``(effective_keys, use_dummy)`` for a possibly-empty grouping.

    When ``group_keys`` is empty, a single temporary constant key (``_grp``) is
    used so the grouped code path also serves the regional (single-group) case.
    """
    if group_keys:
        return list(group_keys), False
    return ['_grp'], True


# ---------------------------------------------------------------------------
# Larry Blain's two-ratio formula
# ---------------------------------------------------------------------------
def hhpop_control(ctpop, controls, op_year, ref_year, group_keys, hh_col, hhpop_col):
    """Apply Larry Blain's two-ratio formula to distribute ``op_year`` HHs.

    Splits PPH bins into ``small`` (pph<3) and ``large`` (pph>=3) and solves,
    per group, for ratios ``r1, r2`` so the resulting HH count and HH population
    match the aggregate controls at ``op_year``.

    Args:
        ctpop (pandas.DataFrame): The CT-by-PPH frame (``year``, ``pph``,
            ``household_count`` plus any ``group_keys``).
        controls (pandas.DataFrame): Aggregate controls containing ``year``,
            ``hh_col``, ``hhpop_col`` plus any ``group_keys``.
        op_year (int): Year being updated.
        ref_year (int): Previous year used as the reference distribution.
        group_keys (list[str]): Grouping columns (``[]`` for global/regional).
        hh_col (str): Name of the aggregate household-count control column.
        hhpop_col (str): Name of the aggregate household-population control column.

    Returns:
        pandas.DataFrame: ``group_keys + ['pph', 'household_count']`` updates
            for ``op_year``.
    """
    gk, use_dummy = _with_dummy_key(group_keys)

    ref = ctpop[ctpop['year'] == ref_year].copy()
    ctrl = controls.copy()
    if use_dummy:
        ref['_grp'] = 0
        ctrl['_grp'] = 0

    ref['hhsize'] = np.where(ref['pph'] < 3, 'small', 'large')
    ref['hhpop_row'] = ref['household_count'] * ref['pph']
    tmp = ref.groupby(gk + ['hhsize'], as_index=False).agg(
        hh=('household_count', 'sum'),
        pop=('hhpop_row', 'sum'),
    )
    wide = tmp.pivot(index=gk, columns='hhsize', values=['hh', 'pop']).reset_index()
    wide.columns = [a if b == '' else f'{a}_{b}' for a, b in wide.columns]

    op_ctrl = ctrl[ctrl['year'] == op_year][gk + [hh_col, hhpop_col]]
    wide = wide.merge(op_ctrl, on=gk, how='left')

    denom = wide['pop_small'] - wide['hh_small'] * wide['pop_large'] / wide['hh_large']
    wide['r1'] = (wide[hhpop_col] - wide[hh_col] * wide['pop_large'] / wide['hh_large']) / denom
    wide['r2'] = (wide[hh_col] - wide['r1'] * wide['hh_small']) / wide['hh_large']

    out = ctpop[ctpop['year'] == op_year].copy()
    if use_dummy:
        out['_grp'] = 0
    out = out[gk + ['pph']]
    out = out.merge(wide[gk + ['r1', 'r2']], on=gk, how='left')
    prev = ref[gk + ['pph', 'household_count']].rename(columns={'household_count': 'prev_hh'})
    out = out.merge(prev, on=gk + ['pph'], how='left')

    is_small = out['pph'] < 3
    out['household_count'] = np.where(
        is_small, out['prev_hh'] * out['r1'], out['prev_hh'] * out['r2']
    )
    out['household_count'] = np.maximum(1, np.round(out['household_count']))
    # Fall back to prev when ratios are undefined (e.g. missing control).
    out['household_count'] = out['household_count'].where(
        out['household_count'].notna() & np.isfinite(out['household_count']),
        out['prev_hh'],
    ).astype(float)
    return out[list(group_keys) + ['pph', 'household_count']]


def iterate_hhpop_control(ctpop, controls, group_keys, hh_col, hhpop_col):
    """Apply Larry Blain's formula iteratively across sorted years."""
    merge_keys = list(group_keys) + ['pph']
    years = sorted(ctpop['year'].unique())
    for i in range(1, len(years)):
        upd = hhpop_control(ctpop, controls, years[i], years[i - 1], group_keys, hh_col, hhpop_col)
        merge = ctpop.merge(
            upd.rename(columns={'household_count': 'new_hh'}),
            on=merge_keys, how='left',
        )
        mask = ctpop['year'] == years[i]
        ctpop.loc[mask, 'household_count'] = merge.loc[mask, 'new_hh'].to_numpy()
    return ctpop


# ---------------------------------------------------------------------------
# Rebalancing
# ---------------------------------------------------------------------------
def rebalance_pop(ctpop, controls, group_keys, hhpop_col):
    """Subtract excess HH-population from the pph==7 bin (R ``rebalance.pop``).

    For each (group, year) where the aggregated ``hhpop`` exceeds the control's
    ``hhpop_col``, subtract ``floor(|diff| / mean_pph7)`` households from the
    pph==7 bin (floored at zero).
    """
    gk = list(group_keys)
    ctpop['hhpop'] = ctpop['mean_pph'] * ctpop['household_count']
    aggr = ctpop.groupby(gk + ['year'], as_index=False).agg(tot=('hhpop', 'sum'))
    bin7 = ctpop[ctpop['pph'] == 7][gk + ['year', 'mean_pph']].rename(
        columns={'mean_pph': 'mean_pph7'}
    )
    aggr = aggr.merge(bin7, on=gk + ['year'], how='left')
    aggr = aggr.merge(
        controls[gk + ['year', hhpop_col]].rename(columns={hhpop_col: 'should_be'}),
        on=gk + ['year'], how='left',
    ).dropna(subset=['should_be'])
    aggr['dif'] = aggr['should_be'] - aggr['tot']
    difs = aggr[aggr['dif'] < 0]

    for _, row in difs.iterrows():
        if not np.isfinite(row['mean_pph7']) or row['mean_pph7'] <= 0:
            continue
        dhhs7 = int(np.floor(abs(row['dif']) / row['mean_pph7']))
        if dhhs7 <= 0:
            continue
        mask = (ctpop['year'] == row['year']) & (ctpop['pph'] == 7)
        for key in gk:
            mask &= ctpop[key] == row[key]
        ctpop.loc[mask, 'household_count'] = np.maximum(
            0.0, ctpop.loc[mask, 'household_count'] - dhhs7
        )
    return ctpop


def rebalance_hhs(ctpop, controls, rng, group_keys, hh_col):
    """Adjust HH counts in bins pph in {1..6} so per-(group, year) totals match.

    For each (group, year) with a difference ``d = should_be - tot``, sample at
    most ``min(|d|, k)`` pph bins without replacement (``k`` = bins with positive
    HH count), weighted by current HH count, and shift each sampled bin by
    ``sign(d)``. Loops until all groups match. Mirrors R ``rebalance.hhs``.

    Returns:
        int: Net households added (positive change across all rows).
    """
    gk = list(group_keys)
    orig = ctpop[gk + ['year', 'pph', 'household_count']].copy()
    subset_mask = ctpop['pph'] < 7

    max_loops = 10000  # safety; algorithm converges quickly
    for _ in range(max_loops):
        aggr = ctpop.groupby(gk + ['year'], as_index=False).agg(tot=('household_count', 'sum'))
        aggr = aggr.merge(
            controls[gk + ['year', hh_col]].rename(columns={hh_col: 'should_be'}),
            on=gk + ['year'], how='left',
        ).dropna(subset=['should_be'])
        aggr['dif'] = aggr['should_be'] - aggr['tot']
        difs = aggr[aggr['dif'].abs() > 0]
        if len(difs) == 0:
            break
        for _, row in difs.iterrows():
            geo_mask = (ctpop['year'] == row['year']) & subset_mask
            for key in gk:
                geo_mask &= ctpop[key] == row[key]
            sub = ctpop.loc[geo_mask, ['pph', 'household_count']]
            eligible = sub[sub['household_count'] > 0]
            if eligible.empty:
                continue
            size = int(min(abs(row['dif']), len(eligible)))
            if size <= 0:
                continue
            weights = eligible['household_count'].to_numpy(dtype=float)
            weights = weights / weights.sum()
            sampled = rng.choice(
                eligible['pph'].to_numpy(), size=size, replace=False, p=weights
            )
            sign = 1 if row['dif'] > 0 else -1
            apply_mask = geo_mask & ctpop['pph'].isin(sampled)
            ctpop.loc[apply_mask, 'household_count'] = (
                ctpop.loc[apply_mask, 'household_count'] + sign
            ).clip(lower=0)

    merged = orig.merge(
        ctpop[gk + ['year', 'pph', 'household_count']].rename(columns={'household_count': 'new_hh'}),
        on=gk + ['year', 'pph'], how='left',
    )
    gained = merged['new_hh'] > merged['household_count']
    lost = merged['new_hh'] < merged['household_count']
    added = merged.loc[gained, 'new_hh'].sum() - merged.loc[gained, 'household_count'].sum()
    subtracted = merged.loc[lost, 'household_count'].sum() - merged.loc[lost, 'new_hh'].sum()
    print(f'  Rebalancing HHs: {int(added)} added, {int(subtracted)} subtracted')
    return int(added)


def outer_rebalance(ctpop, controls, rng, group_keys, hh_col, hhpop_col,
                    max_iterations=20, min_added=10):
    """Run the alternating pop/HH rebalance loop until convergence."""
    for i in range(1, max_iterations + 1):
        print(f'\nIteration: {i}\n==================')
        ctpop = rebalance_pop(ctpop, controls, group_keys, hhpop_col)
        ctpop['hhpop'] = ctpop['mean_pph'] * ctpop['household_count']
        hhadded = rebalance_hhs(ctpop, controls, rng, group_keys, hh_col)
        ctpop['hhpop'] = ctpop['mean_pph'] * ctpop['household_count']
        if hhadded < min_added:
            break
    return ctpop
