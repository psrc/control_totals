import pandas as pd
import numpy as np
from pathlib import Path
from util import Pipeline

RES_BUILDING_TYPES = frozenset({4, 12, 19})


def load_tables(prop_path, lookup_path):
    """Load CSV tables from the proposals and lookup directories.

    Reads buildings, parcels, templates, template components, and
    building-sqft-per-job from *lookup_path*, plus proposals and
    proposal components from *prop_path*.  Excludes MPD proposals
    (``status_id == 3``) and limits components to valid proposals.

    Args:
        prop_path (str or Path): Directory containing development project
            proposals and components CSVs from an unlimited UrbanSim run.
        lookup_path (str or Path): Directory containing base-year lookup
            CSVs (buildings, parcels, templates, constraints, sqft-per-job).

    Returns:
        dict[str, pandas.DataFrame]: Loaded and pre-filtered tables keyed
            by name.
    """
    prop_path, lookup_path = Path(prop_path), Path(lookup_path)
    tables = {
        'buildings': pd.read_csv(lookup_path / 'buildings.csv'),
        'proposals': pd.read_csv(prop_path / 'development_project_proposals.csv'),
        'components': pd.read_csv(prop_path / 'development_project_proposal_components.csv'),
        'parcels': pd.read_csv(lookup_path / 'parcels.csv'),
        'templates': pd.read_csv(lookup_path / 'development_templates.csv'),
        'template_components': pd.read_csv(lookup_path / 'development_template_components.csv'),
        'bsqft_per_job': pd.read_csv(lookup_path / 'building_sqft_per_job.csv'),
    }
    # exclude MPDs
    tables['proposals'] = tables['proposals'].loc[
        tables['proposals']['status_id'] != 3
    ].copy()
    valid_ids = set(tables['proposals']['proposal_id'])
    tables['components'] = tables['components'].loc[
        tables['components']['proposal_id'].isin(valid_ids)
    ].copy()
    return tables


def prepare_buildings(bld_base):
    """Impute missing sqft_per_unit, compute building_sqft, and aggregate to parcel level.

    Residential buildings of type 19 with zero sqft_per_unit are imputed
    to 1000; all other residential types are imputed to 500.  Building
    sqft is adjusted upward when it falls below non-residential sqft.

    Args:
        bld_base (pandas.DataFrame): Raw buildings table from CSV.

    Returns:
        pandas.DataFrame: Parcel-level stock with columns ``pcl_resunits``,
            ``pcl_nonres_sqft``, ``pcl_bldsqft``, ``pcl_job_capacity``.
    """
    bld = bld_base.copy()
    mask_19 = (
        (bld['residential_units'] > 0)
        & (bld['building_type_id'] == 19)
        & (bld['sqft_per_unit'] == 0)
    )
    bld.loc[mask_19, 'sqft_per_unit'] = 1000
    mask_other = (
        (bld['residential_units'] > 0)
        & (bld['building_type_id'] != 19)
        & (bld['sqft_per_unit'] == 0)
    )
    bld.loc[mask_other, 'sqft_per_unit'] = 500
    bld['building_sqft'] = bld['residential_units'] * bld['sqft_per_unit']

    bld = bld[
        ['parcel_id', 'residential_units', 'non_residential_sqft', 'building_sqft', 'job_capacity']
    ].copy()
    adj = (bld['non_residential_sqft'] > 0) & (bld['building_sqft'] < bld['non_residential_sqft'])
    bld.loc[adj, 'building_sqft'] = bld.loc[adj, 'non_residential_sqft']

    return bld.groupby('parcel_id').agg(
        pcl_resunits=('residential_units', 'sum'),
        pcl_nonres_sqft=('non_residential_sqft', 'sum'),
        pcl_bldsqft=('building_sqft', 'sum'),
        pcl_job_capacity=('job_capacity', 'sum'),
    ).reset_index()


def prepare_proposals(tables, pclstock):
    """Merge proposals with components, templates, and stock; compute proposed units and flags.

    Disaggregates proposals into components, computes proposed dwelling
    units and building sqft based on density type (FAR vs. unit-based),
    classifies each proposal as residential / non-residential, and
    filters out proposals smaller than the existing parcel stock.

    Args:
        tables (dict[str, pandas.DataFrame]): Loaded CSV tables.
        pclstock (pandas.DataFrame): Parcel-level building stock.

    Returns:
        pandas.DataFrame: Filtered proposal-component rows with computed
            fields ``proposed_units_new``, ``building_sqft``,
            ``has_non_res``, ``has_res``.
    """
    props = tables['proposals'].copy()
    props = props.merge(tables['templates'][['template_id', 'density_type']], on='template_id')
    prop = props.merge(pclstock, on='parcel_id', how='left')
    prop = prop.merge(tables['parcels'][['parcel_id', 'zone_id']], on='parcel_id')

    propc = prop.merge(
        tables['components'][
            ['building_type_id', 'component_id', 'expected_sales_price_per_sqft', 'proposal_id']
        ],
        on='proposal_id',
    )
    propc = propc.merge(
        tables['template_components'][
            ['template_id', 'component_id', 'building_sqft_per_unit', 'percent_building_sqft']
        ],
        on=['template_id', 'component_id'],
    )
    propc = propc.merge(tables['bsqft_per_job'], on=['building_type_id', 'zone_id'], how='left')

    is_far = propc['density_type'] == 'far'
    propc['proposed_units_new'] = np.where(
        is_far,
        np.maximum(1, propc['units_proposed_orig'] / propc['building_sqft_per_unit']),
        propc['units_proposed_orig'],
    ) * propc['percent_building_sqft'] / 100.0

    propc['building_sqft'] = np.where(
        ~is_far,
        propc['units_proposed_orig'] * propc['building_sqft_per_unit'],
        propc['units_proposed_orig'],
    ) * propc['percent_building_sqft'] / 100.0

    # per-proposal flags
    propc['has_non_res'] = ~propc.groupby('proposal_id')['building_type_id'].transform(
        lambda s: s.isin(RES_BUILDING_TYPES).all()
    )
    propc['has_res'] = propc.groupby('proposal_id')['building_type_id'].transform(
        lambda s: s.isin(RES_BUILDING_TYPES).any()
    )

    # filter out proposals smaller than existing stock
    keep = (
        propc['pcl_bldsqft'].isna()
        | ((propc['units_proposed_orig'] > propc['pcl_resunits']) & ~is_far)
        | ((propc['units_proposed_orig'] > propc['pcl_nonres_sqft']) & is_far)
    )
    return propc.loc[keep].copy()


def _agg_mix_with_hbc(df, kind):
    """Aggregate mixed-use components by (parcel_id, proposal_id) with has_both_comp.

    Computes ``has_both_comp`` using the same logic as the R script:
    ``sum(has_non_res, has_res) > 1`` within each group, i.e. the
    proposal is a genuine mixed-use proposal or has multiple components
    of at least one type.

    Args:
        df (pandas.DataFrame): Subset of proposal components on mixed-use
            parcels filtered to one building-type category.
        kind (str): ``'res'`` for residential, ``'non_res'`` for
            non-residential.

    Returns:
        pandas.DataFrame: Aggregated proposals with ``has_both_comp`` flag.
    """
    if kind == 'res':
        empty_cols = ['parcel_id', 'proposal_id', 'residential_units', 'building_sqft', 'has_both_comp']
    else:
        empty_cols = [
            'parcel_id', 'proposal_id', 'non_residential_sqft', 'building_sqft',
            'job_capacity', 'has_both_comp',
        ]
    if df.empty:
        return pd.DataFrame(columns=empty_cols)

    if kind == 'non_res':
        df = df.copy()
        df['comp_jobs'] = np.maximum(1, np.round(df['proposed_units_new'] / df['building_sqft_per_job']))

    grp = df.groupby(['parcel_id', 'proposal_id'])
    if kind == 'res':
        agg = grp.agg(
            residential_units=('proposed_units_new', 'sum'),
            building_sqft=('building_sqft', 'sum'),
        ).reset_index()
    else:
        agg = grp.agg(
            non_residential_sqft=('proposed_units_new', 'sum'),
            building_sqft=('building_sqft', 'sum'),
            job_capacity=('comp_jobs', 'sum'),
        ).reset_index()

    # has_both_comp: n * has_non_res + n * has_res > 1  (matching R logic)
    flags = grp.agg(
        n=('proposed_units_new', 'size'),
        hnr=('has_non_res', 'first'),
        hr=('has_res', 'first'),
    ).reset_index()
    flags['has_both_comp'] = (
        flags['n'] * flags['hnr'].astype(int) + flags['n'] * flags['hr'].astype(int)
    ) > 1
    return agg.merge(flags[['parcel_id', 'proposal_id', 'has_both_comp']], on=['parcel_id', 'proposal_id'])


def aggregate_proposals(propc):
    """Split proposals by parcel type and aggregate to (parcel_id, proposal_id).

    Classifies parcels as residential-only, non-residential-only, or
    mixed-use based on their proposal flags, then aggregates proposed
    units, sqft, and job capacity within each group.

    Args:
        propc (pandas.DataFrame): Filtered proposal-component rows.

    Returns:
        tuple[pandas.DataFrame, pandas.DataFrame, pandas.DataFrame, pandas.DataFrame]:
            ``(res_units, non_res, res_units_mix, non_res_mix)`` aggregated
            by ``(parcel_id, proposal_id)``.
    """
    pcl_agg = propc.groupby('parcel_id').agg(
        s_nr=('has_non_res', 'sum'),
        s_r=('has_res', 'sum'),
    ).reset_index()
    res_pcls = set(pcl_agg.loc[pcl_agg['s_nr'] == 0, 'parcel_id'])
    nr_pcls = set(pcl_agg.loc[pcl_agg['s_r'] == 0, 'parcel_id'])
    mix_pcls = set(pcl_agg.loc[(pcl_agg['s_r'] > 0) & (pcl_agg['s_nr'] > 0), 'parcel_id'])

    is_res_bt = propc['building_type_id'].isin(RES_BUILDING_TYPES)

    # res-only parcels
    res_units = (
        propc.loc[propc['parcel_id'].isin(res_pcls)]
        .groupby(['parcel_id', 'proposal_id'])
        .agg(residential_units=('proposed_units_new', 'sum'), building_sqft=('building_sqft', 'sum'))
        .reset_index()
    )

    # non-res-only parcels
    nr_data = propc.loc[propc['parcel_id'].isin(nr_pcls)].copy()
    nr_data['comp_jobs'] = np.maximum(
        1, np.round(nr_data['proposed_units_new'] / nr_data['building_sqft_per_job'])
    )
    non_res = (
        nr_data.groupby(['parcel_id', 'proposal_id'])
        .agg(
            non_residential_sqft=('proposed_units_new', 'sum'),
            job_capacity=('comp_jobs', 'sum'),
            building_sqft=('building_sqft', 'sum'),
        )
        .reset_index()
    )

    # mixed-use parcels
    res_units_mix = _agg_mix_with_hbc(
        propc.loc[propc['parcel_id'].isin(mix_pcls) & is_res_bt], 'res'
    )
    non_res_mix = _agg_mix_with_hbc(
        propc.loc[propc['parcel_id'].isin(mix_pcls) & ~is_res_bt], 'non_res'
    )

    return res_units, non_res, res_units_mix, non_res_mix


def filter_undersized(res_units, non_res, res_units_mix, non_res_mix, pclstock):
    """Remove proposals yielding fewer units / sqft than existing parcel stock.

    Mixed-use proposals flagged ``has_both_comp=True`` (genuine mixed-use
    proposals) are retained regardless of stock comparison.

    Args:
        res_units (pandas.DataFrame): Residential proposals.
        non_res (pandas.DataFrame): Non-residential proposals.
        res_units_mix (pandas.DataFrame): Mixed-use residential proposals.
        non_res_mix (pandas.DataFrame): Mixed-use non-residential proposals.
        pclstock (pandas.DataFrame): Parcel-level building stock.

    Returns:
        tuple[pandas.DataFrame, pandas.DataFrame, pandas.DataFrame, pandas.DataFrame]:
            Filtered proposal DataFrames.
    """
    if not non_res.empty:
        nr = non_res.merge(
            pclstock[['parcel_id', 'pcl_job_capacity', 'pcl_nonres_sqft']], on='parcel_id', how='left'
        )
        nr = nr.loc[nr['pcl_job_capacity'].isna() | (nr['pcl_job_capacity'] < nr['job_capacity'])]
        nr = nr.loc[nr['pcl_nonres_sqft'].isna() | (nr['pcl_nonres_sqft'] < nr['non_residential_sqft'])]
        non_res = nr.drop(columns=['pcl_job_capacity', 'pcl_nonres_sqft'])

    if not res_units.empty:
        ru = res_units.merge(pclstock[['parcel_id', 'pcl_resunits']], on='parcel_id', how='left')
        ru = ru.loc[ru['pcl_resunits'].isna() | (ru['pcl_resunits'] < ru['residential_units'])]
        res_units = ru.drop(columns=['pcl_resunits'])

    if not res_units_mix.empty:
        rum = res_units_mix.merge(pclstock[['parcel_id', 'pcl_resunits']], on='parcel_id', how='left')
        rum = rum.loc[
            rum['has_both_comp'] | rum['pcl_resunits'].isna() | (rum['pcl_resunits'] < rum['residential_units'])
        ]
        res_units_mix = rum.drop(columns=['pcl_resunits'])

    if not non_res_mix.empty:
        nrm = non_res_mix.merge(
            pclstock[['parcel_id', 'pcl_nonres_sqft', 'pcl_job_capacity']], on='parcel_id', how='left'
        )
        nrm = nrm.loc[
            nrm['has_both_comp']
            | nrm['pcl_nonres_sqft'].isna()
            | (nrm['pcl_nonres_sqft'] < nrm['non_residential_sqft'])
        ]
        nrm = nrm.loc[
            nrm['has_both_comp']
            | nrm['pcl_job_capacity'].isna()
            | (nrm['pcl_job_capacity'] < nrm['job_capacity'])
        ]
        non_res_mix = nrm.drop(columns=['pcl_nonres_sqft', 'pcl_job_capacity'])

    return res_units, non_res, res_units_mix, non_res_mix


def _resolve_mixed_use(res_units_mix, non_res_mix, res_ratio, mu_sampling, rng_seed):
    """Combine res and non-res max proposals for mixed-use parcels.

    For parcels where the max residential and max non-residential
    proposals are the same (or only one side exists), uses that single
    proposal.  For parcels where the two maxima differ, either samples
    one proposal (when *mu_sampling* is True) or takes both and scales
    their values by *res_ratio* / (100 - res_ratio).

    Args:
        res_units_mix (pandas.DataFrame): Mixed-use residential proposals.
        non_res_mix (pandas.DataFrame): Mixed-use non-residential proposals.
        res_ratio (int): Residential share percentage (0–100).
        mu_sampling (bool): Use parcel-level sampling if True.
        rng_seed (int): Random seed for reproducibility.

    Returns:
        pandas.DataFrame: One row per mixed-use parcel with columns
            ``parcel_id``, ``residential_units_prop``,
            ``building_sqft_prop``, ``non_residential_sqft_prop``,
            ``job_capacity_prop``.
    """
    out_cols = [
        'parcel_id', 'residential_units_prop', 'building_sqft_prop',
        'non_residential_sqft_prop', 'job_capacity_prop',
    ]
    if res_units_mix.empty and non_res_mix.empty:
        return pd.DataFrame(columns=out_cols)

    # select max proposal per parcel for each type
    r_max = (
        res_units_mix.loc[res_units_mix.groupby('parcel_id')['residential_units'].idxmax()]
        if not res_units_mix.empty
        else pd.DataFrame(columns=res_units_mix.columns)
    )
    nr_max = (
        non_res_mix.loc[non_res_mix.groupby('parcel_id')['non_residential_sqft'].idxmax()]
        if not non_res_mix.empty
        else pd.DataFrame(columns=non_res_mix.columns)
    )

    # outer merge on parcel_id
    r_cols = (
        r_max[['parcel_id', 'proposal_id', 'has_both_comp']]
        .rename(columns={'proposal_id': 'pid_r', 'has_both_comp': 'hbc_r'})
        if not r_max.empty
        else pd.DataFrame(columns=['parcel_id', 'pid_r', 'hbc_r'])
    )
    nr_cols = (
        nr_max[['parcel_id', 'proposal_id', 'has_both_comp']]
        .rename(columns={'proposal_id': 'pid_nr', 'has_both_comp': 'hbc_nr'})
        if not nr_max.empty
        else pd.DataFrame(columns=['parcel_id', 'pid_nr', 'hbc_nr'])
    )
    comb = r_cols.merge(nr_cols, on='parcel_id', how='outer')

    # resolve a single proposal_id per parcel
    same_or_r = (comb['pid_r'] == comb['pid_nr']) | comb['pid_nr'].isna()
    nr_only = comb['pid_r'].isna()
    comb['use_pid'] = np.where(same_or_r, comb['pid_r'], np.where(nr_only, comb['pid_nr'], np.nan))

    unresolved = comb['use_pid'].isna()

    if mu_sampling and unresolved.any():
        rng = np.random.RandomState(rng_seed)
        n = unresolved.sum()
        choose_r = rng.random(n) < (res_ratio / 100.0)
        comb.loc[unresolved, 'use_pid'] = np.where(
            choose_r,
            comb.loc[unresolved, 'pid_r'].values,
            comb.loc[unresolved, 'pid_nr'].values,
        )
        unresolved = comb['use_pid'].isna()

    # fill values for resolved parcels via use_pid
    comb = comb.merge(
        res_units_mix[['parcel_id', 'proposal_id', 'residential_units', 'building_sqft']].rename(
            columns={'proposal_id': 'use_pid', 'residential_units': 'ru', 'building_sqft': 'bs_r'}
        ),
        on=['parcel_id', 'use_pid'],
        how='left',
    )
    comb = comb.merge(
        non_res_mix[
            ['parcel_id', 'proposal_id', 'non_residential_sqft', 'building_sqft', 'job_capacity']
        ].rename(
            columns={
                'proposal_id': 'use_pid',
                'non_residential_sqft': 'nrs',
                'building_sqft': 'bs_nr',
                'job_capacity': 'jc',
            }
        ),
        on=['parcel_id', 'use_pid'],
        how='left',
    )

    # handle unresolved parcels (no-sampling mode)
    if not mu_sampling and unresolved.any():
        u = comb.loc[unresolved].copy().reset_index()
        # look up res values via pid_r
        u = u.merge(
            res_units_mix[['parcel_id', 'proposal_id', 'residential_units', 'building_sqft']].rename(
                columns={'proposal_id': 'pid_r', 'residential_units': 'u_ru', 'building_sqft': 'u_bsr'}
            ),
            on=['parcel_id', 'pid_r'],
            how='left',
        )
        # look up nr values via pid_nr
        u = u.merge(
            non_res_mix[
                ['parcel_id', 'proposal_id', 'non_residential_sqft', 'building_sqft', 'job_capacity']
            ].rename(
                columns={
                    'proposal_id': 'pid_nr',
                    'non_residential_sqft': 'u_nrs',
                    'building_sqft': 'u_bsnr',
                    'job_capacity': 'u_jc',
                }
            ),
            on=['parcel_id', 'pid_nr'],
            how='left',
        )
        ratio = res_ratio / 100.0
        hr = u['hbc_r'].fillna(False)
        hn = u['hbc_nr'].fillna(False)
        idx = u['index']  # original comb index
        comb.loc[idx, 'ru'] = np.where(hr, u['u_ru'], ratio * u['u_ru'])
        comb.loc[idx, 'bs_r'] = np.where(hr, u['u_bsr'], ratio * u['u_bsr'])
        comb.loc[idx, 'nrs'] = np.where(hn, u['u_nrs'], (1 - ratio) * u['u_nrs'])
        comb.loc[idx, 'bs_nr'] = np.where(hn, u['u_bsnr'], (1 - ratio) * u['u_bsnr'])
        comb.loc[idx, 'jc'] = np.where(hn, u['u_jc'], (1 - ratio) * u['u_jc'])

    # fill NAs and compute final columns
    comb['residential_units_prop'] = comb['ru'].fillna(0)
    comb['non_residential_sqft_prop'] = comb['nrs'].fillna(0)
    comb['job_capacity_prop'] = comb['jc'].fillna(0)
    comb['building_sqft_prop'] = comb['bs_r'].fillna(0) + comb['bs_nr'].fillna(0)
    return comb[out_cols].copy()


def select_max_and_combine(res_units, non_res, res_units_mix, non_res_mix,
                           res_ratio, mu_sampling, rng_seed):
    """Select the maximum proposal per parcel for each type and combine.

    For residential-only and non-residential-only parcels, selects the
    proposal with the highest residential_units or non_residential_sqft.
    For mixed-use parcels, delegates to :func:`_resolve_mixed_use`.

    Args:
        res_units (pandas.DataFrame): Residential proposals.
        non_res (pandas.DataFrame): Non-residential proposals.
        res_units_mix (pandas.DataFrame): Mixed-use residential proposals.
        non_res_mix (pandas.DataFrame): Mixed-use non-residential proposals.
        res_ratio (int): Residential share percentage for mixed-use parcels.
        mu_sampling (bool): Use parcel-level sampling for mixed-use.
        rng_seed (int): Random seed for reproducibility.

    Returns:
        pandas.DataFrame: One row per parcel with columns ``parcel_id``,
            ``residential_units_prop``, ``building_sqft_prop``,
            ``non_residential_sqft_prop``, ``job_capacity_prop``.
    """
    out_cols = [
        'parcel_id', 'residential_units_prop', 'building_sqft_prop',
        'non_residential_sqft_prop', 'job_capacity_prop',
    ]
    parts = []

    # res-only: max by residential_units
    if not res_units.empty:
        r_max = res_units.loc[res_units.groupby('parcel_id')['residential_units'].idxmax()]
        parts.append(pd.DataFrame({
            'parcel_id': r_max['parcel_id'].values,
            'residential_units_prop': r_max['residential_units'].values,
            'building_sqft_prop': r_max['building_sqft'].values,
            'non_residential_sqft_prop': 0,
            'job_capacity_prop': 0,
        }))

    # non-res-only: max by non_residential_sqft
    if not non_res.empty:
        nr_max = non_res.loc[non_res.groupby('parcel_id')['non_residential_sqft'].idxmax()]
        parts.append(pd.DataFrame({
            'parcel_id': nr_max['parcel_id'].values,
            'non_residential_sqft_prop': nr_max['non_residential_sqft'].values,
            'job_capacity_prop': nr_max['job_capacity'].values,
            'building_sqft_prop': nr_max['building_sqft'].values,
            'residential_units_prop': 0,
        }))

    # mixed-use
    mix = _resolve_mixed_use(res_units_mix, non_res_mix, res_ratio, mu_sampling, rng_seed)
    if not mix.empty:
        parts.append(mix)

    if parts:
        return pd.concat(parts, ignore_index=True)[out_cols]
    return pd.DataFrame(columns=out_cols)


def compute_capacity(comb_max, pclstock, pcl):
    """Merge proposal capacity with existing stock and produce final output.

    For parcels with proposals, capacity equals the proposed values.
    For parcels without proposals, capacity equals the base-year stock.
    Joins parcel attributes (control_id, tod_id, subreg_id, etc.) from
    the parcels table.

    Args:
        comb_max (pandas.DataFrame): Combined max proposals per parcel.
        pclstock (pandas.DataFrame): Parcel-level building stock.
        pcl (pandas.DataFrame): Full parcels table with geographic attributes.

    Returns:
        pandas.DataFrame: Final capacity table with base and capacity
            columns plus parcel attributes.
    """
    all_pcls = comb_max.merge(pclstock, on='parcel_id', how='outer')

    all_pcls['DUbase'] = all_pcls['pcl_resunits'].fillna(0)
    all_pcls['NRSQFbase'] = all_pcls['pcl_nonres_sqft'].fillna(0)
    all_pcls['JOBSPbase'] = all_pcls['pcl_job_capacity'].fillna(0)
    all_pcls['BLSQFbase'] = all_pcls['pcl_bldsqft'].fillna(0)

    all_pcls['DUcapacity'] = np.where(
        all_pcls['residential_units_prop'].isna(), all_pcls['DUbase'], all_pcls['residential_units_prop']
    )
    all_pcls['NRSQFcapacity'] = np.where(
        all_pcls['non_residential_sqft_prop'].isna(), all_pcls['NRSQFbase'], all_pcls['non_residential_sqft_prop']
    )
    all_pcls['JOBSPcapacity'] = np.where(
        all_pcls['job_capacity_prop'].isna(), all_pcls['JOBSPbase'], all_pcls['job_capacity_prop']
    )
    all_pcls['BLSQFcapacity'] = np.where(
        all_pcls['building_sqft_prop'].isna(), all_pcls['BLSQFbase'], all_pcls['building_sqft_prop']
    )

    result = all_pcls[
        ['parcel_id', 'DUbase', 'DUcapacity', 'NRSQFbase', 'NRSQFcapacity',
         'JOBSPbase', 'JOBSPcapacity', 'BLSQFbase', 'BLSQFcapacity']
    ]
    pcl_attrs = pcl[['parcel_id', 'control_id', 'tod_id', 'subreg_id', 'hb_hct_buffer', 'hb_tier']]
    return pcl_attrs.merge(result, on='parcel_id')

def update_ids(result, parcels_hct):
    """Update control_id and subreg_id."""
    result.drop(columns=['control_id', 'subreg_id'], inplace=True, errors='ignore')
    result = result.merge(parcels_hct[['parcel_id', 'control_id', 'subreg_id']], on='parcel_id', how='left')
    return result


def run_step(context):
    """Execute the parcels capacity pipeline step.

    Reads UrbanSim proposal and base-year building CSVs from directories
    configured under ``parcels_capacity`` in settings.yaml, computes
    per-parcel development capacity, and saves the result to CSV and the
    pipeline HDF5 store.

    Expected settings.yaml block::

        parcels_capacity:
          prop_path: "path/to/proposals/csv/year"
          lookup_path: "path/to/base_year/csv/year"
          res_ratio: 50           # residential share for mixed-use (0-100)
          mu_sampling: false      # sample parcels (true) or apply ratio (false)
          rng_seed: 1
          save_csv: true
          file_prefix: "CapacityPclNoSampling_res50"

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    print('Computing parcel capacity...')
    p = Pipeline(settings_path=context['configs_dir'])

    cfg = p.settings['parcels_capacity']
    prop_path = cfg['prop_path']
    lookup_path = cfg['lookup_path']
    res_ratio = cfg.get('res_ratio', 50)
    mu_sampling = cfg.get('mu_sampling', False)
    rng_seed = cfg.get('rng_seed', 1)
    save_csv = cfg.get('save_csv', True)
    file_prefix = cfg.get('file_prefix', f'CapacityPclNoSampling_res{res_ratio}')

    tables = load_tables(prop_path, lookup_path)
    pclstock = prepare_buildings(tables['buildings'])
    propc = prepare_proposals(tables, pclstock)
    res_units, non_res, res_units_mix, non_res_mix = aggregate_proposals(propc)
    res_units, non_res, res_units_mix, non_res_mix = filter_undersized(
        res_units, non_res, res_units_mix, non_res_mix, pclstock
    )
    comb_max = select_max_and_combine(
        res_units, non_res, res_units_mix, non_res_mix, res_ratio, mu_sampling, rng_seed
    )
    result = compute_capacity(comb_max, pclstock, tables['parcels'])
    result = update_ids(result,p.get_geodataframe('parcels_hct'))
    if save_csv:
        out_path = Path(p.get_output_dir()) / f'{file_prefix}.csv'
        result.to_csv(out_path, index=False)
        print(f'  Saved capacity CSV to {out_path}')

    p.save_table('parcels_capacity', result)
    print(f'  {len(result):,} parcels with capacity data')
    return context
