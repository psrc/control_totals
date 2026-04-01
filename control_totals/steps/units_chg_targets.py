import pandas as pd
from util import Pipeline, calc_gq


def load_tables(pipeline):
    """Load and merge adjusted targets with OFM estimates for unit-change counties.

    Joins adjusted unit and population change targets with base-year OFM
    estimates aggregated to target areas, calculates vacancy rates by RGID,
    and computes group quarters for the target horizon year.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.

    Returns:
        pandas.DataFrame: Merged DataFrame with OFM estimates, vacancy rates,
            and group-quarters figures for the target horizon year.
    """
    p = pipeline
    base_year = p.settings['base_year']
    target_year = p.settings['targets_end_year']

    # get units and pop targets that have been adjusted to base year
    units = p.get_table('adjusted_units_change_targets').drop(columns=['start'])
    pop = p.get_table('adjusted_total_pop_change_targets').drop(columns=['start'])
    df = pop.merge(units, on='target_id',how='inner')
    xwalk = p.get_table('control_target_xwalk')
    
    # filter to only counties that are using unit change targets in settings.yaml
    included_counties = p.settings['target_types']['unit_chg']
    df = df[df['county_id'].isin(included_counties)].copy()

    # get ofm estimates for base year
    ofm = p.get_table(f'ofm_parcelized_{base_year}_by_control_area')
    ofm = ofm.merge(xwalk,on='control_id',how='left')
    # aggregate to target areas
    ofm = ofm.groupby('target_id').agg({
        'rgid':'first',
        'county_id':'first',
        'ofm_total_pop':'sum',
        'ofm_hhpop':'sum',
        'ofm_units':'sum',
        'ofm_hh':'sum',
        'ofm_gq':'sum'
    }).reset_index()
    # calculate vacancy rate by RGID for use in calculating targets
    ofm['ofm_vacancy_by_rgid'] = \
        1 - ofm.groupby(['rgid','county_id'])['ofm_hh'].transform('sum') / ofm.groupby(['rgid','county_id'])['ofm_units'].transform('sum')
    # join targets to ofm estimates
    df = df.merge(ofm,on='target_id',how='left')
    # calculate gq for target year
    df = calc_gq(p,df,ofm,target_year,'OFM')
    return df

def targets_calculations(pipeline, df):
    """Calculate horizon-year population and housing targets using unit changes.

    Computes total population, household population, housing units,
    households (using OFM vacancy rates by RGID), and implied household
    size for the targets horizon year.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.
        df (pandas.DataFrame): Merged targets-and-estimates DataFrame
            produced by :func:`load_tables`.

    Returns:
        pandas.DataFrame: The input DataFrame with added horizon-year
            columns for total population, household population, housing
            units, households, and household size.
    """
    p = pipeline
    targets_end_year = p.settings["targets_end_year"]
    total_pop_horizon_col = f'total_pop_{targets_end_year}'
    hhpop_horizon_col = f'hhpop_{targets_end_year}'
    gq_horizon_col = f'gq_{targets_end_year}'
    hhsz_horizon_col = f'hhsz_{targets_end_year}'
    hh_horizon_col = f'hh_{targets_end_year}'
    units_horizon_col = f'units_{targets_end_year}'

    # calculate total population for horizon year using REF GQ
    df[total_pop_horizon_col] = df['ofm_total_pop'] + df['total_pop_chg_adj']
    # calculate hhpop for horizon year
    df[hhpop_horizon_col] = df[total_pop_horizon_col] - df[gq_horizon_col]
    # calculate housing units for horizon year
    df[units_horizon_col] = df['ofm_units'] + df['units_chg_adj']
    # calculat hhlds using OFM vacancy rate by RGID
    df[hh_horizon_col] = df[units_horizon_col] * (1 - df['ofm_vacancy_by_rgid'])
    # calcualte implied hhsz for horizon year for reference
    df[hhsz_horizon_col] = df[hhpop_horizon_col] / df[hh_horizon_col]
    return df

def run_step(context):
    """Execute the unit-change targets pipeline step.

    Calculates targets for counties that use housing-unit changes, then
    persists the result as ``'adjusted_units_change_targets'``.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    p = Pipeline(settings_path=context['configs_dir'])
    print('Calculating targets for counties that use housing targets...')
    df = load_tables(p)
    df = targets_calculations(p,df)
    p.save_table('adjusted_units_change_targets',df)
    return context

    