import pandas as pd
from util import Pipeline, load_input_tables, calc_gq


def load_hhsz_vacancy_rates(pipeline):
    """Load hard-coded King County household sizes and vacancy rates.

    Retrieves the per-RGID household sizes and vacancy rates from the
    pipeline settings.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.

    Returns:
        tuple:
            dict: Mapping of RGID to household size.
            dict: Mapping of RGID to vacancy rate percentage.
    """
    p = pipeline
    # load hard coded king county hhsz and vacancy rates
    king_hhsz = p.settings['king_hhsz']
    king_vac_rates = p.settings['king_vac']
    return king_hhsz, king_vac_rates


def calc_by_rgid(pipeline, targets_df):
    """Calculate housing and population targets aggregated to the RGID level.

    Sums targets to RGID, applies hard-coded household sizes and vacancy
    rates, computes housing units and households, and factors household
    population to match the forced regional total from settings.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.
        targets_df (pandas.DataFrame): Per-target-area DataFrame containing
            decennial housing units and adjusted unit changes.

    Returns:
        pandas.DataFrame: RGID-level DataFrame with columns for horizon-year
            units, households, initial and factored household population.
    """
    p = pipeline
    
    # load target horizon year and set column names for target horizon year
    targets_end_year = p.settings['targets_end_year']
    units_horizon_col = f'units_{targets_end_year}'
    hh_horizon_col = f'hh_{targets_end_year}'
    hhpop_init_horizon_col = f'hhpop_initial_{targets_end_year}'
    hhpop_factored_horizon_col = f'hhpop_factored_{targets_end_year}'
    hhsz_horizon_col = f'hhsz_{targets_end_year}'
    
    #group by rgid to get totals for each rgid
    df = targets_df.drop(columns=['target_id','start','county_id']).groupby('rgid').sum().reset_index()

    # load hard coded king county hhsz and vacancy rates
    king_hhsz, king_vac_rates = load_hhsz_vacancy_rates(p)

    # add hhsz and vacancy rate using hard coded rates from settings.yaml
    df[hhsz_horizon_col] = df['rgid'].map(king_hhsz)
    df['vacancy_rate'] = df['rgid'].map(king_vac_rates)


    # calcuate horizon year units
    df[units_horizon_col] = df['dec_units'] + df['units_chg_adj']

    # calculate horizon year households
    df[hh_horizon_col] = (df[units_horizon_col] * (1 - df['vacancy_rate']/100)).round(0).astype(int)

    # calculate horizon year initial household population (unfactored)
    df[hhpop_init_horizon_col] = (df[hh_horizon_col] * df[hhsz_horizon_col]).round(0).astype(int)

    # load total household population target for horizon year
    hhpop_horizon_forced_total = p.settings['king_hhpop_2044']

    # factor household population to match target
    hhpop_horizon_sum = df[hhpop_init_horizon_col].sum()
    hhpop_factor = hhpop_horizon_forced_total / hhpop_horizon_sum
    df[hhpop_factored_horizon_col] = (df[hhpop_init_horizon_col] * hhpop_factor).round(0).astype(int)
    return df


def calc_by_target_area(pipeline, df, targets_rgid):
    """Calculate housing and population targets at the target-area level.

    Applies adjusted household sizes and vacancy rates per RGID, computes
    horizon-year units, households, and household population for each
    target area, then factors population to match the RGID-level totals.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.
        df (pandas.DataFrame): Per-target-area DataFrame with decennial and
            adjusted target values.
        targets_rgid (pandas.DataFrame): RGID-level targets produced by
            :func:`calc_by_rgid`.

    Returns:
        pandas.DataFrame: The input DataFrame with additional horizon-year
            columns for units, households, and factored household population.
    """
    p = pipeline

    # load hard coded king county hhsz and vacancy rates
    king_hhsz, king_vac_rates = load_hhsz_vacancy_rates(p)

    # get adjusted household sizes for king metro areas
    king_metro_adj_hhsz = p.settings['king_metro_adj_hhsz']
    # replace metro value in king county hhsz dict
    king_hhsz_adj = king_hhsz.copy()
    king_hhsz_adj[1] = king_metro_adj_hhsz
    # map adjusted hhsz to df
    df['king_hhsz'] = df['rgid'].map(king_hhsz_adj)

    # map vacancy rates to df
    df['vacancy_rate'] = df['rgid'].map(king_vac_rates)

    # load target horizon year and set column names for target horizon year
    targets_end_year = p.settings['targets_end_year']
    units_horizon_col = f'units_{targets_end_year}'
    hh_horizon_col = f'hh_{targets_end_year}'
    hhpop_init_horizon_col = f'hhpop_initial_{targets_end_year}'
    hhpop_factored_horizon_col = f'hhpop_factored_{targets_end_year}'
    hhsz_horizon_col = f'hhsz_{targets_end_year}'
    
    #  calculate units for target horizon year
    df[units_horizon_col] = df['units_chg_adj'] + df['dec_units']

    # calculate households for target horizon year
    df[hh_horizon_col] = (df[units_horizon_col] * (1 - df['vacancy_rate']/100)).round(0).astype(int)

    # calculate adjusted hhsz
    df['dec_hhsz_by_rgid'] = df.groupby('rgid')['dec_hhpop'].transform('sum') / df.groupby('rgid')['dec_hh'].transform('sum')
    df[hhsz_horizon_col] = df['king_hhsz'] / df['dec_hhsz_by_rgid'] * df['dec_hhsz']

    # if hhsz is greater than 5, use original value
    df.loc[df[hhsz_horizon_col]>5, hhsz_horizon_col] = df.loc[df[hhsz_horizon_col]>5, 'king_hhsz']

    # calculate initial hhpop for target horizon year
    df[hhpop_init_horizon_col] = (df[hh_horizon_col] * df[hhsz_horizon_col]).round(0).astype(int)

    # sum initial hhpop by rgid and add as a column
    hhpop_horizon_sum_by_rgid_col = f'initial_hhpop_{targets_end_year}_sum_by_rgid'
    df[hhpop_horizon_sum_by_rgid_col] = df.groupby('rgid')[hhpop_init_horizon_col].transform('sum')

    # merge factored hhpop by rgid to df
    df = (
        df.merge(targets_rgid[['rgid', hhpop_factored_horizon_col]]
                .rename(columns={
                    hhpop_factored_horizon_col: f'hhpop_rgid_factored_{targets_end_year}'
                    }), on='rgid', how='left')
    )

    # calculate factored hhpop for target horizon year
    df['hhpop_factor'] = df[f'hhpop_rgid_factored_{targets_end_year}'] / df[hhpop_horizon_sum_by_rgid_col]
    df[f'hhpop_{targets_end_year}'] = (df[hhpop_init_horizon_col] * df['hhpop_factor']).round(0).astype(int)
    return df



def calc_gq_tot_pop(pipeline, df, dec):
    """Calculate group-quarters and total population for the target horizon year.

    Computes group-quarters population using the regional GQ share, then
    adds it to the factored household population to derive total population.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.
        df (pandas.DataFrame): Per-target-area DataFrame with factored
            household population already computed.
        dec (pandas.DataFrame): Decennial GQ data keyed by ``target_id``.

    Returns:
        pandas.DataFrame: The input DataFrame with added group-quarters and
            total-population columns for the horizon year.
    """
    p = pipeline
    # load target horizon year and set column names for target horizon year
    targets_end_year = p.settings['targets_end_year']
    hhpop_factored_horizon_col = f'hhpop_{targets_end_year}'

    # calculate GQ for target areas
    df = calc_gq(p, df, dec, targets_end_year)

    # add GQ to household population to get total population for horizon year
    total_pop_horizon_col = f'total_pop_{targets_end_year}'
    gq_horizon_col = f'gq_{targets_end_year}'
    df[total_pop_horizon_col] = df[hhpop_factored_horizon_col] + df[gq_horizon_col]
    return df


def calculate_targets(pipeline):
    """Run the full King County targets calculation workflow.

    Loads input tables filtered to King County, computes RGID-level and
    target-area-level housing and population targets, and persists the
    result to the pipeline.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.
    """
    p = pipeline
    # load target horizon year
    targets_end_year = p.settings['targets_end_year']
    # load input tables
    df, dec = load_input_tables(p, 'units')
    # filter to king county only
    df = df.loc[df['county_id'] == 53033].copy()
    # perform calculations
    targets_rgid = calc_by_rgid(p, df)
    df = calc_by_target_area(p, df, targets_rgid)
    df = calc_gq_tot_pop(p, df, dec)
    # save table
    p.save_table('adjusted_king_targets',df)


def run_step(context):
    """Execute the King County targets pipeline step.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print('Calculating targets for King County...')
    calculate_targets(p)
    return context
