import pandas as pd
from util import Pipeline
from iteround import saferound

def get_start_year(pipeline):
    """Return the configured start year for the Kitsap targets table.

    Searches the ``targets_tables`` settings list for the entry named
    ``'kitsap_targets'`` and returns its ``total_pop_chg_start`` value.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.

    Returns:
        int: The start year for total population change targets.
    """
    for table in pipeline.settings['targets_tables']:
        if table['name'] == 'kitsap_targets':
            return table['total_pop_chg_start']

def load_tables(pipeline, start_year):
    """Load Kitsap targets and merge base-year OFM controls aggregated to target area.

    Reads the Kitsap targets table, the control-area / target crosswalk,
    and base-year OFM parcel data, then merges them into a single DataFrame
    with OFM estimates summed to ``target_id``.

    Args:
        pipeline (Pipeline): The data pipeline providing access to stored
            tables.
        start_year (int): The base year used to select the OFM parcelized
            table.

    Returns:
        pandas.DataFrame: Kitsap targets merged with aggregated OFM data.
    """
    p = pipeline
    df = p.get_table('kitsap_targets')
    xwalk = p.get_table('control_target_xwalk')[['control_id','target_id']]
    ofm = (
        p.get_table(f'ofm_parcelized_{start_year}_by_control_area')
        .merge(xwalk, on='control_id', how='left').drop(columns=['control_id'])
        .groupby('target_id').sum().reset_index()
    )
    df = df.merge(ofm, on='target_id', how='left')
    return df

def normalize(df, target_total, value_col, new_col):
    """Scale values in a column so their sum matches a target total.

    Applies a proportional adjustment factor to every row in *value_col*
    and stores the result in *new_col*.

    Args:
        df (pandas.DataFrame): The DataFrame to modify.
        target_total (float): The desired sum that *new_col* should total.
        value_col (str): Name of the source column whose values are scaled.
        new_col (str): Name of the new column to store the scaled values.

    Returns:
        pandas.DataFrame: The input DataFrame with *new_col* added.
    """
    df[new_col] = df[value_col] * (target_total / df[value_col].sum())
    return df

def get_target_cols(targets_year):
    """Return a mapping of logical column names to target-year column names.

    Builds a dictionary that maps short descriptive keys (e.g.
    ``'units_target_col'``) to the year-specific column names used in the
    targets DataFrame.

    Args:
        targets_year (int): The horizon year used to construct column names.

    Returns:
        dict[str, str]: Mapping of logical keys to column name strings.
    """
    return {
        'units_target_col': f'units_{targets_year}',
        'hh_target_col': f'hh_{targets_year}',
        'hhpop_target_col': f'hhpop_{targets_year}',
        'total_pop_target_col': f'total_pop_{targets_year}',
        'emp_target_col': f'emp_{targets_year}',
    }

def split_incorp_unincorp(df):
    """Split input targets into incorporated and unincorporated subsets.

    Partitions rows based on the ``HousingJuris`` column.

    Args:
        df (pandas.DataFrame): Kitsap targets DataFrame with a
            ``'HousingJuris'`` column.

    Returns:
        tuple[pandas.DataFrame, pandas.DataFrame]: A two-element tuple of
            ``(incorporated, unincorporated)`` DataFrames.
    """
    df_incorp = df.loc[df['HousingJuris'] != 'Unincorporated'].copy()
    df_unincorp = df.loc[df['HousingJuris'] == 'Unincorporated'].copy()
    return df_incorp, df_unincorp

def compute_unincorp_hh_targets(df_unincorp, cols, unincorp_units_target):
    """Compute and normalize unincorporated household targets from population targets.

    Derives household-population targets from total-population targets
    using group-quarters shares, estimates household size ratios from
    base-year data, and normalises the preliminary household count to
    match the expected total.

    Args:
        df_unincorp (pandas.DataFrame): Unincorporated area rows with
            OFM base-year columns and population targets.
        cols (dict[str, str]): Column-name mapping returned by
            :func:`get_target_cols`.
        unincorp_units_target (float): Total unit target for
            unincorporated areas.

    Returns:
        pandas.DataFrame: Updated DataFrame with a normalised household
            target column.
    """
    hhpop_target_col = cols['hhpop_target_col']
    total_pop_target_col = cols['total_pop_target_col']

    df_unincorp['ofm_gq_pct'] = df_unincorp['ofm_gq'] / df_unincorp['ofm_total_pop']
    df_unincorp[hhpop_target_col] = df_unincorp[total_pop_target_col] * (1 - df_unincorp['ofm_gq_pct'])

    start_uninc_hhsz = df_unincorp['ofm_hhpop'].sum() / df_unincorp['ofm_hh'].sum()
    start_uninc_vacancy = 1 - df_unincorp['ofm_hh'].sum() / df_unincorp['ofm_units'].sum()
    unincorp_hh_target = unincorp_units_target * (1 - start_uninc_vacancy)
    target_uninc_hhsz = df_unincorp[hhpop_target_col].sum() / unincorp_hh_target
    target_to_start_hhsz_ratio = target_uninc_hhsz / start_uninc_hhsz

    df_unincorp['ofm_hhsz'] = df_unincorp['ofm_hhpop'] / df_unincorp['ofm_hh']
    df_unincorp['hhsz_target'] = df_unincorp['ofm_hhsz'] * target_to_start_hhsz_ratio
    df_unincorp['prelim_hh_target'] = df_unincorp[hhpop_target_col] / df_unincorp['hhsz_target']
    return normalize(df_unincorp, unincorp_hh_target, 'prelim_hh_target', cols['hh_target_col'])

def get_start_year_units_col(pipeline):
    """Return the start-year units column name from the Kitsap targets config.

    Searches the ``targets_tables`` settings list for the entry named
    ``'kitsap_targets'`` and returns its ``start_year_units_col`` value.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.

    Returns:
        str: Column name for start-year housing units.
    """
    p = pipeline
    for table in p.settings['targets_tables']:
        if table['name'] == 'kitsap_targets':
            return table['start_year_units_col']
        
def calculate_start_year_unincorp_units(pipeline, df):
    """Normalise start-year housing units to match the OFM unit total.

    Reads the start-year units column from settings, sums it, and
    normalises ``ofm_units`` to that total.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.
        df (pandas.DataFrame): Unincorporated area DataFrame with an
            ``'ofm_units'`` column.

    Returns:
        pandas.DataFrame: Updated DataFrame with a
            ``'start_year_units_norm'`` column.
    """
    p = pipeline
    start_year_units_col = get_start_year_units_col(p)
    start_year_units_total = df[start_year_units_col].sum()
    df = normalize(df, start_year_units_total, 'ofm_units', 'start_year_units_norm')
    return df

def allocate_unincorp_units(pipeline,df_unincorp, cols, unincorp_units_target):
    """Allocate and round unincorporated housing-unit targets.

    Computes preliminary unit targets from household targets and vacancy
    rates, normalises to the target total, rounds to integers using
    ``saferound``, and calculates the unit change from start year.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.
        df_unincorp (pandas.DataFrame): Unincorporated area rows with
            household targets and OFM base-year columns.
        cols (dict[str, str]): Column-name mapping returned by
            :func:`get_target_cols`.
        unincorp_units_target (float): Total unit target for
            unincorporated areas.

    Returns:
        pandas.DataFrame: Updated DataFrame with unit targets and
            ``'units_chg'`` column.
    """
    df_unincorp['ofm_vacancy'] = 1 - df_unincorp['ofm_hh'] / df_unincorp['ofm_units']
    df_unincorp['prelim_units_target'] = df_unincorp[cols['hh_target_col']] * (1 - df_unincorp['ofm_vacancy'])
    df_unincorp = normalize(df_unincorp, unincorp_units_target, 'prelim_units_target', 'norm_units_target')
    df_unincorp[cols['units_target_col']] = saferound(df_unincorp['norm_units_target'], 0)
    df_unincorp[cols['units_target_col']] = df_unincorp[cols['units_target_col']].astype(int)
    df_unincorp = calculate_start_year_unincorp_units(pipeline, df_unincorp)
    df_unincorp['units_chg'] = df_unincorp[cols['units_target_col']] - df_unincorp['start_year_units_norm']
    df_unincorp['units_chg'] = saferound(df_unincorp['units_chg'],0)
    df_unincorp['units_chg'] = df_unincorp['units_chg'].astype(int)
    return df_unincorp

def finalize_targets(df_incorp, df_unincorp, cols):
    """Combine incorporated and unincorporated rows into the final output.

    Concatenates both subsets and selects the columns needed for
    downstream steps.

    Args:
        df_incorp (pandas.DataFrame): Incorporated area targets.
        df_unincorp (pandas.DataFrame): Unincorporated area targets with
            rebalanced housing allocations.
        cols (dict[str, str]): Column-name mapping returned by
            :func:`get_target_cols`.

    Returns:
        pandas.DataFrame: Combined DataFrame with the final output columns.
    """
    df_out = pd.concat([df_incorp, df_unincorp], ignore_index=True)
    keep_cols = [
        'target_id', 'name', 'HousingJuris', 'total_pop_chg', 'units_chg', 'emp_chg',
        cols['total_pop_target_col'], cols['units_target_col'], cols['emp_target_col']
    ]
    df_out = df_out[keep_cols]
    return df_out

def split_housing_growth_targets(pipeline,df, targets_year):
    """Split and rebalance unincorporated Kitsap housing targets.

    Partitions the targets into incorporated and unincorporated subsets,
    recomputes household and unit targets for unincorporated areas, and
    recombines them into a single output.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.
        df (pandas.DataFrame): Kitsap targets DataFrame.
        targets_year (int): The horizon year for the targets.

    Returns:
        pandas.DataFrame: Rebalanced Kitsap targets.
    """
    cols = get_target_cols(targets_year)
    df_incorp, df_unincorp = split_incorp_unincorp(df)
    unincorp_units_target = df_unincorp[cols['units_target_col']].sum()

    df_unincorp = compute_unincorp_hh_targets(df_unincorp, cols, unincorp_units_target)
    df_unincorp = allocate_unincorp_units(pipeline,df_unincorp, cols, unincorp_units_target)

    return finalize_targets(df_incorp, df_unincorp, cols)

def run_step(context):
    """Execute the Kitsap unincorporated housing-target split pipeline step.

    Loads Kitsap targets, splits and rebalances unincorporated housing
    allocations, and saves the updated targets back to the pipeline.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    print('Splitting unincorporated Kitsap housing targets...')
    p = Pipeline(settings_path=context['configs_dir'])
    start_year = get_start_year(p)
    targets_year = p.settings['targets_end_year']
    df = load_tables(p, start_year)
    df = split_housing_growth_targets(p, df, targets_year)
    p.save_table('kitsap_targets',df)
    return context