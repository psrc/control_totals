import pandas as pd
from util import Pipeline


def combine_targets(pipeline, target_type):
    """Combine growth-change targets from all county target tables for a given type.

    Reads each target table listed in the pipeline settings that contains a
    column for the specified target type, extracts the change value and start
    year, and concatenates them into a single DataFrame.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.
        target_type (str): The type of target to combine. One of
            ``'total_pop'``, ``'units'``, or ``'emp'``.

    Returns:
        pandas.DataFrame: A DataFrame with columns ``['target_id',
            '<target_type>_chg', 'start']``.
    """
    df = pd.DataFrame()
    for table in pipeline.settings['targets_tables']:
        if f'{target_type}_chg_col' in table:
            df_table = pipeline.get_table(table['name'])

            # add start year column
            df_table['start'] = table[f'{target_type}_chg_start']

            df = pd.concat([df, df_table], ignore_index=True)
    return df[['target_id', f'{target_type}_chg', 'start']]


def sum_estimates_to_target_area(pipeline, year, target_type, table):
    """Sum base-year estimates to target areas for a given year and type.

    Loads estimates from the pipeline, joins them to target IDs via the
    control-target crosswalk, and aggregates to the target-area level.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.
        year (int): The estimate vintage year to load.
        target_type (str): The type of estimate. One of ``'total_pop'``,
            ``'units'``, or ``'emp'``.
        table (str): The base table name prefix, e.g. ``'ofm_parcelized'``
            or ``'employment'``.

    Returns:
        pandas.DataFrame: A DataFrame with columns ``['target_id',
            '<target_type>_<year>']`` containing the summed estimates.
    """

    p = pipeline
    
    if target_type == 'emp':
        # get column name for employment excluding military, resource and construction
        emp_col = 'TotEmpNoMil-ResCon'
        col_name = emp_col
    else:
        col_name = f'ofm_{target_type}'

    # get control area to target lookup
    xwalk = p.get_table('control_target_xwalk')
    
    # sum estimates by target areas
    df = (
        p.get_table(f'{table}_{year}_by_control_area')
        # add year suffix to ofm column
        .rename(columns={f'{col_name}':f'{target_type}_{year}'})
        # join to target ids
        .merge(xwalk[['control_id', 'target_id']], on='control_id', how='left')
        # groupby sum to target id
        .groupby('target_id').sum().reset_index()
        # return only target id and needed ofm column
        [['target_id', f'{target_type}_{year}']]
    )
    return df

def get_estimates_all_years(pipeline, start_years, target_type, table):
    """Collect estimates for the base year and all target start years.

    Iterates over the unique start years (plus the global base year) and
    merges each year's summed estimates into a single wide DataFrame.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.
        start_years (list[int]): Start years found in the combined targets.
        target_type (str): The type of estimate. One of ``'total_pop'``,
            ``'units'``, or ``'emp'``.
        table (str): The base table name prefix, e.g. ``'ofm_parcelized'``
            or ``'employment'``.

    Returns:
        pandas.DataFrame: A wide DataFrame keyed by ``target_id`` with one
            column per year containing the summed estimates.
    """
    p = pipeline
    base_year = p.settings['base_year']

    # create empty dataframe to hold all years of needed ofm columns
    est_all_years = pd.DataFrame()
    
    # loop through baseyear and start years and sum ofm to target area

    for start_year in list(set([base_year] + start_years)):
        ofm_df = sum_estimates_to_target_area(p, start_year, target_type, table)

        # merge to all years dataframe
        est_all_years = (
            est_all_years.merge(ofm_df, on='target_id', how='outer')
            if not est_all_years.empty
            else ofm_df
        )
    return est_all_years

def adjust_targets(pipeline, target_type, table):
    """Adjust growth-change targets to the base year.

    Computes the change between each target's original start year and the
    global base year using OFM or employment estimates, then subtracts
    that change from the raw target to produce an adjusted target. Saves
    the result to the pipeline HDF5 store.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.
        target_type (str): The type of target to adjust. One of
            ``'total_pop'``, ``'units'``, or ``'emp'``.
        table (str): The base table name prefix used to look up estimates,
            e.g. ``'ofm_parcelized'`` or ``'employment'``.
    """

    p = pipeline
    base_year = p.settings['base_year']

    # combine county targets
    df = combine_targets(p, target_type)

    # get unique start years in the targets
    start_years = df['start'].unique().tolist()

    # get estimates for all start years and base year amd merge to targets
    est_all_years = get_estimates_all_years(p, start_years, target_type, table)
    df = df.merge(est_all_years, on='target_id', how='left')

    # loop through each row to calculate change from target start year to base year
    for index, row in df.iterrows():
        start = int(row['start'])
        start_col = f'{target_type}_{start}'
        base_col = f'{target_type}_{base_year}'
        est_chg_col = f'est_{target_type}_chg'
        df.at[index, est_chg_col] = row[base_col] - row[start_col]

    chg_adj_col = f'{target_type}_chg_adj'
    chg_col = f'{target_type}_chg'
    # fill NA, round and clip to 0 (no negative change)
    df[est_chg_col] = df[est_chg_col].fillna(0).round(0).clip(lower=0).astype(int)
    # adjust target change by subtracting est change, minimum of 0
    df[chg_adj_col] = (df[chg_col] - df[est_chg_col]).clip(lower=0)

    # save adjusted targets table
    table_name = f'adjusted_{target_type}_change_targets'
    out_df = df[['target_id','start',chg_col,chg_adj_col]]
    p.save_table(table_name,out_df)


def run_step(context):
    """Execute the adjust-targets-to-base-year pipeline step.

    Adjusts housing-unit, total-population, and employment growth-change
    targets so that growth is measured relative to the configured base year.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key with the path to the configuration
            directory.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    p = Pipeline(settings_path=context['configs_dir'])
    print("Adjusting targets to base year using OFM and Employment estimates...")
    adjust_targets(p,'units','ofm_parcelized')
    adjust_targets(p,'total_pop','ofm_parcelized')
    adjust_targets(p,'emp','employment')
    return context