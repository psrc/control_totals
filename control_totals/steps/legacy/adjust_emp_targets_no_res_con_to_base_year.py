import pandas as pd
import numpy as np

from control_totals.steps.legacy.adjust_targets_to_decennial import combine_targets
from control_totals.util.pipeline import Pipeline


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

    # get control area to target lookup
    xwalk = p.get_table('control_target_xwalk')
    
    # sum estimates by target areas
    df = (
        p.get_table(f'{table}_{year}_by_control_area')
        # join to target ids
        .merge(xwalk[['control_id','county_id', 'target_id']], on='control_id', how='left')
        .drop(columns='control_name', errors='ignore')
        # groupby sum to target id
        .groupby(['target_id','county_id']).sum()
        .drop(columns='control_id', errors='ignore')
        .add_suffix(f'_{year}')
        .reset_index()
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
    years = list(set([base_year] + start_years))

    # remove 2020 from years
    if target_type in ['total_pop','units']:
        years.remove(2020) if 2020 in years else None

    for start_year in years:
        print(f"Summing {table} estimates for year {start_year} to target areas...")
        ofm_df = sum_estimates_to_target_area(p, start_year, target_type, table)

        # merge to all years dataframe
        est_all_years = (
            est_all_years.merge(ofm_df, on=['target_id','county_id'], how='outer')
            if not est_all_years.empty
            else ofm_df
        )
    return est_all_years

def adjust_targets(pipeline):
    p = pipeline
    base_year = p.settings['base_year']

    # combine county targets
    df = combine_targets(pipeline, 'emp', emp_target_type='no_res_con')

    # get unique start years in the targets
    start_years = df['start'].unique().tolist()

    # get estimates for all start years and base year amd merge to targets
    est_all_years = get_estimates_all_years(p, start_years, 'emp', 'employment')
    if not est_all_years.empty:
        df_all = df.merge(est_all_years, on=['target_id'], how='left')

    df_out = pd.DataFrame()
    # iterate over each start year and adjust employment targets relative to the base year
    for start_year in start_years:
        df = df_all[df_all['start'] == start_year].copy()

        # load resource and construction employment targets from settings
        res_con_targets = p.settings['resource_construction_emp_targets']

        # change 2019 to 2020 emp w/ res con, no military
        df['emp_chg_no_military'] = df[f'Emp_TotNoMil_{base_year}'] - df[f'Emp_TotNoMil_{start_year}']
        # percent of employment 2020 that is res con
        df['pct_res_con'] = df[f'Emp_ConRes_{base_year}'] / df[f'Emp_TotNoMil_{base_year}']
        # res con job change based on emp target
        df['res_con_chg'] = df['emp_chg'] * df['pct_res_con']
        # normalize resource and construction employment changes by county using the county targets from settings
        df['res_con_chg_norm'] = df['county_id'].map(res_con_targets) * df['res_con_chg'] / df.groupby('county_id')['res_con_chg'].transform('sum')
        # if the adjusted employment change is negative, fall back to the original employment change
        # otherwise use the adjusted employment change
        emp_chg_adj = df['emp_chg'] - df['emp_chg_no_military'] + df['res_con_chg_norm']
        df['emp_chg_adj'] = np.where(emp_chg_adj < 0, df['emp_chg'], emp_chg_adj)
        df['emp_chg_adj'] = df['emp_chg_adj'].fillna(0)
        # append the adjusted targets for this start year to the output dataframe
        df_out = pd.concat([df_out, df[['target_id','start','emp_chg','emp_chg_adj']]], ignore_index=True)

    # return adjusted targets
    return df_out

def run_step(context):
    p = Pipeline(settings_path=context['configs_dir'])
    print("Adjusting employment targets without resource and construction to base year...")
    df_out = adjust_targets(p)
    p.save_table('adjusted_emp_change_targets_no_res_con',df_out)