import pandas as pd
from pathlib import Path
from control_totals.util import Pipeline


def load_tables(pipeline):
    """Load and merge base-year and target data for control total creation.

    Reads extrapolated targets, the control-area / target crosswalk,
    base-year OFM population estimates, and base-year employment
    estimates, then joins them into a single DataFrame.

    Args:
        pipeline (Pipeline): The data pipeline providing access to
            stored tables and settings.

    Returns:
        pandas.DataFrame: Merged DataFrame with base-year actuals and
            horizon-year targets keyed by control area.
    """
    p = pipeline
    base_year = p.settings['base_year']
    target_year = p.settings['targets_end_year']
    control_year = p.settings['end_year']
    target_cols = [
        'target_id',
        f'hh_{target_year}',
        f'total_pop_{target_year}',
        f'gq_{target_year}',
        f'hhpop_{target_year}',
        f'emp_{target_year}',
        
        f'hh_{control_year}',
        f'total_pop_{control_year}',
        f'gq_{control_year}',
        f'hhpop_{control_year}',
        f'emp_{control_year}'
    ]
    # load extrapolated targets
    targets = p.get_table('extrapolated_targets')[target_cols].set_index('target_id').astype(float).reset_index()
    # load control area to target xwalk
    xwalk = p.get_table('control_target_xwalk')
    # load base year dec data
    dec = p.get_table(f'decennial_by_control_area')
    # load base year employment data
    base_emp = p.get_table(f'employment_{base_year}_by_control_area')
    # merge all tables together
    df = (
        xwalk
        .merge(dec, on='control_id', how='left').drop(columns='control_name', errors='ignore')
        .merge(base_emp, on='control_id', how='left').drop(columns='control_name', errors='ignore')
        .merge(targets, on='target_id', how='left')
    )

    return df

def recalc_excluded_control_areas(pipeline, df):
    """Reset horizon-year values for excluded control areas to base-year actuals.

    For control areas flagged with ``exclude_from_target == 1`` (e.g.
    military bases), overwrites population, household, group-quarters,
    and employment targets with their base-year equivalents.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.
        df (pandas.DataFrame): Control totals DataFrame with both
            base-year and horizon-year columns.

    Returns:
        pandas.DataFrame: Updated DataFrame with excluded areas reset.
    """
    p = pipeline
    targets_end_year = p.settings['targets_end_year']
    controls_end_year = p.settings['end_year']
    
    # flag from xwalk for control areas to exclude from target totals
    mask = df['exclude_from_target'] == 1
    # left is horizon year column name, right is base year column name
    updates = {
    'total_pop': 'dec_total_pop',
    'hhpop': 'dec_hhpop',
    'gq': 'dec_gq',
    'hh': 'dec_hh',
    'emp': f'Emp_TotNoMil',
    }
    # for each horizon year and for each column above, set the value to equal base year value
    # if the control area is flagged for exclusion.
    for year in [targets_end_year, controls_end_year]:
        for prefix, src in updates.items():
            df.loc[mask, f'{prefix}_{year}'] = df.loc[mask, src]

    # subtract excluded-area values from sibling controls that share a target_id
    excluded_controls = df.loc[mask, 'control_id'].values
    excluded_targets = df.loc[df['control_id'].isin(excluded_controls), 'target_id'].values

    prefixes = ['hh', 'total_pop', 'gq', 'hhpop', 'emp']
    subtract_cols = [f'{p}_{y}' for y in [targets_end_year, controls_end_year] for p in prefixes]

    excluded = df['control_id'].isin(excluded_controls)
    in_target = df['target_id'].isin(excluded_targets)

    no_growth_df = (
        df.loc[excluded, ['target_id'] + subtract_cols]
        .groupby('target_id').sum()
    )
    subtracted_df = (
        df.loc[in_target & ~excluded, ['control_id'] + subtract_cols]
        .set_index('control_id')
        .subtract(no_growth_df)
        .dropna()
    )

    df = df.set_index('control_id')
    df.loc[subtracted_df.index, subtract_cols] = subtracted_df
    df = df.reset_index()
    return df

def save_r_scrpt_inputs(pipeline, control_totals_df):
    """Prepare and export an Excel workbook consumed by downstream R scripts.

    Renames columns to the legacy naming convention expected by the R
    scripts, merges in 2018 base-year data, computes derived fields
    (e.g. employment targets, population targets, GQ percentages, PPH),
    and writes the result to ``control_id_working.xlsx``.

    Args:
        pipeline (Pipeline): The data pipeline providing access to
            stored tables and the data directory.
        control_totals_df (pandas.DataFrame): Control totals DataFrame
            produced by earlier steps.

    Returns:
        pandas.DataFrame: The final DataFrame written to Excel.
    """
    p = pipeline
    
    # rename columns for r script inputs, this will need to be updated to dynamically change based
    # on base year in settings, not sure why the targets were'nt just adjusted to 2018 to begin with.
    rename_cols_2018 = {
        'ofm_total_pop': 'Pop18',
        'ofm_hhpop': 'HHpop18',
        'ofm_hh': 'HH18',
        'ofm_gq': 'GQ18',
        'ofm_units': 'Units18',
    }
    ofm_2018 = p.get_table('ofm_parcelized_2018_by_control_area').rename(columns=rename_cols_2018)
    emp_2018 = p.get_table('employment_2018_by_control_area').rename(columns={'Emp_TotNoMil': 'Emp18'})
    
    rename_cols_2020 = {
        'rgid': 'RGID',
        'target_name': 'name',
        'dec_total_pop': 'TotPop20',
        'dec_hhpop': 'HHpop20',
        'dec_hh': 'HH20',
        'dec_gq': 'GQ20',
        'dec_units': 'Units20',
        'Emp_TotNoMil': 'TotEmp20_wCRnoMil',
        'total_pop_2044': 'TotPop44',
        'total_pop_2050': 'TotPop50',
        'hhpop_2044': 'HHpop44',
        'hhpop_2050': 'HHpop50',
        'hh_2044': 'HH44',
        'hh_2050': 'HH50',
        'gq_2044': 'GQ44',
        'gq_2050': 'GQ50',
        'emp_2044': 'TotEmp44_wCRnoMil',
        'emp_2050': 'TotEmp50_wCRnoMil',
    }
    df = (
        control_totals_df
        .merge(ofm_2018, on='control_id', how='left')
        .merge(emp_2018, on='control_id', how='left')
        .rename(columns=rename_cols_2020)
    )

    # calculate additional columns needed for r script
    df['TotEmpTrg_wCRnoMil'] = df['TotEmp50_wCRnoMil'] - df['TotEmp20_wCRnoMil']
    df['TotPopTrg'] = df['TotPop50'] - df['TotPop20']
    df['GQpct50'] = (df['GQ50'] / df['TotPop50']).fillna(0).replace([float('inf'), -float('inf')], 0)
    df['PPH50'] = (df['HHpop50'] / df['HH50']).fillna(0).replace([float('inf'), -float('inf')], 0)
    # take last 2 digits of county id
    df['county_id'] = df['county_id'].astype(str).str[-2:].astype(int)
    # export final table to excel for r script input
    df.to_excel(Path(p.get_data_dir()) / 'control_id_working.xlsx', index=False)

    return df


def run_step(context):
    """Execute the control-totals creation pipeline step.

    Loads base-year and target tables, resets excluded control areas,
    exports R-script inputs, and saves the final control totals to
    the pipeline.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    p = Pipeline(settings_path=context['configs_dir'])
    df = load_tables(p)
    df = recalc_excluded_control_areas(p, df)
    df = save_r_scrpt_inputs(p, df)
    p.save_table('control_totals', df)
    return context