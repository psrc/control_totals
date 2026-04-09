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
    base_year = p.settings['base_year']
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

    # apply any hard-coded employment target overrides from settings
    emp_overrides = p.settings.get('emp_target_overrides', {})
    for control_id, emp_target in emp_overrides.items():
        # add override target to base year emp
        df.loc[df['control_id'] == control_id, f'emp_{targets_end_year}'] = emp_target + df.loc[df['control_id'] == control_id, f'Emp_TotNoMil']
        # extrapolate override taret out to controls end year
        extrap_denominator = (targets_end_year - base_year) * (controls_end_year - targets_end_year)
        df.loc[df['control_id'] == control_id, f'emp_{controls_end_year}'] = emp_target / extrap_denominator + df.loc[df['control_id'] == control_id, f'emp_{targets_end_year}']

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
    base_year = p.settings.get('base_year')
    ref_base_year = p.settings.get('ref_base_year')
    targets_end_year = p.settings.get('targets_end_year')
    controls_end_year = p.settings.get('end_year')

    base_year_2_digits = str(base_year)[-2:]
    ref_base_year_2_digits = str(ref_base_year)[-2:]
    targets_end_year_2_digits = str(targets_end_year)[-2:]
    controls_end_year_2_digits = str(controls_end_year)[-2:]

    # rename columns for r script inputs, this will need to be updated to dynamically change based
    # on base year in settings, not sure why the targets were'nt just adjusted to 2018 to begin with.

    rename_cols_ref = {
        'ofm_total_pop': f'Pop{ref_base_year_2_digits}',
        'ofm_hhpop': f'HHpop{ref_base_year_2_digits}',
        'ofm_hh': f'HH{ref_base_year_2_digits}',
        'ofm_gq': f'GQ{ref_base_year_2_digits}',
        'ofm_units': f'Units{ref_base_year_2_digits}',
    }

    ofm_ref_base_year = p.get_table(f'ofm_parcelized_{ref_base_year}_by_control_area').rename(columns=rename_cols_ref)
    emp_ref_base_year = p.get_table(f'employment_{ref_base_year}_by_control_area').rename(columns={'Emp_TotNoMil': f'Emp{ref_base_year_2_digits}'})
    
    rename_cols_base_year = {
        'rgid': 'RGID',
        'target_name': 'name',
        'dec_total_pop': f'TotPop{base_year_2_digits}',
        'dec_hhpop': f'HHpop{base_year_2_digits}',
        'dec_hh': f'HH{base_year_2_digits}',
        'dec_gq': f'GQ{base_year_2_digits}',
        'dec_units': f'Units{base_year_2_digits}',
        'Emp_TotNoMil': f'TotEmp{base_year_2_digits}_wCRnoMil',
        f'total_pop_{targets_end_year}': f'TotPop{targets_end_year_2_digits}',
        f'total_pop_{controls_end_year}': f'TotPop{controls_end_year_2_digits}',
        f'hhpop_{targets_end_year}': f'HHpop{targets_end_year_2_digits}',
        f'hhpop_{controls_end_year}': f'HHpop{controls_end_year_2_digits}',
        f'hh_{targets_end_year}': f'HH{targets_end_year_2_digits}',
        f'hh_{controls_end_year}': f'HH{controls_end_year_2_digits}',
        f'gq_{targets_end_year}': f'GQ{targets_end_year_2_digits}',
        f'gq_{controls_end_year}': f'GQ{controls_end_year_2_digits}',
        f'emp_{targets_end_year}': f'TotEmp{targets_end_year_2_digits}_wCRnoMil',
        f'emp_{controls_end_year}': f'TotEmp{controls_end_year_2_digits}_wCRnoMil',
    }
    df = (
        control_totals_df
        .merge(ofm_ref_base_year, on='control_id', how='left')
        .merge(emp_ref_base_year, on='control_id', how='left')
        .rename(columns=rename_cols_base_year)
    )

    # calculate additional columns needed for r script
    df[f'TotEmpTrg_wCRnoMil'] = df[f'TotEmp{targets_end_year_2_digits}_wCRnoMil'] - df[f'TotEmp{base_year_2_digits}_wCRnoMil']
    df[f'TotPopTrg'] = df[f'TotPop{targets_end_year_2_digits}'] - df[f'TotPop{base_year_2_digits}']
    df[f'GQpct{controls_end_year_2_digits}'] = (df[f'GQ{controls_end_year_2_digits}'] / df[f'TotPop{controls_end_year_2_digits}']).fillna(0).replace([float('inf'), -float('inf')], 0)
    df[f'PPH{controls_end_year_2_digits}'] = (df[f'HHpop{controls_end_year_2_digits}'] / df[f'HH{controls_end_year_2_digits}']).fillna(0).replace([float('inf'), -float('inf')], 0)
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