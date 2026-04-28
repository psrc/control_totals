import pandas as pd
from util import Pipeline, load_input_tables, calc_gq


def calc_dec_hhsz(dec):
    """Calculate the regional decennial household size.

    Args:
        dec (pandas.DataFrame): Decennial census data with ``dec_hhpop``
            and ``dec_hh`` columns.

    Returns:
        float: The region-wide average household size.
    """
    return dec['dec_hhpop'].sum() / dec['dec_hh'].sum()

def calc_ref_horizon_hhsz(pipeline):
    """Calculate the REF projection household size at the targets horizon year.

    Reads the Regional Economic Forecast (REF) projection table and divides
    household population by households for the configured targets end year.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.

    Returns:
        float: The projected household size at the targets horizon year.
    """
    p = pipeline
    horizon_year = p.settings['targets_end_year']
    ref = p.get_table('ref_projection')
    hhpop = ref.loc[ref.variable == 'HH Pop', str(horizon_year)].item()
    hh = ref.loc[ref.variable == 'HH', str(horizon_year)].item()
    return hhpop / hh

def calc_horizon_hhsz(df, dec_hhsz, ref_hhsz, hhsz_horizon_col):
    """Calculate the horizon-year household size for each target area.

    Applies the ratio of reference-to-decennial regional household size to
    each target area's decennial household size. Values above 5 or equal
    to 0 are replaced with the reference household size.

    Args:
        df (pandas.DataFrame): Target-area DataFrame containing a
            ``dec_hhsz`` column.
        dec_hhsz (float): The region-wide decennial household size.
        ref_hhsz (float): The REF projection household size at the
            horizon year.
        hhsz_horizon_col (str): The name of the output column to create.

    Returns:
        pandas.DataFrame: The input DataFrame with the new household-size
            column added.
    """
    # Calculate horizon year household size
    df[hhsz_horizon_col] = (ref_hhsz / dec_hhsz * df['dec_hhsz']).fillna(0)
    # if hhsz is greater than 5 or equal to 0, set to REF hhsz
    df.loc[(df[hhsz_horizon_col]>5) | (df[hhsz_horizon_col]==0) , hhsz_horizon_col] = ref_hhsz
    return df

def calculate_targets(pipeline):
    """Run the full total-population-change targets calculation workflow.

    Loads input tables filtered to counties using total-population-change
    targets, computes horizon-year total population, group quarters,
    household population, household size, and households, then saves the
    result to the pipeline.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.
    """
    p = pipeline
    # Get target column names
    targets_end_year = p.settings["targets_end_year"]
    hhpop_horizon_col = f'hhpop_{targets_end_year}'
    gq_horizon_col = f'gq_{targets_end_year}'
    hhsz_horizon_col = f'hhsz_{targets_end_year}'
    hh_horizon_col = f'hh_{targets_end_year}'
    # Load input tables
    df, dec = load_input_tables(p, 'total_pop')

    # filter to only counties that are using total population change targets in settings.yaml
    included_counties = p.settings['target_types']['total_pop_chg']
    df = df[df['county_id'].isin(included_counties)].copy()

    # Calculate total population for horizon year
    total_pop_horizon_col = f'total_pop_{targets_end_year}'
    df[total_pop_horizon_col] = df['dec_total_pop'] + df['total_pop_chg_adj']

    # Calculate GQ and household population for horizon year
    df = calc_gq(p, df, dec, targets_end_year)
    df[hhpop_horizon_col] = df[total_pop_horizon_col] - df[gq_horizon_col]
    
    # Calculate household size: pct change from decennial to REF applied to decennial hhsz
    dec_hhsz = calc_dec_hhsz(dec)
    ref_hhsz = calc_ref_horizon_hhsz(p)
    df = calc_horizon_hhsz(df, dec_hhsz, ref_hhsz, hhsz_horizon_col)

    # Calculate households for horizon year
    df[hh_horizon_col] = (df[hhpop_horizon_col] / df[hhsz_horizon_col]).round(0).astype(int)
    
    # Save table
    p.save_table('adjusted_total_pop_change_targets', df)


def run_step(context):
    """Execute the total-population-change targets pipeline step.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print('Calculating targets for counties that use population targets...')
    calculate_targets(p)
    return context