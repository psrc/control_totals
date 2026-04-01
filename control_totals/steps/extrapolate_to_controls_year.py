import pandas as pd
from util import Pipeline, calc_gq

def filter_targets_type(p, df, target_type):
    """Filter a targets DataFrame to counties that use a given target type.

    Args:
        p (Pipeline): The data pipeline providing access to settings.
        df (pandas.DataFrame): The targets DataFrame to filter.
        target_type (str): The target type key in ``settings['target_types']``,
            e.g. ``'total_pop_chg'`` or ``'unit_chg'``.

    Returns:
        pandas.DataFrame: Filtered DataFrame containing only rows whose
            ``county_id`` appears in the target-type county list.
    """
    counties = p.settings['target_types'][target_type]
    return df[df['county_id'].isin(counties)]

def maybe_load_adjusted_targets(p, table_name, target_type):
    """Load an adjusted-targets table if its county list is non-empty and the table exists.

    Returns ``None`` when the target type has no configured counties or when
    the HDF5 store does not contain the requested table.

    Args:
        p (Pipeline): The data pipeline providing access to settings and
            stored tables.
        table_name (str): The HDF5 key of the table to load.
        target_type (str): The target type key used to filter by county.

    Returns:
        pandas.DataFrame or None: The filtered targets DataFrame, or
            ``None`` if the table is unavailable or has no matching counties.
    """
    counties = p.settings['target_types'][target_type]
    if not counties:
        return None

    with pd.HDFStore(p.get_hdf5_path()) as store:
        table_keys = {key.lstrip('/') for key in store.keys()}

    if table_name not in table_keys:
        return None

    df = p.get_table(table_name)
    return filter_targets_type(p, df, target_type)

def load_targets_tables(pipeline):
    """Load and merge all adjusted population, housing, and employment targets.

    Concatenates population-change, unit-change, and King County targets
    (when applicable), then outer-merges the result with the combined
    employment-change targets from both the resource-and-construction and
    no-resource-and-construction steps.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.

    Returns:
        pandas.DataFrame: Merged DataFrame with all adjusted target columns
            keyed by ``target_id``.
    """
    p = pipeline
    target_frames = []

    pop_change_targets = maybe_load_adjusted_targets(
        p, 'adjusted_total_pop_change_targets', 'total_pop_chg'
    )
    if pop_change_targets is not None:
        target_frames.append(pop_change_targets)

    unit_change_targets = maybe_load_adjusted_targets(
        p, 'adjusted_unit_change_targets', 'unit_chg'
    )
    if unit_change_targets is not None:
        target_frames.append(unit_change_targets)

    # if king county method is specified, bring in those targets as well
    if p.settings['target_types']['king_cnty_method']:
        king_targets = p.get_table('adjusted_king_targets')
        target_frames.append(king_targets)

    if target_frames:
        pop_unit_targets = pd.concat(target_frames, ignore_index=True)
        if 'start' in pop_unit_targets.columns:
            pop_unit_targets = pop_unit_targets.drop(columns=['start'])
    else:
        pop_unit_targets = pd.DataFrame()

    emp_change_targets_res_con = p.get_table('adjusted_emp_change_targets_res_con')
    emp_change_targets_no_res_con = p.get_table('adjusted_emp_change_targets_no_res_con')
    emp_targets = pd.concat([emp_change_targets_res_con, emp_change_targets_no_res_con], ignore_index=True).drop(columns=['start'])
    return pop_unit_targets.merge(emp_targets.drop(columns=['county_id','rgid'], errors='ignore'), on='target_id', how='outer')


def extrapolate_target(pipeline, df, col):
    """Extrapolate a single indicator from the targets horizon year to the controls horizon year.

    Calculates the average annual change between the base year and targets
    horizon year and extends the trend to the controls horizon year.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.
        df (pandas.DataFrame): The targets DataFrame to modify in place.
        col (str): The indicator prefix, one of ``'hh'``, ``'total_pop'``,
            or ``'emp'``.

    Returns:
        pandas.DataFrame: The input DataFrame with a new column for the
            controls horizon year.
    """
    # col is either 'hh' or 'total_pop'
    p = pipeline
    base_year = p.settings['base_year']
    targets_end_year = p.settings['targets_end_year']
    controls_end_year = p.settings['end_year']

    # extrapolate to controls horizon year based on the avg annual change from base year to targets horizon year
    years_to_target = targets_end_year - base_year
    years_to_control = controls_end_year - base_year
    annual_change_col = f'{col}_annual_change'
    if col == 'emp':
        base_col = f'Emp_TotNoMil_{base_year}'
    else:
        base_col = f'dec_{col}'
    target_col = f'{col}_{targets_end_year}'
    df[annual_change_col] = (df[target_col] - df[base_col]) / years_to_target
    control_col = f'{col}_{controls_end_year}'
    df[control_col] = df[base_col] + df[annual_change_col] * years_to_control
    df[control_col] = df[control_col].round(0).fillna(0).astype(int)

    return df


def extrapolate_to_controls_year(pipeline):
    """Extrapolate all indicators from the targets end year to the controls end year.

    Loads the merged targets tables, extrapolates households, total
    population, and employment out to the controls horizon year, then
    computes group quarters, household population, and household size for
    that year. Saves the result as ``'extrapolated_targets'``.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.
    """
    p = pipeline
    # get controls horizon year from settings.yaml
    controls_end_year = p.settings['end_year']
    # load targets tables
    df = load_targets_tables(p)
    # extrapolate hh and total_pop to controls horizon year
    df = extrapolate_target(p,df,'hh')
    df = extrapolate_target(p,df,'total_pop')
    df = extrapolate_target(p,df,'emp')
    # calculate gq for controls horizon year
    dec = df[['target_id','dec_gq']]
    df = calc_gq(p,df,dec,controls_end_year)
    # calculate hhpop for controls horizon year
    df[f'hhpop_{controls_end_year}'] = df[f'gq_{controls_end_year}'] + df[f'total_pop_{controls_end_year}']
    # calculate hhsz for controls horizon year
    hhsz_control_col = f'hhsz_{controls_end_year}'
    df[hhsz_control_col] = df[f'hhpop_{controls_end_year}'] / df[f'hh_{controls_end_year}']
    # save table
    p.save_table('extrapolated_targets', df)

def run_step(context):
    """Execute the extrapolation pipeline step.

    Extrapolates targets from the configured targets end year out to the
    control totals end year.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    controls_end_year = p.settings['end_year']
    targets_end_year = p.settings['targets_end_year']
    print(f'Extrapolating from targets end year ({targets_end_year}) to control total end year ({controls_end_year})...')
    extrapolate_to_controls_year(p)
    return context