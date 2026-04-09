from util import Pipeline,load_base_year_emp


def load_targets(pipeline):
    """Load adjusted employment change targets with base-year employment data.

    Merges the adjusted employment change targets table with base-year
    employment totals that include resource and construction sectors.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.

    Returns:
        pandas.DataFrame: Merged DataFrame containing target IDs, adjusted
            employment changes, and base-year employment figures.
    """
    p = pipeline
    emp = load_base_year_emp(p,'res_con')
    df = (
        p.get_table('adjusted_emp_change_targets')
        .merge(emp, on='target_id', how='inner')
    )
    return df

def calc_targets(pipeline):
    """Calculate employment targets including resource and construction.

    Adds the adjusted employment change directly to the base-year
    employment total (no-military) to produce the horizon-year employment
    figure for each target area.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.

    Returns:
        pandas.DataFrame: DataFrame with a horizon-year employment column
            added.
    """
    p = pipeline
    df = load_targets(p)

    base_year = p.settings['base_year']
    end_year = p.settings['targets_end_year']

    emp_end_year_col = f'emp_{end_year}'
    emp_no_mil_base_year_col = f'Emp_TotNoMil_{base_year}'

    df[emp_end_year_col] = (df[emp_no_mil_base_year_col] + df['emp_chg_adj']).round(0).astype(int)

    return df

def run_step(context):
    """Execute the employment-change (with resource/construction) pipeline step.

    Calculates employment targets for counties that include resource and
    construction employment, then persists the results to the pipeline.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print('Calculating targets for counties that include resource and construction employment...')
    df = calc_targets(p)
    p.save_table('adjusted_emp_change_targets_res_con', df)
    return context