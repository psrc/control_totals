import pandas as pd
from util import Pipeline, load_base_year_emp


def load_targets(pipeline):
    """Load adjusted employment change targets with base-year employment data.

    Merges the adjusted employment change targets table with base-year
    employment totals that exclude resource and construction sectors.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.

    Returns:
        pandas.DataFrame: Merged DataFrame containing target IDs, adjusted
            employment changes, and base-year employment figures.
    """
    p = pipeline
    emp = load_base_year_emp(p,'excludes res con')
    df = (
        p.get_table('adjusted_emp_change_targets')
        .merge(emp, on='target_id', how='inner')
    )
    return df

def calc_targets(pipeline):
    """Calculate employment targets excluding resource and construction.

    Computes the resource-and-construction employment share for each target
    area, allocates a county-level resource-and-construction growth amount
    back to target areas, and adds it to the base adjusted employment change
    to produce final horizon-year employment totals.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.

    Returns:
        pandas.DataFrame: DataFrame with adjusted employment targets
            including a horizon-year employment column.
    """
    p = pipeline
    df = load_targets(p)

    base_year = p.settings['base_year']
    end_year = p.settings['targets_end_year']

    # calculate resource and construction employment percentage in base year
    emp_no_mil_base_year_col = f'Emp_TotNoMil_{base_year}'
    res_con_col = f'Emp_ConRes_{base_year}'
    df['res_con_pct'] = (df[res_con_col] / df[emp_no_mil_base_year_col]).replace([float('inf'), -float('inf')], 0).fillna(0)

    # multiply the base year resource and construction employment percentage by the employment change target
    df['res_con_target_pct'] = df['res_con_pct'] * df['emp_chg']

    # sum targets to county level
    df['emp_chg_cnty_sum'] = df.groupby('county_id')['emp_chg'].transform('sum')
    # get resource and construction employment growth percentage from ref projection
    res_con_emp_growth_pct = p.settings['res_con_emp_growth_pct']
    # calculate resource and construction employment change target for county
    df['res_con_emp_chg_cnty_sum'] = df['emp_chg_cnty_sum'] * res_con_emp_growth_pct
    # allocate county resource and construction employment change target to targets based on their resource and construction target percentage
    df['res_con_emp_chg_target'] = df['res_con_target_pct'] * df['res_con_emp_chg_cnty_sum'] / df.groupby('county_id')['res_con_target_pct'].transform('sum')

    # adjust employment change target by adding resource and construction employment change target
    df['emp_chg_adj_res_con'] = df['emp_chg_adj'] + df['res_con_emp_chg_target']

    # add with base year employment to get adjusted employment total
    emp_no_mil_end_year_col = f'emp_{end_year}'
    df[emp_no_mil_end_year_col] = (df[emp_no_mil_base_year_col] + df['emp_chg_adj_res_con']).fillna(0).round(0).astype(int)

    return df

def run_step(context):
    """Execute the employment-change (no resource/construction) pipeline step.

    Calculates employment targets for counties that exclude resource and
    construction employment, then persists the results to the pipeline.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print('Calculating targets for counties that exclude resource and construction employment...')
    df = calc_targets(p)
    p.save_table('adjusted_emp_change_targets_no_res_con', df)
    return context