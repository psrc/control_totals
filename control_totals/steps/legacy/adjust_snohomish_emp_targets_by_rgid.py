import pandas as pd

from control_totals.util import Pipeline


def adjust_emp_targets(pipeline: Pipeline) -> pd.DataFrame:
    p = pipeline
    target_year = p.settings['targets_end_year']
    
    # load targets and join to rgids
    df = p.get_table('adjusted_emp_change_targets_calculations')
    xwalk = p.get_table('target_rgid_xwalk')[['target_id','rgid']]
    df = df.merge(xwalk, on='target_id', how='left')
    # split snohomish to seperate df
    snohomish_df = df[df['county_id'] == 53061].copy()
    remainder_df = df[df['county_id'] != 53061].copy()
    # get hard coded Snohomish County employment totals by rgid from settings.yaml
    snohomish_emp_totals = pd.Series(p.settings[f'snohomish_emp_target_totals'])
    # calculate preliminary employment by rgid for Snohomish County
    prelim_emp_by_rgid = snohomish_df.groupby('rgid')[f'emp_{target_year}'].sum()
    # calculate adjustment ratio
    adj_ratio = snohomish_emp_totals / prelim_emp_by_rgid
    # apply adjustment ratio to snohomish_df
    snohomish_df[f'emp_{target_year}'] = (
        round(snohomish_df['rgid'].map(adj_ratio) * snohomish_df[f'emp_{target_year}'])
    )
    # combine snohomish_df and remainder_df back into a single dataframe
    df = pd.concat([snohomish_df, remainder_df], ignore_index=True)
    return df


def run_step(context):
    p = Pipeline(settings_path=context['configs_dir'])
    df = adjust_emp_targets(p)
    p.save_table('adjusted_emp_change_targets_calculations', df)
    return context