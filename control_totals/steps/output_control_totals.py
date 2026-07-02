import pandas as pd
from pathlib import Path
from control_totals.util import Pipeline, get_mysql_engine, get_mysql_config


def create_alloc_control_totals(subregional_df, regional_df, base_year):
    '''
    Creates allocation control totals by combining subregional and regional control totals.
    Drops base year and non-luvit years from subregional data and combines with regional data.
    '''
    subregional_df = subregional_df.loc[subregional_df.year > base_year].copy()
    non_luvit_years = subregional_df.loc[subregional_df.subreg_id == -1, 'year'].unique().tolist()
    luvit_years = subregional_df.loc[subregional_df.subreg_id != -1, 'year'].unique().tolist()
    
    subregional_luvit = subregional_df.loc[subregional_df.year.isin(luvit_years)].copy()
    regional_non_luvit = regional_df.loc[regional_df.year.isin(non_luvit_years)].copy()
    regional_non_luvit['subreg_id'] = -1
    
    return pd.concat([subregional_luvit, regional_non_luvit])


def _save_to_csv(data_by_type, table_names, out_dir):
    for name, df in zip(table_names, data_by_type):
        path = Path(out_dir) / f'{name}.csv'
        df.to_csv(path, index=False)
        print(f'Saved {len(df)} rows to {path}')


def _save_to_mysql(data_by_type, table_names, mysql_engine, mysql_db):
    for name, df in zip(table_names, data_by_type):
        df.to_sql(name, mysql_engine, if_exists='replace', index=False)
        print(f'Wrote {len(df)} rows to {mysql_db}.{name}')


def run_step(context):
    pipeline = Pipeline(settings_path=context['configs_dir'])
    cfg = pipeline.settings.get('output_control_totals', {})
    base_year = pipeline.settings.get('base_year')

    # Create allocation CTs for hh and emp
    data_types = ['hh', 'emp']
    alloc_out = {}
    for dtype in data_types:
        subregional = pipeline.get_table(f'subregionalCTs_{dtype}')
        regional = pipeline.get_table(f'regionalCTs_{dtype}')
        alloc_out[dtype] = create_alloc_control_totals(subregional, regional, base_year)
        print(f"Allocation control totals ({dtype}) row count: {len(alloc_out[dtype])}")

    # Save to pipeline.h5
    table_names = {
        'hh': cfg.get('allocation_control_totals_hh', 'annual_household_control_totals'),
        'emp': cfg.get('allocation_control_totals_emp', 'annual_employment_control_totals'),
    }
    reg_table_names = {
        'hh': cfg.get('simulation_control_totals_hh', 'annual_household_control_totals_region'),
        'emp': cfg.get('simulation_control_totals_emp', 'annual_employment_control_totals_region'),
    }
    
    for dtype in data_types:
        pipeline.save_table(table_names[dtype], alloc_out[dtype])
        print(f'Saved {len(alloc_out[dtype])} rows to {table_names[dtype]} in pipeline.h5')
        pipeline.save_table(reg_table_names[dtype], pipeline.get_table(f'regionalCTs_{dtype}'))
        print(f'Saved {len(pipeline.get_table(f"regionalCTs_{dtype}"))} rows to {reg_table_names[dtype]} in pipeline.h5')

    # Save to CSV
    if cfg.get('save_to_csv', False):
        out_dir = pipeline.get_output_dir()
        all_data = [alloc_out['hh'], alloc_out['emp'], 
                    pipeline.get_table('regionalCTs_hh'), pipeline.get_table('regionalCTs_emp')]
        all_tables = [table_names['hh'], table_names['emp'], reg_table_names['hh'], reg_table_names['emp']]
        _save_to_csv(all_data, all_tables, out_dir)

    # Save to MySQL
    if cfg.get('save_to_mysql', False):
        mysql_db = cfg.get('mysql_db')
        if not mysql_db:
            raise ValueError('regional_cts.mysql_db must be set when save_to_mysql is true')
        mysql_creds = get_mysql_config(pipeline)
        mysql_engine = get_mysql_engine(
            mysql_db,
            creds_path=mysql_creds['creds_path'],
            user_env=mysql_creds['user_env'],
            password_env=mysql_creds['password_env'],
            host_env=mysql_creds['host_env'],
        )
        all_data = [alloc_out['emp'], alloc_out['hh'],
                    pipeline.get_table('regionalCTs_emp'), pipeline.get_table('regionalCTs_hh')]
        all_tables = [table_names['emp'], table_names['hh'], 
                       reg_table_names['emp'], reg_table_names['hh']]
        _save_to_mysql(all_data, all_tables, mysql_engine, mysql_db)
		
		
