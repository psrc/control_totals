from pathlib import Path
from control_totals.util import Pipeline,get_mysql_engine, get_mysql_config

def subreg_id_counts(df):
	count= len(df.loc[df['subreg_id'] != -1,'subreg_id'].unique())
	return count

def run_step(context):
	pipeline = Pipeline(settings_path=context['configs_dir'])
	cfg = pipeline.settings.get('regional_cts', {})
	if cfg.get('save_to_mysql', False):
		hh = pipeline.get_table('annual_household_control_totals')
		print(f'{subreg_id_counts(hh)} subregions in household control totals')
		emp = pipeline.get_table('annual_employment_control_totals')
		print(f'{subreg_id_counts(emp)} subregions in employment control totals')
		hh_reg = pipeline.get_table('annual_household_control_totals_region')
		emp_reg = pipeline.get_table('annual_employment_control_totals_region')
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
		mysql_tables = cfg.get('mysql_tables', {
			'emp': 'annual_employment_control_totals',
			'hh': 'annual_household_control_totals',
			'hh_reg': 'annual_household_control_totals_region',
			'emp_reg': 'annual_employment_control_totals_region',
		})
		create_emp_totals = cfg.get('create_emp_totals', False)
		if create_emp_totals:
			emp.to_sql(mysql_tables['emp'], mysql_engine, if_exists='replace', index=False)
			print(f'Wrote {len(emp)} rows to {mysql_db}.{mysql_tables["emp"]}')
			emp_reg.to_sql(mysql_tables['emp_reg'], mysql_engine, if_exists='replace', index=False)
			print(f'Wrote {len(emp_reg)} rows to {mysql_db}.{mysql_tables["emp_reg"]}')
		hh.to_sql(mysql_tables['hh'], mysql_engine, if_exists='replace', index=False)
		print(f'Wrote {len(hh)} rows to {mysql_db}.{mysql_tables["hh"]}')
		hh_reg.to_sql(mysql_tables['hh_reg'], mysql_engine, if_exists='replace', index=False)
		print(f'Wrote {len(hh_reg)} rows to {mysql_db}.{mysql_tables["hh_reg"]}')
		
		
