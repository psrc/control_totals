from pathlib import Path
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from util import Pipeline


PROJECT_ROOT = Path(__file__).resolve().parents[1]
R_SCRIPTS_DIR = PROJECT_ROOT / 'r_scripts'


def _read_mysql_creds(creds_path):
	"""Read MySQL credentials from a plain-text credentials file.

	Expects three non-empty lines: username, password, and host.

	Args:
		creds_path (pathlib.Path): Path to the credentials file.

	Returns:
		tuple[str, str, str]: ``(username, password, host)``.

	Raises:
		ValueError: If fewer than three lines are found.
	"""
	lines = [line.strip() for line in creds_path.read_text().splitlines() if line.strip()]
	if len(lines) < 3:
		raise ValueError('Expected username, password, and host in creds.txt')
	return lines[0], lines[1], lines[2]


def load_base_data_from_mysql(base_db, creds_path):
	"""Load household, person, and job base data from a MySQL database.

	Queries the base-year MySQL database for household/person and job
	counts grouped by subreg and control geography, then merges with
	control and subreg metadata.

	Args:
		base_db (str): Name of the MySQL database (e.g.
			``'2018_parcel_baseyear'``).
		creds_path (pathlib.Path): Path to the credentials file.

	Returns:
		pandas.DataFrame: Base data with columns including
			``split_geo_id``, ``nosplit_geo_id``, ``households``,
			``persons``, and ``jobs``.
	"""
	user, password, host = _read_mysql_creds(creds_path)
	engine = create_engine(
		f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@{host}/{base_db}"
	)

	households_query = f"""
		select t3.control_id as nosplit_geo_id,
			   t3.subreg_id as split_geo_id,
			   count(t1.household_id) as households,
			   sum(persons) as persons
		from households as t1
		join buildings as t2 on t1.building_id = t2.building_id
		join {base_db}.parcels as t3 on t2.parcel_id = t3.parcel_id
		group by t3.subreg_id, t3.control_id
	"""
	jobs_query = f"""
		select t3.control_id as nosplit_geo_id,
			   t3.subreg_id as split_geo_id,
			   count(t1.job_id) as jobs
		from jobs as t1
		join buildings as t2 on t1.building_id = t2.building_id
		join {base_db}.parcels as t3 on t2.parcel_id = t3.parcel_id
		group by t3.subreg_id, t3.control_id
	"""

	hh_base = pd.read_sql_query(households_query, engine)
	job_base = pd.read_sql_query(jobs_query, engine)
	base_data = hh_base.merge(job_base, on=['split_geo_id', 'nosplit_geo_id'], how='outer')

	controls = pd.read_sql_query('select * from controls', engine).drop_duplicates()
	controls = controls.rename(columns={'control_id': 'nosplit_geo_id', 'control_name': 'name'})

	subregs = pd.read_sql_query('select * from subregs', engine)
	subregs = subregs.rename(columns={'subreg_id': 'split_geo_id', 'rgs_id': 'RGid'})
	subregs['nosplit_geo_id'] = np.where(subregs['split_geo_id'] > 1000, subregs['split_geo_id'] - 1000, subregs['split_geo_id'])
	subregs = subregs.drop(columns=['subreg_name'], errors='ignore')

	geos = controls.merge(subregs, on=['nosplit_geo_id', 'county_id'], how='inner')
	base_data = geos.merge(base_data, on=['split_geo_id', 'nosplit_geo_id'], how='outer')
	for column in ['households', 'persons', 'jobs']:
		base_data[column] = base_data[column].fillna(0)
	return base_data


def maybe_save_base_data(base_data, base_data_path):
	"""Save base data to a local file if a supported format is specified.

	If the destination path has an R-data extension, it is written as
	pickle instead.

	Args:
		base_data (pandas.DataFrame): The base data to save.
		base_data_path (pathlib.Path): Destination file path.
	"""
	output_path = base_data_path if base_data_path.suffix.lower() in {'.pkl', '.parquet', '.csv'} else base_data_path.with_suffix('.pkl')
	if output_path.suffix.lower() == '.parquet':
		base_data.to_parquet(output_path, index=False)
	elif output_path.suffix.lower() == '.csv':
		base_data.to_csv(output_path, index=False)
	else:
		base_data.to_pickle(output_path)


def get_base_data_table_name(base_year):
	"""Return the HDF5 table key for cached base data.

	Args:
		base_year (int): The base year.

	Returns:
		str: The table key, e.g. ``'split_hct_base_data_2020'``.
	"""
	return f'split_hct_base_data_{int(base_year)}'


def _resolve_path(base_path, candidate):
	"""Resolve a file path that may be relative to a base directory.

	Args:
		base_path (pathlib.Path): The base directory for relative paths.
		candidate (str or pathlib.Path): The path to resolve.

	Returns:
		pathlib.Path: The resolved absolute path.
	"""
	candidate_path = Path(candidate)
	return candidate_path if candidate_path.is_absolute() else base_path / candidate_path


def run_step(context):
	"""Execute the base-data loading pipeline step for the HCT split.

	Connects to a MySQL parcel base-year database, loads household and job
	counts by subreg/control geography, and saves the result to the
	pipeline HDF5 store. Optionally writes a local file copy.

	Args:
		context (dict): The pypyr context dictionary, expected to contain
			a ``'configs_dir'`` key and optional keys such as
			``split_hct_base_year``, ``split_hct_parcel_base_year``,
			``split_hct_creds_file``, ``split_hct_base_data_file``, and
			``split_hct_save_base_data_file``.

	Returns:
		dict: The unchanged pypyr context dictionary.
	"""
	pipeline = Pipeline(settings_path=context['configs_dir'])
	base_year = int(context.get('split_hct_base_year', 2020))
	parcel_base_year = int(context.get('split_hct_parcel_base_year', 2018))
	creds_path = _resolve_path(R_SCRIPTS_DIR, context.get('split_hct_creds_file', 'creds.txt'))
	legacy_base_data_path = _resolve_path(PROJECT_ROOT, context.get('split_hct_base_data_file', R_SCRIPTS_DIR / 'inputs' / f'base_data_shares_{base_year}.rda'))

	base_data = load_base_data_from_mysql(f'{parcel_base_year}_parcel_baseyear', creds_path)
	pipeline.save_table(get_base_data_table_name(base_year), base_data)

	if bool(context.get('split_hct_save_base_data_file', False)):
		maybe_save_base_data(base_data, legacy_base_data_path)

	return context