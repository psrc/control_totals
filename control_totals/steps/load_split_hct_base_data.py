import os
from pathlib import Path
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from util import Pipeline


PROJECT_ROOT = Path(__file__).resolve().parents[1]
R_SCRIPTS_DIR = PROJECT_ROOT / 'r_scripts'


def _read_mysql_creds(creds_path=None, user_env='URBANSIM_MYSQL_USER', password_env='URBANSIM_MYSQL_PASSWORD', host_env='URBANSIM_MYSQL_HOST'):
	"""Resolve MySQL credentials from environment variables or a credentials file.

	Prefers environment variables (whose names default to ``URBANSIM_MYSQL_USER``,
	``URBANSIM_MYSQL_PASSWORD``, and ``URBANSIM_MYSQL_HOST`` but can be overridden
	via settings). Falls back to a plain-text credentials file (three non-empty
	lines: username, password, host) when one or more env vars are missing and
	``creds_path`` exists.

	Args:
		creds_path (pathlib.Path, optional): Path to a fallback credentials file.
		user_env (str, optional): Env var name for the MySQL username.
		password_env (str, optional): Env var name for the MySQL password.
		host_env (str, optional): Env var name for the MySQL host.

	Returns:
		tuple[str, str, str]: ``(username, password, host)``.

	Raises:
		ValueError: If credentials cannot be resolved from env vars or file.
	"""
	user = os.environ.get(user_env)
	password = os.environ.get(password_env)
	host = os.environ.get(host_env)
	if user and password and host:
		return user, password, host

	if creds_path is not None and Path(creds_path).exists():
		lines = [line.strip() for line in Path(creds_path).read_text().splitlines() if line.strip()]
		if len(lines) < 3:
			raise ValueError('Expected username, password, and host in creds file')
		return lines[0], lines[1], lines[2]

	raise ValueError(
		f'MySQL credentials not found. Set {user_env}, {password_env}, '
		f'and {host_env} environment variables, or provide a creds file.'
	)


def load_base_data_from_mysql(base_db, creds_path, user_env='URBANSIM_MYSQL_USER', password_env='URBANSIM_MYSQL_PASSWORD', host_env='URBANSIM_MYSQL_HOST'):
	"""Load household, person, and job base data from a MySQL database.

	Queries the base-year MySQL database for household/person and job
	counts grouped by parcel, then merges the two result sets.

	Args:
		base_db (str): Name of the MySQL database (e.g.
			``'2018_parcel_baseyear'``).
		creds_path (pathlib.Path): Path to the credentials file.
		user_env (str, optional): Env var name for the MySQL username.
		password_env (str, optional): Env var name for the MySQL password.
		host_env (str, optional): Env var name for the MySQL host.

	Returns:
		pandas.DataFrame: Base data with columns ``parcel_id``,
			``households``, ``persons``, and ``jobs``.
	"""
	user, password, host = _read_mysql_creds(creds_path, user_env=user_env, password_env=password_env, host_env=host_env)
	engine = create_engine(
		f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@{host}/{base_db}"
	)

	households_query = f"""
		select t2.parcel_id,
			   count(t1.household_id) as households,
			   sum(persons) as persons
		from households as t1
		join buildings as t2 on t1.building_id = t2.building_id
		group by t2.parcel_id
	"""
	jobs_query = f"""
		select t2.parcel_id,
			   count(t1.job_id) as jobs
		from jobs as t1
		join buildings as t2 on t1.building_id = t2.building_id
		group by t2.parcel_id
	"""

	hh_base = pd.read_sql_query(households_query, engine)
	job_base = pd.read_sql_query(jobs_query, engine)
	base_data = hh_base.merge(job_base, on='parcel_id', how='outer')
	for column in ['households', 'persons', 'jobs']:
		base_data[column] = base_data[column].fillna(0)
	return base_data

def aggregate_base_data(p, base_data):
	parcels_hct = p.get_table('current_parcel_control_area_xwalk')[['parcel_id', 'subreg_id', 'control_id']]
	base_data = base_data.merge(parcels_hct, on='parcel_id')
	agg_cols = ['households', 'persons', 'jobs']
	base_data = base_data.groupby(['subreg_id', 'control_id'], as_index=False)[agg_cols].sum()
	base_data.rename(columns={'subreg_id': 'split_geo_id', 'control_id': 'nosplit_geo_id'}, inplace=True)

	xwalk = p.get_table('control_target_xwalk')[['control_id', 'control_name', 'rgid']].drop_duplicates()
	base_data = base_data.merge(
		xwalk.rename(columns={'control_id': 'nosplit_geo_id', 'control_name': 'name', 'rgid': 'RGID'}),
		on='nosplit_geo_id',
		how='left',
	)
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


def _load_base_data_from_file(base_data_path):
	"""Load base-data from a local file (.pkl, .parquet, or .csv)."""
	if base_data_path.suffix.lower() == '.pkl':
		return pd.read_pickle(base_data_path)
	if base_data_path.suffix.lower() == '.parquet':
		return pd.read_parquet(base_data_path)
	if base_data_path.suffix.lower() == '.csv':
		return pd.read_csv(base_data_path)
	raise ValueError(f'Unsupported base data file type: {base_data_path.suffix}')


def run_step(context):
	"""Execute the base-data loading pipeline step for the HCT split.

	Honors the ``split_hct`` settings block. When ``use_mysql`` is True,
	loads household/job counts from a MySQL parcel base-year database,
	aggregates to subreg/control geography, and saves to the pipeline
	HDF5 store. When ``use_mysql`` is False, attempts to load a cached
	copy from the pipeline HDF5 store, falling back to ``base_data_file``
	if present, and otherwise leaves the pipeline as-is so a downstream
	step can populate it.

	Args:
		context (dict): The pypyr context dictionary, expected to contain
			a ``'configs_dir'`` key.

	Returns:
		dict: The unchanged pypyr context dictionary.
	"""
	pipeline = Pipeline(settings_path=context['configs_dir'])
	base_year = pipeline.settings['base_year']
	cfg = pipeline.settings.get('split_hct', {})

	data_dir = Path(pipeline.get_data_dir())
	parcel_base_year = int(cfg.get('parcel_base_year', 2018))
	creds_path = data_dir / cfg.get('creds_file', 'creds.txt')
	base_data_path = data_dir / cfg.get('base_data_file', f'inputs/base_data_shares_{base_year}.rda')
	use_mysql = bool(cfg.get('use_mysql', False))
	save_base_data_file = bool(cfg.get('save_base_data_file', False))
	table_name = get_base_data_table_name(base_year)

	if use_mysql:
		base_data = load_base_data_from_mysql(
			f'{parcel_base_year}_parcel_baseyear',
			creds_path,
			user_env=cfg.get('urbansim_mysql_user', 'URBANSIM_MYSQL_USER'),
			password_env=cfg.get('urbansim_mysql_pass', 'URBANSIM_MYSQL_PASSWORD'),
			host_env=cfg.get('urbansim_mysql_host', 'URBANSIM_MYSQL_HOST'),
		)
		base_data = aggregate_base_data(pipeline, base_data)
		pipeline.save_table(table_name, base_data)
		if save_base_data_file:
			maybe_save_base_data(base_data, base_data_path)
		return context

	if pipeline.check_table_exists(table_name):
		print(f'Base data table {table_name} already present in pipeline.h5; skipping load.')
		return context

	if base_data_path.exists() and base_data_path.suffix.lower() in {'.pkl', '.parquet', '.csv'}:
		base_data = _load_base_data_from_file(base_data_path)
		pipeline.save_table(table_name, base_data)
		return context

	print(
		f'Skipping load_split_hct_base_data: use_mysql is false, no cached '
		f'table {table_name} in pipeline.h5, and no usable base data file at '
		f'{base_data_path}. Set split_hct.use_mysql=true or provide a '
		f'.pkl/.parquet/.csv base_data_file.'
	)
	return context