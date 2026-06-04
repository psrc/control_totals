from pathlib import Path

import numpy as np
import pandas as pd

from util import Pipeline, get_mysql_engine, get_mysql_config
from util.db_helpers import DEFAULT_USER_ENV, DEFAULT_PASSWORD_ENV, DEFAULT_HOST_ENV


PROJECT_ROOT = Path(__file__).resolve().parents[1]
R_SCRIPTS_DIR = PROJECT_ROOT / 'r_scripts'


def load_base_data_from_mysql(base_db, creds_path, user_env=DEFAULT_USER_ENV, password_env=DEFAULT_PASSWORD_ENV, host_env=DEFAULT_HOST_ENV):
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
	engine = get_mysql_engine(
		base_db, creds_path, user_env=user_env, password_env=password_env, host_env=host_env
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
	# Left-join from the xwalk so control areas whose parcels had no households
	# or jobs in the parcel-base-year MySQL DB still appear (with zeros) rather
	# than being dropped by an inner-join.
	base_data = parcels_hct.merge(base_data, on='parcel_id', how='left')
	agg_cols = ['households', 'persons', 'jobs']
	for col in agg_cols:
		base_data[col] = base_data[col].fillna(0)
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


def get_subreg_pph_table_names(base_year):
	"""Return the three HDF5 table keys for PPH base-year data.

	Args:
		base_year (int): The base year.

	Returns:
		tuple[str, str, str]: Keys for ``hh_by_pph``,
			``mean_pph_by_subreg``, and ``mean_pph_by_county`` tables.
	"""
	by = int(base_year)
	return (
		f'subreg_hh_by_pph_{by}',
		f'subreg_mean_pph_{by}',
		f'subreg_mean_pph_county_{by}',
	)


def load_pph_base_data_from_mysql(base_db, creds_path,
								  user_env=DEFAULT_USER_ENV,
								  password_env=DEFAULT_PASSWORD_ENV,
								  host_env=DEFAULT_HOST_ENV):
	"""Load parcel-level base-year household counts and person sums by PPH bin.

	Queries the parcel base-year database for household counts and total
	person counts grouped by parcel and capped persons-per-household bin
	(``CASE WHEN persons > 7 THEN 7 ELSE persons END``). Returns parcel-level
	data so the caller can aggregate using the current control-area
	crosswalk (the parcel base-year DB's ``subreg_id``/``county_id_orig``
	columns are stale).

	Args:
		base_db (str): MySQL database name (e.g. ``'2023_parcel_baseyear'``).
		creds_path (pathlib.Path): Path to the credentials file.
		user_env (str, optional): Env var name for the MySQL username.
		password_env (str, optional): Env var name for the MySQL password.
		host_env (str, optional): Env var name for the MySQL host.

	Returns:
		pandas.DataFrame: Columns ``parcel_id``, ``pph`` (1..7),
			``household_count``, ``persons_sum``.
	"""
	engine = get_mysql_engine(
		base_db, creds_path, user_env=user_env, password_env=password_env, host_env=host_env
	)

	query = """
		SELECT t2.parcel_id,
			   (CASE WHEN t1.persons > 7 THEN 7 ELSE t1.persons END) AS pph,
			   COUNT(t1.household_id) AS household_count,
			   SUM(t1.persons) AS persons_sum
		FROM households AS t1
		JOIN buildings AS t2 ON t1.building_id = t2.building_id
		GROUP BY t2.parcel_id,
				 (CASE WHEN t1.persons > 7 THEN 7 ELSE t1.persons END)
	"""
	return pd.read_sql_query(query, engine)


def aggregate_pph_base_data(p, parcel_pph):
	"""Aggregate parcel-level PPH data to subreg/county using the current xwalk.

	Mirrors :func:`aggregate_base_data`: joins parcel-level rows to
	``current_parcel_control_area_xwalk`` to obtain the up-to-date
	``subreg_id``/``control_id`` for each parcel, then derives ``county_id``
	from ``control_target_xwalk``. Produces three aggregated frames matching
	what the legacy R queries returned, but using the current control-area
	geography.

	Args:
		p (Pipeline): The data pipeline (used to read crosswalks).
		parcel_pph (pandas.DataFrame): Output of
			:func:`load_pph_base_data_from_mysql`.

	Returns:
		tuple[pandas.DataFrame, pandas.DataFrame, pandas.DataFrame]:
			``hh_by_pph`` (subreg_id, county_id, pph, household_count),
			``mean_pph_subreg`` (subreg_id, mean_pph) for the 7+ bin,
			``mean_pph_county`` (county_id, mean_pph) for the 7+ bin.
	"""
	parcels_hct = p.get_table('current_parcel_control_area_xwalk')[['parcel_id', 'subreg_id', 'control_id']]
	cnty_xwalk = (
		p.get_table('control_target_xwalk')[['control_id', 'county_id']]
		.drop_duplicates()
	)

	merged = (
		parcels_hct
		.merge(parcel_pph, on='parcel_id', how='inner')
		.merge(cnty_xwalk, on='control_id', how='left')
	)

	hh_by_pph = (
		merged
		.groupby(['subreg_id', 'county_id', 'pph'], as_index=False)[['household_count', 'persons_sum']]
		.sum()
	)

	bin7 = hh_by_pph[hh_by_pph['pph'] == 7]

	subreg_sums = bin7.groupby('subreg_id', as_index=False)[['household_count', 'persons_sum']].sum()
	subreg_sums['mean_pph'] = subreg_sums['persons_sum'] / subreg_sums['household_count'].replace(0, np.nan)
	mean_pph_subreg = subreg_sums[['subreg_id', 'mean_pph']]

	county_sums = bin7.groupby('county_id', as_index=False)[['household_count', 'persons_sum']].sum()
	county_sums['mean_pph'] = county_sums['persons_sum'] / county_sums['household_count'].replace(0, np.nan)
	mean_pph_county = county_sums[['county_id', 'mean_pph']]

	hh_by_pph = hh_by_pph.drop(columns=['persons_sum'])
	return hh_by_pph, mean_pph_subreg, mean_pph_county


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
	db = get_mysql_config(pipeline)
	creds_path = db['creds_path']
	base_data_path = data_dir / cfg.get('base_data_file', f'inputs/base_data_shares_{base_year}.rda')
	use_mysql = bool(cfg.get('use_mysql', False))
	save_base_data_file = bool(cfg.get('save_base_data_file', False))
	table_name = get_base_data_table_name(base_year)

	if use_mysql:
		base_db = db['database']
		user_env = db['user_env']
		password_env = db['password_env']
		host_env = db['host_env']

		base_data = load_base_data_from_mysql(
			base_db,
			creds_path,
			user_env=user_env,
			password_env=password_env,
			host_env=host_env,
		)
		base_data = aggregate_base_data(pipeline, base_data)
		pipeline.save_table(table_name, base_data)
		if save_base_data_file:
			maybe_save_base_data(base_data, base_data_path)

		# Also fetch and aggregate base-year HH-by-PPH data, used by the
		# downstream subregionalCTs step. Centralizing MySQL access here
		# means subregionalCTs only needs to read from pipeline.h5.
		hh_key, mean_subreg_key, mean_county_key = get_subreg_pph_table_names(base_year)
		parcel_pph = load_pph_base_data_from_mysql(
			base_db,
			creds_path,
			user_env=user_env,
			password_env=password_env,
			host_env=host_env,
		)
		hh_by_pph, mean_pph_subreg, mean_pph_county = aggregate_pph_base_data(pipeline, parcel_pph)
		pipeline.save_table(hh_key, hh_by_pph)
		pipeline.save_table(mean_subreg_key, mean_pph_subreg)
		pipeline.save_table(mean_county_key, mean_pph_county)
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