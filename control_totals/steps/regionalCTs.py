"""Regional control totals step.

Port of ``r_scripts/regionalCTs.R`` (active configuration only). Disaggregates
the regional aggregate household / household-population forecast into bins by
persons-per-household (PPH), workers, and income:

  1. Loads base-year household counts grouped by (pph, workers, income) from the
	 base-year parcel MySQL database (the R ``get.data.from.mysql = TRUE`` path).
  2. Loads the regional forecast (``hh_pop``, ``household_count``, ``job_count``)
	 from the pipeline table ``split_ct_unrolled_regional`` -- the Python
	 equivalent of the R ``unrolled_regional`` Excel sheet.
  3. Builds a year x pph grid (``CTpph``) and applies Larry Blain's two-ratio
	 formula across years, then rebalances to match the aggregate HH / HHpop
	 controls.
  4. Expands to year x pph x workers x income using base-year shares and writes
	 the UrbanSim-format ``annual_household_control_totals_region`` table to the
	 pipeline HDF5 store (and optionally a CSV).

Employment control totals (the R ``create.emp.totals = TRUE`` branch) are
supported but disabled by default; enable them via the ``regional_cts``
settings (``create_emp_totals`` / ``scale_emp_controls``).

The inactive R branches (PUMS / ``psrccensus``, Elmer ODBC forecast, REF
forecast sheet, and MySQL output) are intentionally omitted.

Note: the rebalancing step uses weighted random sampling with a fixed seed.
Results are reproducible run-to-run in Python but are NOT bit-for-bit identical
to the R script, because NumPy and R use different RNG algorithms.
"""
from pathlib import Path

import numpy as np
import pandas as pd

from util import Pipeline, get_mysql_engine, get_mysql_config
from util.db_helpers import DEFAULT_USER_ENV, DEFAULT_PASSWORD_ENV, DEFAULT_HOST_ENV
from util import ct_allocation
from util.ct_allocation import INCOME_BINS as DEFAULT_INCOME_BINS
from util.ct_allocation import INCOME_LABELS as DEFAULT_INCOME_LABELS


# ---------------------------------------------------------------------------
# Input loading
# ---------------------------------------------------------------------------
def load_base_year_hh(base_db, base_year, income_bins, income_labels, creds_path=None,
					   user_env=DEFAULT_USER_ENV,
					   password_env=DEFAULT_PASSWORD_ENV,
					   host_env=DEFAULT_HOST_ENV):
	"""Load base-year household counts grouped by (pph, workers, income).

	Reproduces the R ``get.data.from.mysql`` query: groups the base-year
	``households`` table into PPH bins 1..7, worker bins 0..4, and the income
	brackets defined by ``income_bins`` / ``income_labels``, returning counts,
	the average persons-per-household (``mean_pph``) per group, and within-PPH
	shares.

	Args:
		base_db (str): Name of the base-year MySQL database
			(e.g. ``'2023_parcel_baseyear'``).
		base_year (int): Base year label assigned to the ``year`` column.
		income_bins (list[int]): Lower (inclusive) bound of each income bracket.
		income_labels (list[str]): Label for each income bracket.
		creds_path (pathlib.Path, optional): Fallback credentials file.
		user_env, password_env, host_env (str): Env var names for credentials.

	Returns:
		pandas.DataFrame: Columns ``year, pph, workers, income, count,
			mean_pph, share``.
	"""
	engine = get_mysql_engine(
		base_db, creds_path, user_env=user_env, password_env=password_env, host_env=host_env
	)

	prsn_sql = "CASE WHEN h.persons < 7 THEN h.persons ELSE 7 END"
	wrkr_sql = "CASE WHEN h.workers < 4 THEN h.workers ELSE 4 END"
	# Build the income CASE expression from the configured brackets, highest
	# bound first, falling back to the lowest bracket's label.
	inc_clauses = ' '.join(
		f"WHEN h.income >= {income_bins[i]} THEN '{income_labels[i]}'"
		for i in range(len(income_bins) - 1, 0, -1)
	)
	inc_sql = f"CASE {inc_clauses} ELSE '{income_labels[0]}' END"
	query = (
		f"SELECT {int(base_year)} AS year, {prsn_sql} AS pph, {wrkr_sql} AS workers, "
		f"{inc_sql} AS income, count(*) AS count, avg(h.persons) AS mean_pph "
		f"FROM households AS h "
		f"GROUP BY {prsn_sql}, {wrkr_sql}, {inc_sql}"
	)

	hhs_full = pd.read_sql_query(query, engine)
	hhs_full['income'] = hhs_full['income'].str.strip()
	for col in ('year', 'pph', 'workers', 'count'):
		hhs_full[col] = hhs_full[col].astype(int)

	# Within-PPH share (R: hhs_full[sums, share := count / hh_tot])
	hhs_full['share'] = hhs_full['count'] / hhs_full.groupby('pph')['count'].transform('sum')
	return hhs_full


def load_regional_forecast(pipeline, base_year, table_name='split_ct_unrolled_regional'):
	"""Load the regional forecast and rename to the R ``forecast`` columns.

	Reads ``split_ct_unrolled_regional`` from the pipeline (the equivalent of
	the R ``unrolled_regional`` Excel sheet) and maps
	``total_hhpop -> hh_pop``, ``total_hh -> household_count``,
	``total_emp -> job_count``.

	Returns:
		pandas.DataFrame: Columns ``year, hh_pop, household_count, job_count``.
	"""
	reg = pipeline.get_table(table_name).copy()
	reg['year'] = reg['year'].astype(int)
	rename = {'total_hhpop': 'hh_pop', 'total_hh': 'household_count', 'total_emp': 'job_count'}
	missing = set(rename) - set(reg.columns)
	if missing:
		raise KeyError(f'{table_name} is missing required columns: {missing}')
	forecast = reg.rename(columns=rename)[['year', 'hh_pop', 'household_count', 'job_count']]
	return forecast.sort_values('year').reset_index(drop=True)


# ---------------------------------------------------------------------------
# CTpph construction (households by size)
# ---------------------------------------------------------------------------
def build_ctpph_grid(hhs_full, forecast, base_year):
	"""Build the year x pph grid (``CTpph``) seeded with base-year counts.

	Mirrors the R block "1. Households by size": all (year, pph) combinations
	for ``year >= base_year``, base-year HH counts by pph (NA-filled with 1),
	and ``mean_pph`` per pph (unweighted mean of the base-year group means).

	Returns:
		pandas.DataFrame: Columns ``year, pph, household_count, mean_pph``.
	"""
	base_year = int(base_year)
	years = np.array(
		sorted(forecast.loc[forecast['year'] >= base_year, 'year'].unique()), dtype=int
	)
	pphs = np.arange(1, 8, dtype=int)
	ctpph = pd.MultiIndex.from_product(
		[years, pphs], names=['year', 'pph']
	).to_frame(index=False)

	# Base-year HH counts by pph (summed over workers/income)
	base_counts = hhs_full.groupby('pph', as_index=False)['count'].sum().rename(
		columns={'count': 'base_hh'}
	)
	ctpph = ctpph.merge(base_counts, on='pph', how='left')
	is_base = ctpph['year'] == base_year
	ctpph['household_count'] = np.where(is_base, ctpph['base_hh'], np.nan)
	ctpph['household_count'] = ctpph['household_count'].fillna(1).astype(float)

	# mean_pph per pph: unweighted mean of base-year group means (R behaviour)
	mean_pph = hhs_full.groupby('pph', as_index=False)['mean_pph'].mean()
	ctpph = ctpph.merge(mean_pph, on='pph', how='left')
	ctpph['mean_pph'] = ctpph['mean_pph'].fillna(ctpph['pph'].astype(float))

	return ctpph.drop(columns=['base_hh'])


# ---------------------------------------------------------------------------
# Larry Blain's formula & rebalancing (shared with subregionalCTs via
# util.ct_allocation; regional uses an empty grouping and the forecast's
# hh_pop / household_count control columns).
# ---------------------------------------------------------------------------
def iterate_hhpop_control(ctpph, forecast):
	"""Apply Larry Blain's formula iteratively across sorted years (regional)."""
	return ct_allocation.iterate_hhpop_control(
		ctpph, forecast, group_keys=[], hh_col='household_count', hhpop_col='hh_pop'
	)


def outer_rebalance(ctpph, forecast, rng, max_iterations=20, min_added=10):
	"""Run the alternating pop/HH rebalance loop until convergence (regional)."""
	return ct_allocation.outer_rebalance(
		ctpph, forecast, rng, group_keys=[], hh_col='household_count', hhpop_col='hh_pop',
		max_iterations=max_iterations, min_added=min_added,
	)


# ---------------------------------------------------------------------------
# Workers & income disaggregation (R block "2.")
# ---------------------------------------------------------------------------
def build_ctpop(ctpph, hhs_full, forecast, income_labels):
	"""Expand ``CTpph`` to year x pph x workers x income using base-year shares.

	Builds all combinations of (year, pph 1..7, workers 0..4, income), drops
	nonsense rows (``pph < workers``), applies base-year within-PPH worker /
	income shares, and multiplies by the per-(year, pph) HH count from
	``CTpph``.

	Returns:
		pandas.DataFrame: Columns ``year, pph, workers, income, share,
			household_count``.
	"""
	years = np.array(sorted(forecast['year'].unique()), dtype=int)
	pphs = np.arange(1, 8, dtype=int)
	workers = np.arange(0, 5, dtype=int)
	incomes = list(income_labels)

	ctpop = pd.MultiIndex.from_product(
		[years, pphs, workers, incomes], names=['year', 'pph', 'workers', 'income']
	).to_frame(index=False)
	ctpop = ctpop[ctpop['pph'] >= ctpop['workers']].reset_index(drop=True)

	# Apply base-year worker & income shares by pph
	shares = hhs_full[['pph', 'workers', 'income', 'share']]
	ctpop = ctpop.merge(shares, on=['pph', 'workers', 'income'], how='left')

	# Apply per-(year, pph) HH counts from CTpph
	ctpop = ctpop.merge(
		ctpph[['year', 'pph', 'household_count']], on=['year', 'pph'], how='left'
	)
	ctpop['household_count'] = ctpop['share'] * ctpop['household_count']
	return ctpop


# ---------------------------------------------------------------------------
# Output construction (R block "3.")
# ---------------------------------------------------------------------------
HH_COLS = [
	'year', 'total_number_of_households',
	'income_min', 'income_max',
	'persons_min', 'persons_max',
	'workers_min', 'workers_max',
]


def build_hh_output(ctpop, base_year, income_bins, income_labels):
	"""Build the regional HH UrbanSim CT rows (``year > base_year``)."""
	base_year = int(base_year)
	df = ctpop[ctpop['year'] > base_year].copy()
	df = df[df['household_count'].notna()].copy()

	n = len(income_bins)
	inc_idx = df['income'].map({label: i for i, label in enumerate(income_labels)})
	df['income_min'] = inc_idx.map(lambda i: income_bins[i]).astype(int)
	# Top bracket is open-ended (max = -1); others end one below the next bound.
	df['income_max'] = inc_idx.map(
		lambda i: income_bins[i + 1] - 1 if i + 1 < n else -1
	).astype(int)
	df['persons_min'] = df['pph'].astype(int)
	df['persons_max'] = np.where(df['pph'] < 7, df['pph'], -1).astype(int)
	df['workers_min'] = df['workers'].astype(int)
	df['workers_max'] = np.where(df['workers'] < 4, df['workers'], -1).astype(int)
	df['total_number_of_households'] = df['household_count'].round().astype(int)
	return df[HH_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Employment control totals (R block "if(create.emp.totals)")
# ---------------------------------------------------------------------------
def load_emp_control_totals(emp_ct_table, base_db, creds_path=None,
							user_env=DEFAULT_USER_ENV,
							password_env=DEFAULT_PASSWORD_ENV,
							host_env=DEFAULT_HOST_ENV):
	"""Load the base employment control totals table from MySQL.

	Reproduces the R ``select * from <emp.ct.table>`` query, dropping the
	``city_id`` column when present. ``emp_ct_table`` may be fully qualified
	(``database.table``); the engine connects to ``base_db`` and MySQL resolves
	the cross-database reference, matching the R behaviour.

	Args:
		emp_ct_table (str): Source employment CT table, optionally fully
			qualified (e.g.
			``'psrc_2014_parcel_baseyear_just_friends.annual_employment_control_totals_lum_sector'``).
		base_db (str): Database the engine connects to.
		creds_path (pathlib.Path, optional): Fallback credentials file.
		user_env, password_env, host_env (str): Env var names for credentials.

	Returns:
		pandas.DataFrame: The employment CT table, without ``city_id``.
	"""
	engine = get_mysql_engine(
		base_db, creds_path, user_env=user_env, password_env=password_env, host_env=host_env
	)
	emp = pd.read_sql_query(f'SELECT * FROM {emp_ct_table}', engine)
	if 'city_id' in emp.columns:
		emp = emp.drop(columns=['city_id'])
	return emp


def build_emp_output(emp, forecast, base_year, scale_emp_controls=True):
	"""Filter (and optionally scale) employment control totals to the forecast.

	Mirrors the R ``create.emp.totals`` block: keeps ``year > base_year`` and,
	when ``scale_emp_controls`` is true, rescales each year's
	``total_number_of_jobs`` proportionally so that the per-year total matches
	the forecast ``job_count``.

	Args:
		emp (pandas.DataFrame): Base employment CT table (must contain ``year``
			and ``total_number_of_jobs``).
		forecast (pandas.DataFrame): Regional forecast with ``year`` and
			``job_count`` (only required when scaling).
		base_year (int): Base year; only rows with ``year > base_year`` are kept.
		scale_emp_controls (bool): If true, scale to the forecast ``job_count``;
			otherwise the source totals are copied through.

	Returns:
		pandas.DataFrame: Employment CT rows with an integer
			``total_number_of_jobs`` column.
	"""
	base_year = int(base_year)
	df = emp[emp['year'].astype(int) > base_year].copy()
	df['year'] = df['year'].astype(int)

	if scale_emp_controls:
		if 'job_count' not in forecast.columns:
			raise KeyError('forecast must contain a job_count column to scale employment controls')
		df['share'] = df['total_number_of_jobs'] / df.groupby('year')['total_number_of_jobs'].transform('sum')
		forecast_total = forecast.set_index('year')['job_count']
		df['forecast_total'] = df['year'].map(forecast_total)
		df['total_number_of_jobs'] = (df['forecast_total'] * df['share']).round().astype(int)
		df = df.drop(columns=['share', 'forecast_total'])
	else:
		df['total_number_of_jobs'] = df['total_number_of_jobs'].round().astype(int)

	return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step entry point
# ---------------------------------------------------------------------------
def run_step(context):
	"""Execute the regional control totals pipeline step.

	Loads base-year HH composition (MySQL) and the regional forecast
	(``split_ct_unrolled_regional``), distributes the forecast into pph /
	workers / income bins via Larry Blain's formula plus rebalancing, and
	saves the UrbanSim-format ``annual_household_control_totals_region`` table
	to ``pipeline.h5`` (and optionally a CSV).

	Args:
		context (dict): pypyr context (must contain ``'configs_dir'``).

	Returns:
		dict: The unchanged context.
	"""
	pipeline = Pipeline(settings_path=context['configs_dir'])
	cfg = pipeline.settings.get('regional_cts', {})

	base_year = int(pipeline.settings['base_year'])
	end_year = int(pipeline.settings.get('end_year', 0)) or None

	db = get_mysql_config(pipeline)
	base_db = db['database']
	creds_path = db['creds_path']
	user_env = db['user_env']
	password_env = db['password_env']
	host_env = db['host_env']

	rng_seed = int(cfg.get('rng_seed', 1234))
	max_iter = int(cfg.get('max_outer_iterations', 20))
	min_added = int(cfg.get('min_added_break', 10))
	save_csv = bool(cfg.get('save_csv', True))
	forecast_table = cfg.get('forecast_table', 'split_ct_unrolled_regional')
	output_table = cfg.get('output_table', 'annual_household_control_totals_region')
	income_bins = cfg.get('income_bins', DEFAULT_INCOME_BINS)
	income_labels = cfg.get('income_labels', DEFAULT_INCOME_LABELS)
	if len(income_bins) != len(income_labels):
		raise ValueError('regional_cts.income_bins and income_labels must have the same length')

	create_emp_totals = bool(cfg.get('create_emp_totals', False))
	scale_emp_controls = bool(cfg.get('scale_emp_controls', True))
	emp_ct_table = cfg.get('emp_ct_table')
	emp_output_table = cfg.get('emp_output_table', 'annual_employment_control_totals_region')
	if create_emp_totals and not emp_ct_table:
		raise ValueError('regional_cts.emp_ct_table must be set when create_emp_totals is true')

	rng = np.random.default_rng(rng_seed)

	print('Loading inputs...')
	hhs_full = load_base_year_hh(
		base_db, base_year, income_bins, income_labels, creds_path=creds_path,
		user_env=user_env, password_env=password_env, host_env=host_env,
	)
	forecast = load_regional_forecast(pipeline, base_year, table_name=forecast_table)

	print('Building CTpph grid and seeding base-year counts...')
	ctpph = build_ctpph_grid(hhs_full, forecast, base_year)

	print('Running Larry Blain HH/PPH allocation across years...')
	ctpph = iterate_hhpop_control(ctpph, forecast)
	ctpph['hhpop'] = ctpph['mean_pph'] * ctpph['household_count']

	print('Rebalancing to match aggregate controls...')
	ctpph = outer_rebalance(ctpph, forecast, rng, max_iterations=max_iter, min_added=min_added)

	print('Disaggregating by workers and income...')
	ctpop = build_ctpop(ctpph, hhs_full, forecast, income_labels)

	print('Building output table...')
	res_hh = build_hh_output(ctpop, base_year, income_bins, income_labels)
	if end_year is not None:
		res_hh = res_hh[res_hh['year'] <= end_year].reset_index(drop=True)

	pipeline.save_table(output_table, res_hh)

	if save_csv:
		out_dir = Path(pipeline.get_output_dir())
		out_dir.mkdir(parents=True, exist_ok=True)
		csv_path = out_dir / f'{output_table}.csv'
		res_hh.to_csv(csv_path, index=False)
		print(f'Wrote CSV output to {csv_path}')

	if create_emp_totals:
		print('Building employment control totals...')
		emp = load_emp_control_totals(
			emp_ct_table, base_db, creds_path=creds_path,
			user_env=user_env, password_env=password_env, host_env=host_env,
		)
		res_emp = build_emp_output(emp, forecast, base_year, scale_emp_controls=scale_emp_controls)
		if end_year is not None:
			res_emp = res_emp[res_emp['year'] <= end_year].reset_index(drop=True)

		pipeline.save_table(emp_output_table, res_emp)

		if save_csv:
			out_dir = Path(pipeline.get_output_dir())
			out_dir.mkdir(parents=True, exist_ok=True)
			emp_csv_path = out_dir / f'{emp_output_table}.csv'
			res_emp.to_csv(emp_csv_path, index=False)
			print(f'Wrote CSV output to {emp_csv_path}')

	return context
