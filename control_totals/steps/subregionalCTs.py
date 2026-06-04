"""Subregional control totals step.

Port of ``r_scripts/subregionalCTs.R``. Distributes per-(subreg, year)
household control totals into persons-per-household (PPH) bins 1..7 using
Larry Blain's two-ratio formula, rebalances to match aggregate HH/HHpop
controls, and writes the final ``annual_household_control_totals`` and
``annual_employment_control_totals`` tables (with both subregional rows
and ``subreg_id == -1`` regional rows) to the pipeline HDF5 store.
"""
from pathlib import Path

import numpy as np
import pandas as pd

from util import Pipeline
from util import ct_allocation
from steps.load_split_hct_base_data import get_subreg_pph_table_names


# ---------------------------------------------------------------------------
# Input loading
# ---------------------------------------------------------------------------
def load_unrolled_cts(pipeline, base_year):
	"""Return the subregional unrolled control totals (`split_ct_unrolled`).

	Casts ``year`` to int, filters to ``year >= base_year``, and ensures
	the expected indicator columns are present.

	Args:
		pipeline (Pipeline): The data pipeline.
		base_year (int): Lower bound on years to keep.

	Returns:
		pandas.DataFrame: Columns ``subreg_id, year, total_hh, total_hhpop, total_emp``.
	"""
	cts = pipeline.get_table('split_ct_unrolled').copy()
	cts['year'] = cts['year'].astype(int)
	cts = cts[cts['year'] >= int(base_year)].reset_index(drop=True)
	required = {'subreg_id', 'year', 'total_hh', 'total_hhpop', 'total_emp'}
	missing = required - set(cts.columns)
	if missing:
		raise KeyError(f'split_ct_unrolled is missing required columns: {missing}')
	return cts


def load_regional_unrolled(pipeline, base_year):
	"""Return the regional unrolled control totals (`split_ct_unrolled_regional`).

	Args:
		pipeline (Pipeline): The data pipeline.
		base_year (int): Lower bound on years to keep.

	Returns:
		pandas.DataFrame: Columns ``subreg_id, year, total_hh, total_hhpop, total_emp``.
	"""
	reg = pipeline.get_table('split_ct_unrolled_regional').copy()
	reg['year'] = reg['year'].astype(int)
	reg = reg[reg['year'] >= int(base_year)].reset_index(drop=True)
	return reg


def load_subreg_county_xwalk(pipeline):
	"""Build a unique ``subreg_id -> county_id`` mapping.

	Joins ``current_parcel_control_area_xwalk`` (which maps the current
	parcels to ``subreg_id`` and ``control_id``) to ``control_target_xwalk``
	(which maps ``control_id`` to ``county_id``). Replaces the R script's
	``subregs`` MySQL fetch.

	Args:
		pipeline (Pipeline): The data pipeline.

	Returns:
		pandas.DataFrame: Two columns, ``subreg_id`` and ``county_id``,
			with one row per subreg.
	"""
	parcels = pipeline.get_table('current_parcel_control_area_xwalk')[['subreg_id', 'control_id']]
	cnty = pipeline.get_table('control_target_xwalk')[['control_id', 'county_id']].drop_duplicates()
	xwalk = (
		parcels.drop_duplicates()
		.merge(cnty, on='control_id', how='left')[['subreg_id', 'county_id']]
		.drop_duplicates()
		.reset_index(drop=True)
	)
	dups = xwalk['subreg_id'].duplicated(keep=False)
	if dups.any():
		raise ValueError(
			'subreg_id maps to multiple county_id values: '
			f"{xwalk.loc[dups].sort_values('subreg_id').to_dict('records')}"
		)
	return xwalk


def load_borrow_distribution(pipeline, table_name='borrow_distribution'):
	"""Read the recipient/donor borrow-distribution table from the pipeline."""
	bdist = pipeline.get_table(table_name).copy()
	required = {'recipient_geo_id', 'donor_geo_id'}
	missing = required - set(bdist.columns)
	if missing:
		raise KeyError(f'{table_name} is missing required columns: {missing}')
	return bdist[['recipient_geo_id', 'donor_geo_id']].drop_duplicates()


def load_pph_base_tables(pipeline, base_year):
	"""Read the three cached PPH base-year tables from the pipeline.

	These tables are produced by
	:mod:`steps.load_split_hct_base_data` when ``split_hct.use_mysql`` is
	true. Raises if any are missing so the user knows to run that step first.

	Returns:
		tuple[pandas.DataFrame, pandas.DataFrame, pandas.DataFrame]:
			``hh_by_pph, mean_pph_subreg, mean_pph_county``.
	"""
	hh_key, mean_subreg_key, mean_county_key = get_subreg_pph_table_names(base_year)
	missing = [
		k for k in (hh_key, mean_subreg_key, mean_county_key)
		if not pipeline.check_table_exists(k)
	]
	if missing:
		raise FileNotFoundError(
			f'Missing PPH base-year tables in pipeline.h5: {missing}. '
			'Run `steps.load_split_hct_base_data` first (with split_hct.use_mysql=true).'
		)
	return (
		pipeline.get_table(hh_key),
		pipeline.get_table(mean_subreg_key),
		pipeline.get_table(mean_county_key),
	)


# ---------------------------------------------------------------------------
# CTpop construction
# ---------------------------------------------------------------------------
def build_ctpop_grid(cts, base_year):
	"""Build the full ``(subreg_id x year x pph)`` grid (CTpop).

	Cartesian product of all subreg_ids in ``cts``, sorted unique years
	(adding ``base_year`` if absent), and PPH bins 1..7.

	Returns:
		pandas.DataFrame: Columns ``subreg_id, year, pph`` (int).
	"""
	subregs = np.sort(cts['subreg_id'].unique())
	years = np.array(sorted(set(cts['year'].tolist() + [int(base_year)])), dtype=int)
	pphs = np.arange(1, 8, dtype=int)
	grid = pd.MultiIndex.from_product(
		[subregs, years, pphs], names=['subreg_id', 'year', 'pph']
	).to_frame(index=False)
	return grid


def seed_base_year_counts(ctpop, hh_by_pph, mean_pph_subreg, mean_pph_county,
						   subreg_county, base_year):
	"""Populate base-year household counts and ``mean_pph`` on ``CTpop``.

	Mirrors the R logic: left-join base-year HH counts by (subreg, pph),
	NA-fill with 1, attach county_id, set ``mean_pph = pph`` for pph<7,
	use subreg avg for pph==7, fall back to county avg when the subreg
	value is missing or based on fewer than 5 households.

	Returns:
		pandas.DataFrame: Augmented ``CTpop`` with columns ``household_count``,
			``county_id``, and ``mean_pph``.
	"""
	base_year = int(base_year)
	ctpop = ctpop.merge(subreg_county, on='subreg_id', how='left')

	# Base-year HH counts by pph
	base_counts = hh_by_pph[['subreg_id', 'pph', 'household_count']]
	ctpop = ctpop.merge(base_counts, on=['subreg_id', 'pph'], how='left')
	# Only base-year rows actually use the joined counts; null out non-base years.
	is_base = ctpop['year'] == base_year
	ctpop.loc[~is_base, 'household_count'] = np.nan
	ctpop['household_count'] = ctpop['household_count'].fillna(1).astype(float)

	# mean_pph: for pph<7 just the bin value; for pph==7 use subreg avg w/ county fallback.
	# Subreg-level mean for the 7+ bin (also collect base-year HH count in that bin to apply
	# the R rule "household_count < 5 OR mean_pph is NA => fall back to county avg").
	bin7 = hh_by_pph[hh_by_pph['pph'] == 7][['subreg_id', 'household_count']].rename(
		columns={'household_count': 'bin7_hh_count'}
	)
	subreg_avg = mean_pph_subreg[['subreg_id', 'mean_pph']].rename(columns={'mean_pph': 'subreg_mean_pph'})
	county_avg = mean_pph_county[['county_id', 'mean_pph']].rename(columns={'mean_pph': 'county_mean_pph'})

	ctpop = ctpop.merge(subreg_avg, on='subreg_id', how='left')
	ctpop = ctpop.merge(bin7, on='subreg_id', how='left')
	ctpop = ctpop.merge(county_avg, on='county_id', how='left')

	ctpop['mean_pph'] = ctpop['pph'].astype(float)
	is7 = ctpop['pph'] == 7
	use_county = is7 & (ctpop['subreg_mean_pph'].isna() | (ctpop['bin7_hh_count'].fillna(0) < 5))
	ctpop.loc[is7 & ~use_county, 'mean_pph'] = ctpop.loc[is7 & ~use_county, 'subreg_mean_pph']
	ctpop.loc[use_county, 'mean_pph'] = ctpop.loc[use_county, 'county_mean_pph']
	# Final fallback if a county avg is also missing: keep pph (7).
	ctpop['mean_pph'] = ctpop['mean_pph'].fillna(ctpop['pph'].astype(float))

	return ctpop.drop(columns=['subreg_mean_pph', 'county_mean_pph', 'bin7_hh_count'])


def apply_borrowed_distribution(ctpop, cts, bdist, base_year, last_year):
	"""Replace base-year pph distributions for recipient subregs with their donors'.

	For each ``recipient -> donor`` pair, copy the donor's base-year pph
	share (normalized HH proportions) and rescale by the recipient's
	target ``total_hh`` at ``last_year`` from ``cts``. Mirrors R's
	``set.count.by.borrowed.distr``.

	Returns:
		pandas.DataFrame: ``ctpop`` with updated base-year ``household_count``
			values for recipient subregs.
	"""
	base_year = int(base_year)
	last_year = int(last_year)

	base = ctpop[ctpop['year'] == base_year].copy()
	totals = base.groupby('subreg_id')['household_count'].transform('sum')
	base['hhdistr'] = np.where(totals > 0, base['household_count'] / totals, 0.0)
	donor_distr = base[['subreg_id', 'pph', 'hhdistr']].rename(
		columns={'subreg_id': 'donor_geo_id', 'hhdistr': 'donor_distr'}
	)

	# Recipient -> donor mapping
	pairs = bdist.rename(columns={'recipient_geo_id': 'subreg_id'})
	rec_grid = (
		ctpop[ctpop['year'] == base_year][['subreg_id', 'pph']]
		.merge(pairs, on='subreg_id', how='inner')
		.merge(donor_distr, on=['donor_geo_id', 'pph'], how='left')
	)
	# Multiply by recipient's last-year regional control (`total_hh` from cts)
	target = cts[cts['year'] == last_year][['subreg_id', 'total_hh']]
	rec_grid = rec_grid.merge(target, on='subreg_id', how='left')
	rec_grid['new_hh'] = np.maximum(
		1.0, np.round(rec_grid['donor_distr'].fillna(0) * rec_grid['total_hh'].fillna(0))
	)

	# Apply back to ctpop at base year (left-join the recipient updates and overwrite where present)
	updates = rec_grid[['subreg_id', 'pph', 'new_hh']]
	merged = ctpop.merge(updates, on=['subreg_id', 'pph'], how='left')
	apply_mask = (ctpop['year'] == base_year) & merged['new_hh'].notna()
	ctpop.loc[apply_mask, 'household_count'] = merged.loc[apply_mask, 'new_hh'].to_numpy()
	return ctpop


# ---------------------------------------------------------------------------
# Larry Blain's formula & rebalancing (shared with regionalCTs via
# util.ct_allocation; subregional groups by subreg_id and uses the
# total_hh / total_hhpop control columns).
# ---------------------------------------------------------------------------
def iterate_hhpop_control(ctpop, cts):
	"""Apply Larry Blain's formula iteratively across sorted years (subregional)."""
	return ct_allocation.iterate_hhpop_control(
		ctpop, cts, group_keys=['subreg_id'], hh_col='total_hh', hhpop_col='total_hhpop'
	)


def outer_rebalance(ctpop, cts, rng, max_iterations=20, min_added=10):
	"""Run the alternating pop/HH rebalance loop until convergence (subregional)."""
	return ct_allocation.outer_rebalance(
		ctpop, cts, rng, group_keys=['subreg_id'], hh_col='total_hh', hhpop_col='total_hhpop',
		max_iterations=max_iterations, min_added=min_added,
	)


# ---------------------------------------------------------------------------
# Output construction
# ---------------------------------------------------------------------------
HH_COLS = [
	'subreg_id', 'year', 'total_number_of_households',
	'income_min', 'income_max', 'persons_min', 'persons_max',
	'workers_min', 'workers_max',
]
EMP_COLS = ['subreg_id', 'year', 'total_number_of_jobs', 'home_based_status', 'sector_id']


def build_hh_output(ctpop, base_year):
	"""Build the subregional HH UrbanSim CT rows (year > base_year)."""
	df = ctpop[ctpop['year'] > int(base_year)].copy()
	df['household_count'] = df['household_count'].round().astype(int)
	df['total_number_of_households'] = df['household_count']
	df['income_min'] = 0
	df['income_max'] = -1
	df['persons_min'] = df['pph'].astype(int)
	df['persons_max'] = np.where(df['pph'] < 7, df['pph'], -1).astype(int)
	df['workers_min'] = 0
	df['workers_max'] = -1
	return df[HH_COLS].reset_index(drop=True)


def build_emp_output(cts):
	"""Build the subregional Emp UrbanSim CT rows from `split_ct_unrolled`."""
	df = cts[['subreg_id', 'year', 'total_emp']].copy()
	df['total_number_of_jobs'] = df['total_emp'].round().astype(int)
	df['home_based_status'] = -1
	df['sector_id'] = -1
	return df[EMP_COLS].reset_index(drop=True)


def build_regional_hh_rows(regional, sub_years):
	"""Build regional HH rows for years NOT covered by the subregional output."""
	df = regional[~regional['year'].isin(sub_years)].copy()
	df['total_number_of_households'] = df['total_hh'].round().astype(int)
	df['subreg_id'] = -1
	df['income_min'] = 0
	df['income_max'] = -1
	df['persons_min'] = 0
	df['persons_max'] = -1
	df['workers_min'] = 0
	df['workers_max'] = -1
	return df[HH_COLS].reset_index(drop=True)


def build_regional_emp_rows(regional, sub_years):
	"""Build regional Emp rows for years NOT covered by the subregional output."""
	df = regional[~regional['year'].isin(sub_years)].copy()
	df['total_number_of_jobs'] = df['total_emp'].round().astype(int)
	df['subreg_id'] = -1
	df['home_based_status'] = -1
	df['sector_id'] = -1
	return df[EMP_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------
def check_results(ctpop, cts, base_year):
	"""Log min/max differences in HH and HHpop vs. the input controls."""
	check = (
		ctpop[ctpop['year'] > int(base_year)]
		.assign(hhpop=lambda d: d['mean_pph'] * d['household_count'])
		.groupby(['subreg_id', 'year'], as_index=False)
		.agg(total_hh_sub=('household_count', 'sum'), total_hhpop_sub=('hhpop', 'sum'))
		.merge(cts[['subreg_id', 'year', 'total_hh', 'total_hhpop']], on=['subreg_id', 'year'], how='left')
	)
	check['diff_hh'] = check['total_hh_sub'] - check['total_hh']
	check['diff_hhpop'] = check['total_hhpop_sub'] - check['total_hhpop']
	print(f"\nHH diff range:    [{check['diff_hh'].min()}, {check['diff_hh'].max()}]")
	print(f"HHpop diff range: [{check['diff_hhpop'].min():.1f}, {check['diff_hhpop'].max():.1f}]")
	return check


# ---------------------------------------------------------------------------
# Step entry point
# ---------------------------------------------------------------------------
def run_step(context):
	"""Execute the subregional control totals pipeline step.

	Reads ``split_ct_unrolled`` and ``split_ct_unrolled_regional`` from the
	pipeline, attaches base-year PPH data (from MySQL when ``use_mysql`` is
	true or the cache is missing), applies the borrow distribution for
	small geographies, iterates Larry Blain's formula across years,
	rebalances to controls, and saves the final UrbanSim-format
	``annual_household_control_totals`` and ``annual_employment_control_totals``
	tables to ``pipeline.h5``. Optionally writes CSVs to the output dir.

	Args:
		context (dict): pypyr context (must contain ``'configs_dir'``).

	Returns:
		dict: The unchanged context.
	"""
	pipeline = Pipeline(settings_path=context['configs_dir'])
	cfg = pipeline.settings.get('subregional_cts', {})

	base_year = int(pipeline.settings['base_year'])
	end_year = int(pipeline.settings['end_year'])
	rng_seed = int(cfg.get('rng_seed', 1234))
	max_iter = int(cfg.get('max_outer_iterations', 20))
	min_added = int(cfg.get('min_added_break', 10))
	save_csv = bool(cfg.get('save_csv', True))
	borrow_table = cfg.get('borrow_distribution_table', 'borrow_distribution')

	rng = np.random.default_rng(rng_seed)

	print('Loading inputs...')
	cts = load_unrolled_cts(pipeline, base_year)
	regional = load_regional_unrolled(pipeline, base_year)
	subreg_county = load_subreg_county_xwalk(pipeline)
	bdist = load_borrow_distribution(pipeline, table_name=borrow_table)
	hh_by_pph, mean_pph_subreg, mean_pph_county = load_pph_base_tables(pipeline, base_year)

	print('Building CTpop grid and seeding base-year counts...')
	ctpop = build_ctpop_grid(cts, base_year)
	ctpop = seed_base_year_counts(ctpop, hh_by_pph, mean_pph_subreg, mean_pph_county, subreg_county, base_year)

	last_year = int(cts['year'].max())
	print(f'Applying borrowed distributions (last_year={last_year})...')
	ctpop = apply_borrowed_distribution(ctpop, cts, bdist, base_year, last_year)

	print('Running Larry Blain HH/PPH allocation across years...')
	ctpop = iterate_hhpop_control(ctpop, cts)
	ctpop['hhpop'] = ctpop['mean_pph'] * ctpop['household_count']

	print('Rebalancing to match aggregate controls...')
	ctpop = outer_rebalance(ctpop, cts, rng, max_iterations=max_iter, min_added=min_added)

	check_results(ctpop, cts, base_year)

	print('Building output tables...')
	sub_hh = build_hh_output(ctpop, base_year)
	sub_emp = build_emp_output(cts)
	sub_hh_years = set(sub_hh['year'].unique())
	sub_emp_years = set(sub_emp['year'].unique())
	reg_hh = build_regional_hh_rows(regional, sub_hh_years)
	reg_emp = build_regional_emp_rows(regional, sub_emp_years)

	res_hh = pd.concat([reg_hh, sub_hh], ignore_index=True)
	res_emp = pd.concat([reg_emp, sub_emp], ignore_index=True)

	# Filter to <= end_year as a safety net
	res_hh = res_hh[res_hh['year'] <= end_year].reset_index(drop=True)
	res_emp = res_emp[res_emp['year'] <= end_year].reset_index(drop=True)

	pipeline.save_table('annual_household_control_totals', res_hh)
	pipeline.save_table('annual_employment_control_totals', res_emp)

	if save_csv:
		out_dir = Path(pipeline.get_output_dir())
		out_dir.mkdir(parents=True, exist_ok=True)
		res_hh.to_csv(out_dir / 'annual_household_control_totals.csv', index=False)
		res_emp.to_csv(out_dir / 'annual_employment_control_totals.csv', index=False)
		print(f'Wrote CSV outputs to {out_dir}')

	return context
