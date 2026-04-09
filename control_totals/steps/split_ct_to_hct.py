from datetime import date
from pathlib import Path
import warnings

import numpy as np
import pandas as pd

from util import Pipeline
from steps.create_control_totals_rebased_targets import interpolate_controls_with_anchors, unroll_controls
from steps.load_split_hct_base_data import get_base_data_table_name, load_base_data_from_mysql, maybe_save_base_data


def _series_divide(numerator, denominator, default=0.0):
	"""Element-wise division returning a default where the denominator is zero.

	Args:
		numerator (array-like): Numerator values.
		denominator (array-like): Denominator values.
		default (float, optional): Value to use where division by zero would
			occur. Defaults to 0.0.

	Returns:
		pandas.Series: Result of element-wise division.
	"""
	numerator_values = np.asarray(numerator, dtype=float)
	denominator_values = np.asarray(denominator, dtype=float)
	result = np.full_like(numerator_values, default, dtype=float)
	np.divide(numerator_values, denominator_values, out=result, where=denominator_values != 0)
	return pd.Series(result, index=getattr(numerator, 'index', None))


def _read_control_sheet(workbook_path, sheet_name, base_year, target_year):
	"""Read a single indicator sheet from the control-totals workbook.

	Renames the ``control_id``, base-year, and target-year columns to
	standard names (``nosplit_geo_id``, ``base``, ``target``).

	Args:
		workbook_path (pathlib.Path): Path to the Excel workbook.
		sheet_name (str): Name of the sheet to read.
		base_year (int): The base year whose column to rename.
		target_year (int): The target year whose column to rename.

	Returns:
		pandas.DataFrame: DataFrame with columns ``['nosplit_geo_id',
			'base', 'target']``.
	"""
	frame = pd.read_excel(workbook_path, sheet_name=sheet_name)
	rename_map = {
		'control_id': 'nosplit_geo_id',
		str(base_year): 'base',
		base_year: 'base',
		str(target_year): 'target',
		target_year: 'target',
	}
	frame = frame.rename(columns=rename_map)
	return frame[['nosplit_geo_id', 'base', 'target']].copy()


def load_targets(workbook_path, base_year_in_targets, target_year):
	"""Load HH, Emp, and HHPop targets from the control-totals workbook.

	Also computes derived columns for persons-per-household and population
	growth on the HH target sheet.

	Args:
		workbook_path (pathlib.Path): Path to the Control-Totals-LUVit workbook.
		base_year_in_targets (int): The base year used in the targets.
		target_year (int): The target horizon year.

	Returns:
		tuple:
			dict: Targets dictionary with keys ``'HH'``, ``'Emp'``, ``'HHPop'``.
			dict: Raw control-totals sheets (same keys) before derived columns.
	"""
	ct_hh = _read_control_sheet(workbook_path, 'HH', base_year_in_targets, target_year)
	ct_emp = _read_control_sheet(workbook_path, 'Emp', base_year_in_targets, target_year)
	ct_pop = _read_control_sheet(workbook_path, 'HHPop', base_year_in_targets, target_year)

	targets = {
		'HH': ct_hh.copy(),
		'Emp': ct_emp.copy(),
		'HHPop': ct_pop.copy(),
	}
	hh_growth = targets['HH']['target'] - targets['HH']['base']
	pop_growth = targets['HHPop']['target'] - targets['HHPop']['base']
	targets['HH']['trg_pph'] = _series_divide(pop_growth, hh_growth, default=0).fillna(0)
	targets['HH']['trg_pop'] = targets['HHPop']['target'] - targets['HHPop']['base']
	return targets, {'HH': ct_hh, 'Emp': ct_emp, 'HHPop': ct_pop}


def load_capacity(capacity_path):
	"""Load and aggregate parcel capacity data.

	Reads the parcel-level capacity CSV, computes total capacity columns,
	and aggregates to the split/no-split geography level.

	Args:
		capacity_path (pathlib.Path or str): Path to the capacity CSV.

	Returns:
		tuple:
			pandas.DataFrame: Parcel-level capacity with added
				``DUtotcap`` and ``EMPtotcap`` columns.
			pandas.DataFrame: Geography-level aggregated capacity.
	"""
	pcl_cap = pd.read_csv(capacity_path)
	pcl_cap = pcl_cap.rename(columns={'subreg_id': 'split_geo_id', 'control_id': 'nosplit_geo_id'})
	pcl_cap['DUtotcap'] = np.maximum(pcl_cap['DUbase'], pcl_cap['DUcapacity'])
	pcl_cap['EMPtotcap'] = np.maximum(pcl_cap['JOBSPbase'], pcl_cap['JOBSPcapacity'])
	geo_cap = (
		pcl_cap.groupby(['split_geo_id', 'nosplit_geo_id'], as_index=False)[['DUtotcap', 'EMPtotcap']]
		.sum()
	)
	return pcl_cap, geo_cap


def _load_r_object(path):
	"""Load the first object from an R data file.

	Args:
		path (pathlib.Path or str): Path to the ``.rda``, ``.rdata``, or
			``.rds`` file.

	Returns:
		pandas.DataFrame: The loaded DataFrame.

	Raises:
		ValueError: If the file contains no objects.
	"""
	import pyreadr

	result = pyreadr.read_r(str(path))
	if not result:
		raise ValueError(f'No objects found in {path}')
	return next(iter(result.values())).copy()


def load_base_data_from_file(base_data_path):
	"""Load base-data from a local file in various formats.

	Supports ``.rda`` / ``.rdata`` / ``.rds``, ``.pkl``, ``.parquet``,
	and ``.csv`` files.

	Args:
		base_data_path (pathlib.Path): Path to the base-data file.

	Returns:
		pandas.DataFrame: The loaded base data.

	Raises:
		FileNotFoundError: If the file does not exist.
		ValueError: If the file extension is not supported.
	"""
	if not base_data_path.exists():
		raise FileNotFoundError(f'Base data file not found: {base_data_path}')
	if base_data_path.suffix.lower() in {'.rda', '.rdata', '.rds'}:
		return _load_r_object(base_data_path)
	if base_data_path.suffix.lower() == '.pkl':
		return pd.read_pickle(base_data_path)
	if base_data_path.suffix.lower() == '.parquet':
		return pd.read_parquet(base_data_path)
	if base_data_path.suffix.lower() == '.csv':
		return pd.read_csv(base_data_path)
	raise ValueError(f'Unsupported base data file type: {base_data_path.suffix}')



def load_base_data(pipeline, base_year, use_mysql=False, parcel_base_year=2018, creds_path=None, legacy_base_data_path=None, save_legacy_file=False):
	"""Load or fetch base data for the HCT split step.

	Attempts to retrieve cached base data from the pipeline HDF5 store.
	If ``use_mysql`` is True, fetches fresh data from MySQL instead. Falls
	back to a legacy local file when the HDF5 table is missing.

	Args:
		pipeline (Pipeline): The data pipeline.
		base_year (int): The base year for the table key.
		use_mysql (bool, optional): Whether to query MySQL directly.
			Defaults to False.
		parcel_base_year (int, optional): The parcel base-year database
			year. Defaults to 2018.
		creds_path (pathlib.Path, optional): Path to MySQL credentials.
		legacy_base_data_path (pathlib.Path, optional): Fallback local
			file path.
		save_legacy_file (bool, optional): Whether to persist fetched
			data to a local file. Defaults to False.

	Returns:
		pandas.DataFrame: The base data.

	Raises:
		ValueError: If ``use_mysql`` is True but ``creds_path`` is None.
		KeyError: If no cached or fallback data can be found.
	"""
	table_name = get_base_data_table_name(base_year)
	if use_mysql:
		if creds_path is None:
			raise ValueError('creds_path is required when split_hct_use_mysql is True.')
		base_data = load_base_data_from_mysql(f'{parcel_base_year}_parcel_baseyear', creds_path)
		pipeline.save_table(table_name, base_data)
		if save_legacy_file and legacy_base_data_path is not None:
			maybe_save_base_data(base_data, legacy_base_data_path)
		return base_data

	try:
		return pipeline.get_table(table_name)
	except (KeyError, FileNotFoundError, OSError):
		if legacy_base_data_path is not None and Path(legacy_base_data_path).exists():
			base_data = load_base_data_from_file(Path(legacy_base_data_path))
			pipeline.save_table(table_name, base_data)
			return base_data
		raise KeyError(
			f'Base data table {table_name} not found in pipeline.h5. '
			'Run steps.load_split_hct_base_data or set split_hct_use_mysql=True.'
		)


def prepare_base_data(base_data, ct_sheets):
	"""Enrich base data with group totals and control-totals base-year values.

	Computes within-geography totals for households, persons, and jobs,
	then merges base-year values from the control-totals sheets.

	Args:
		base_data (pandas.DataFrame): Raw base data with per-split-geo
			household, person, and job counts.
		ct_sheets (dict): Control-totals sheets with keys ``'HH'``,
			``'Emp'``, ``'HHPop'``.

	Returns:
		pandas.DataFrame: Enriched base data with group totals and a
			boolean ``is_tod`` column.
	"""
	base_data = base_data.copy()
	base_data['hhtot'] = base_data.groupby('nosplit_geo_id')['households'].transform('sum')
	base_data['poptot'] = base_data.groupby('nosplit_geo_id')['persons'].transform('sum')
	base_data['jobtot'] = base_data.groupby('nosplit_geo_id')['jobs'].transform('sum')

	base_data = base_data.merge(
		ct_sheets['HH'].rename(columns={'base': 'hhtot_base_nosplit'}),
		on='nosplit_geo_id',
		how='left',
	)
	base_data = base_data.merge(
		ct_sheets['HHPop'].rename(columns={'base': 'poptot_base_nosplit'}),
		on='nosplit_geo_id',
		how='left',
	)
	base_data = base_data.merge(
		ct_sheets['Emp'].rename(columns={'base': 'emptot_base_nosplit'}),
		on='nosplit_geo_id',
		how='left',
	)
	base_data['is_tod'] = base_data['split_geo_id'] != base_data['nosplit_geo_id']
	return base_data

def update_control_hct_areas(base_data, parcels_hct):
	"""Update base-data split geographies using parcel-level HCT flags.

	Placeholder for future integration of dynamic parcel-based HCT
	geography updates. Currently a no-op.

	Args:
		base_data (pandas.DataFrame): The enriched base data.
		parcels_hct (geopandas.GeoDataFrame): Parcel-level HCT flags.
	"""
	pass

def create_ct_generators(base_data):
	"""Create per-indicator generator DataFrames for the HCT split.

	Builds HH, Emp, and HHPop generator frames from the enriched base
	data. The HH frame includes persons-per-household ratios used to
	control population distribution.

	Args:
		base_data (pandas.DataFrame): Enriched base data from
			:func:`prepare_base_data`.

	Returns:
		dict: Dictionary with keys ``'HH'``, ``'Emp'``, ``'HHPop'``,
			each containing a pandas.DataFrame.
	"""
	hh = base_data[
		['split_geo_id', 'nosplit_geo_id', 'is_tod', 'RGID', 'name', 'households', 'persons', 'poptot_base_nosplit', 'hhtot_base_nosplit']
	].copy()
	hh = hh.rename(columns={'households': 'base'})
	hh['pph_base'] = _series_divide(hh['persons'], hh['base'], default=0).fillna(0)
	hh['pph_base_nosplit'] = _series_divide(hh['poptot_base_nosplit'], hh['hhtot_base_nosplit'], default=0).fillna(0)
	hh['pph_ratio'] = _series_divide(hh['pph_base'], hh['pph_base_nosplit'], default=0).fillna(0)
	hh.loc[hh['is_tod'], 'pph_ratio'] = hh.loc[hh['is_tod'], 'pph_ratio'].clip(upper=0.9999)
	hh.loc[~hh['is_tod'], 'pph_ratio'] = hh.loc[~hh['is_tod'], 'pph_ratio'].clip(lower=0.0001)

	emp = base_data[['split_geo_id', 'nosplit_geo_id', 'is_tod', 'RGID', 'name', 'jobs']].copy().rename(columns={'jobs': 'base'})
	pop = base_data[['split_geo_id', 'nosplit_geo_id', 'is_tod', 'RGID', 'name', 'persons']].copy().rename(columns={'persons': 'base'})
	return {'HH': hh, 'Emp': emp, 'HHPop': pop}


def merge_with_capacity(df, geo_cap, capacity_prefix):
	"""Merge geography-level capacity data into a generator DataFrame.

	Adds total capacity, net capacity, geography-level net capacity, and
	a capacity share column. Missing capacity is filled with zero; TOD
	areas with no capacity share default to 100%.

	Args:
		df (pandas.DataFrame): Generator DataFrame to merge into.
		geo_cap (pandas.DataFrame): Geography-level aggregated capacity.
		capacity_prefix (str): Column prefix, either ``'DU'`` or ``'EMP'``.

	Returns:
		pandas.DataFrame: The input DataFrame with added capacity columns.
	"""
	capacity_col = f'{capacity_prefix}totcap'
	merged = df.merge(
		geo_cap[['split_geo_id', 'nosplit_geo_id', capacity_col]].rename(columns={capacity_col: 'totcap'}),
		on=['split_geo_id', 'nosplit_geo_id'],
		how='left',
	)
	merged['totcap'] = merged['totcap'].fillna(0)
	merged['netcap'] = np.maximum(0, merged['totcap'] - merged['base'])
	merged['geonetcap'] = merged.groupby('nosplit_geo_id')['netcap'].transform('sum')
	merged['capshare'] = _series_divide(merged['netcap'] * 100, merged['geonetcap'], default=np.nan)
	merged.loc[merged['capshare'].isna() & merged['is_tod'], 'capshare'] = 100
	merged.loc[merged['capshare'].isna() & ~merged['is_tod'], 'capshare'] = 0
	return merged


def _assign_non_tod_from_tod(df, value_col):
	"""Set non-TOD area values as the residual after summing TOD values.

	For each no-split geography that has a TOD split, the non-TOD row's
	value is set to the geography-level target growth minus the TOD value.

	Args:
		df (pandas.DataFrame): Working DataFrame with ``is_tod``,
			``nosplit_geo_id``, and ``trggrowth`` columns.
		value_col (str): Name of the column to update.

	Returns:
		pandas.DataFrame: Updated DataFrame.
	"""
	tod_values = df.loc[df['is_tod']].groupby('nosplit_geo_id')[value_col].sum().rename('_tod_value')
	df = df.merge(tod_values, on='nosplit_geo_id', how='left')
	non_tod_mask = ~df['is_tod'] & df['_tod_value'].notna()
	df.loc[non_tod_mask, value_col] = df.loc[non_tod_mask, 'trggrowth'] - df.loc[non_tod_mask, '_tod_value']
	df[value_col] = df[value_col].fillna(df['trggrowth'])
	return df.drop(columns=['_tod_value'])


def _apply_non_tod_capacity_overflow(df, value_col):
	"""Redirect growth that exceeds non-TOD capacity back to the TOD area.

	When a non-TOD row's assigned growth exceeds its net capacity, the
	excess is moved to the corresponding TOD row within the same geography.

	Args:
		df (pandas.DataFrame): Working DataFrame with ``is_tod``,
			``has_tod``, ``netcap``, and ``nosplit_geo_id`` columns.
		value_col (str): Name of the column to adjust.

	Returns:
		pandas.DataFrame: Updated DataFrame with overflow redistributed.
	"""
	df = df.copy()
	df['overflow'] = 0.0
	mask = (~df['is_tod']) & df['has_tod'] & (df[value_col] > df['netcap'])
	df.loc[mask, 'overflow'] = df.loc[mask, 'netcap'] - df.loc[mask, value_col]
	df.loc[mask, value_col] = df.loc[mask, 'netcap']
	overflow = df.loc[mask].groupby('nosplit_geo_id')['overflow'].sum().rename('_overflow')
	df = df.merge(overflow, on='nosplit_geo_id', how='left')
	df.loc[df['is_tod'] & df['_overflow'].notna(), value_col] = df.loc[df['is_tod'] & df['_overflow'].notna(), value_col] - df.loc[df['is_tod'] & df['_overflow'].notna(), '_overflow']
	return df.drop(columns=['_overflow'])


def _update_hh_population(df):
	"""Iteratively update household population to satisfy PPH constraints.

	Recalculates population from weighted targets and PPH ratios,
	assigns non-TOD population as the residual, and checks that PPH
	does not exceed the maximum allowed. Increases TOD PPH slightly
	for violating geographies and repeats until convergence.

	Args:
		df (pandas.DataFrame): Working HH DataFrame with ``wtrg``,
			``wtrg.pph``, ``has_tod``, ``is_tod``, ``geotrg.pop``, and
			``max.wtrg.pph`` columns.

	Returns:
		pandas.DataFrame: Updated DataFrame with converged population
			and PPH columns.
	"""
	df = df.copy()
	while True:
		df['wtrg.pop'] = df['wtrg'] * np.maximum(1.2, df['wtrg.pph'])
		df.loc[~df['has_tod'], 'wtrg.pop'] = df.loc[~df['has_tod'], 'geotrg.pop']

		tod_pop = df.loc[df['is_tod']].groupby('nosplit_geo_id')['wtrg.pop'].sum().rename('_tod_pop')
		df = df.merge(tod_pop, on='nosplit_geo_id', how='left')
		non_tod_mask = (~df['is_tod']) & df['has_tod'] & df['_tod_pop'].notna()
		df.loc[non_tod_mask, 'wtrg.pop'] = df.loc[non_tod_mask, 'geotrg.pop'] - df.loc[non_tod_mask, '_tod_pop']
		df['wtrg.pop'] = df['wtrg.pop'].fillna(df['geotrg.pop'])
		df = df.drop(columns=['_tod_pop'])

		df['wtrg.pph'] = _series_divide(df['wtrg.pop'], df['wtrg'], default=0).fillna(0)
		violation_mask = (~df['is_tod']) & df['has_tod'] & (df['wtrg.pph'] > df['max.wtrg.pph'])
		if not violation_mask.any():
			return df
		violating_ids = df.loc[violation_mask, 'nosplit_geo_id'].unique()
		df.loc[df['is_tod'] & df['nosplit_geo_id'].isin(violating_ids), 'wtrg.pph'] *= 1.01


def _compute_growth_share(df, value_col):
	"""Compute each row's share of its geography's target growth.

	Args:
		df (pandas.DataFrame): Working DataFrame with ``trggrowth``.
		value_col (str): Column whose values to express as a share.

	Returns:
		pandas.DataFrame: DataFrame with an added ``target.share`` column.
	"""
	df = df.copy()
	df['target.share'] = np.round(_series_divide(df[value_col] * 100, df['trggrowth'], default=0).fillna(0), 1)
	return df


def _regional_share(df, indicator):
	"""Calculate the region-wide TOD share of growth.

	For HH, uses population-weighted growth; for other indicators, uses
	unweighted growth.

	Args:
		df (pandas.DataFrame): Working DataFrame.
		indicator (str): Indicator name (``'HH'``, ``'Emp'``, etc.).

	Returns:
		float: TOD share as a percentage (0–100).
	"""
	if indicator == 'HH':
		numerator = (df.loc[df['is_tod'], 'wtrg'] * df.loc[df['is_tod'], 'wtrg.pph']).sum()
		denominator = (df['wtrg'] * df['wtrg.pph']).sum()
	else:
		numerator = df.loc[df['is_tod'], 'wtrg'].sum()
		denominator = df['wtrg'].sum()
	return 0.0 if denominator == 0 else numerator / denominator * 100


def _regional_share_subset(df, aggr_geo):
	"""Calculate the TOD share of growth excluding aggregated no-growth areas.

	Args:
		df (pandas.DataFrame): Working DataFrame.
		aggr_geo (numpy.ndarray): Array of nosplit_geo_id values that were
			aggregated.

	Returns:
		float: Subset TOD share as a percentage (0–100).
	"""
	denom_mask = df['has_tod'] | ((df['nosplit_geo_id'].isin(aggr_geo)) & (df['RGID'] <= 3))
	numerator = df.loc[df['is_tod'], 'wtrg'].sum()
	denominator = df.loc[denom_mask, 'wtrg'].sum()
	return 0.0 if denominator == 0 else numerator / denominator * 100


def _weights_snapshot(df, indicator, iteration, total_share):
	"""Capture a snapshot of TOD growth weights for diagnostics.

	Returns ``None`` for the HHPop indicator since weights are derived
	from HH.

	Args:
		df (pandas.DataFrame): Working DataFrame.
		indicator (str): Indicator name.
		iteration (int): Current iteration number.
		total_share (float): Current region-wide TOD share.

	Returns:
		pandas.DataFrame or None: Snapshot DataFrame with diagnostics per
			TOD area, or ``None`` for HHPop.
	"""
	if indicator == 'HHPop':
		return None
	weights = df.loc[df['is_tod'], ['nosplit_geo_id', 'RGID', 'name', 'scale', 'target.share', 'wtrg']].copy()
	weights['todcap.share'] = df.loc[df['is_tod'], 'todcap.share'].to_numpy() * 100 if 'todcap.share' in df else np.nan
	weights['incr'] = df.loc[df['is_tod'], 'incr'].to_numpy() if 'incr' in df else np.nan
	weights['remcap'] = df.loc[df['is_tod'], 'true_remcap'].to_numpy() if 'true_remcap' in df else np.nan
	weights['iter'] = iteration
	total_row = pd.DataFrame([
		{
			'nosplit_geo_id': 0,
			'RGID': -1,
			'name': 'Total',
			'todcap.share': np.nan,
			'incr': np.nan,
			'scale': np.nan,
			'target.share': total_share,
			'remcap': np.nan,
			'wtrg': np.nan,
			'iter': iteration,
		}
	])
	return pd.concat([weights, total_row], ignore_index=True, sort=False)


def _aggregate_no_growth_area_rows(df, indicator, aggr_geo):
	"""Collapse TOD and non-TOD rows for no-growth geographies into single rows.

	Geographies identified as no-growth areas are aggregated so that the
	split algorithm does not attempt to redistribute their minimal growth.

	Args:
		df (pandas.DataFrame): Working DataFrame.
		indicator (str): Indicator name (``'HH'``, ``'Emp'``, ``'HHPop'``).
		aggr_geo (numpy.ndarray): Array of ``nosplit_geo_id`` values to
			aggregate.

	Returns:
		pandas.DataFrame: DataFrame with aggregated rows replacing the
			original split rows for the specified geographies.
	"""
	if len(aggr_geo) == 0:
		return df.copy()

	source = df.loc[df['nosplit_geo_id'].isin(aggr_geo)].copy()
	if indicator != 'HHPop':
		aggregated = source.groupby(['nosplit_geo_id', 'RGID', 'name'], as_index=False).agg(
			base=('base', 'sum'),
			totcap=('totcap', 'sum'),
			netcap=('netcap', 'sum'),
			geonetcap=('geonetcap', 'mean'),
			geotottarget_orig=('geotottarget.orig', 'mean'),
		)
		aggregated['is_tod'] = False
		aggregated['capshare'] = 100.0
		aggregated['has_tod'] = False
	else:
		aggregated = source.groupby(['nosplit_geo_id', 'RGID', 'name'], as_index=False).agg(
			base=('base', 'sum'),
			geotottarget_orig=('geotottarget.orig', 'mean'),
		)
		aggregated['is_tod'] = False

	aggregated = aggregated.rename(columns={'geotottarget_orig': 'geotottarget.orig'})

	if indicator == 'HH':
		hh_extra = source.groupby(['nosplit_geo_id', 'RGID', 'name'], as_index=False).agg(
			pph_base=('pph_base_nosplit', 'mean'),
			geotrg_pph=('geotrg.pph', 'mean'),
			geotrg_pop=('geotrg.pop', 'mean'),
		)
		hh_extra = hh_extra.rename(columns={'geotrg_pph': 'geotrg.pph', 'geotrg_pop': 'geotrg.pop'})
		hh_extra['pph_ratio'] = 1.0
		hh_extra['trg.pph'] = hh_extra['geotrg.pph']
		hh_extra['wtrg.pph'] = hh_extra['geotrg.pph']
		hh_extra['is_tod'] = False
		aggregated = aggregated.merge(hh_extra, on=['nosplit_geo_id', 'RGID', 'name', 'is_tod'], how='left')

	aggregated['trggrowth'] = aggregated['geotottarget.orig'] - aggregated['base']
	preserved = df.loc[~df['nosplit_geo_id'].isin(aggr_geo)].copy()
	return pd.concat([preserved, aggregated], ignore_index=True, sort=False)


def split_targets_for_scenario(targets, ct_generators, geo_cap, scenario, trgshare, step_values, aggregate_no_growth_areas, max_iterations):
	"""Run the iterative TOD/non-TOD growth-split algorithm for one scenario.

	For each indicator (HH, Emp, HHPop), distributes growth between TOD and
	non-TOD areas using capacity shares, then iteratively scales TOD
	growth upward until the regional TOD share target is met or
	convergence / max iterations is reached.

	Args:
		targets (dict): Per-indicator target DataFrames.
		ct_generators (dict): Per-indicator generator DataFrames.
		geo_cap (pandas.DataFrame): Geography-level capacity.
		scenario (dict): Per-indicator minimum-share lists by RG.
		trgshare (dict): Regional TOD share targets by indicator.
		step_values (list[float]): Per-RG iteration step sizes.
		aggregate_no_growth_areas (bool): Whether to collapse no-growth
			geographies before splitting.
		max_iterations (int): Maximum number of scaling iterations.

	Returns:
		dict: Dictionary with keys ``'hhres'``, ``'popres'``, ``'empres'``,
			``'check'``, ``'checkdf'``, ``'weights'``.
	"""
	ct_df = {}
	checks = {}
	todshare = {}
	todshare_sub = {}
	weights = {}
	aggr_geo = np.array([], dtype=int)

	for indicator in ['HH', 'Emp', 'HHPop']:
		target_df = targets[indicator].copy()
		target_df['trggrowth'] = target_df['target'] - target_df['base']

		working = ct_generators[indicator].merge(
			target_df[['nosplit_geo_id', 'trggrowth', 'target']].rename(columns={'target': 'geotottarget.orig'}),
			on='nosplit_geo_id',
			how='left',
		)

		if indicator == 'HH':
			working = working.merge(
				target_df[['nosplit_geo_id', 'trg_pph', 'trg_pop']].rename(columns={'trg_pph': 'geotrg.pph', 'trg_pop': 'geotrg.pop'}),
				on='nosplit_geo_id',
				how='left',
			)
			working['trg.pph'] = working['pph_ratio'] * working['geotrg.pph']
			working['wtrg.pph'] = working['trg.pph']

		if aggregate_no_growth_areas:
			if indicator == 'HH':
				aggr_geo = working.loc[
					working['is_tod'] & ((working['trggrowth'] <= 500) | (working['capshare'] == 0) | (working['RGID'] > 3)),
					'nosplit_geo_id',
				].unique()
			elif indicator == 'Emp':
				no_pass = working.loc[
					working['nosplit_geo_id'].isin(aggr_geo)
					& working['is_tod']
					& (working['trggrowth'] > 500)
					& (working['capshare'] > 0)
					& (working['RGID'] <= 3),
					'nosplit_geo_id',
				].unique()
				if len(no_pass) > 0:
					warnings.warn(f'Aggregated employment geographies exceeded filter: {sorted(no_pass.tolist())}')
			working = _aggregate_no_growth_area_rows(working, indicator, aggr_geo)

		if indicator == 'HHPop':
			hh_split = ct_df['HH'][['nosplit_geo_id', 'is_tod', 'has_tod', 'target.share', 'wtrg', 'wtrg.pph']].copy()
			hh_split['pop'] = hh_split['wtrg'] * hh_split['wtrg.pph']
			working = working.merge(
				hh_split[['nosplit_geo_id', 'is_tod', 'has_tod', 'target.share', 'pop']],
				on=['nosplit_geo_id', 'is_tod'],
				how='left',
			)
			working.loc[working['is_tod'], 'wtrg'] = working.loc[working['is_tod'], 'pop']
			working = _assign_non_tod_from_tod(working, 'wtrg')
			working['wtrg'] = working['wtrg'].fillna(working['trggrowth'])
		else:
			tod_ids = set(working.loc[working['is_tod'], 'nosplit_geo_id'])
			working['has_tod'] = True
			working.loc[~working['is_tod'] & ~working['nosplit_geo_id'].isin(tod_ids), 'has_tod'] = False
			working.loc[~working['has_tod'], 'capshare'] = 100.0

			working.loc[working['is_tod'], 'trg0'] = np.minimum(
				working.loc[working['is_tod'], 'netcap'],
				working.loc[working['is_tod'], 'trggrowth'] * working.loc[working['is_tod'], 'capshare'] / 100,
			)
			working = _assign_non_tod_from_tod(working, 'trg0')
			working = _apply_non_tod_capacity_overflow(working, 'trg0')
			working = _compute_growth_share(working, 'trg0')

			working['incr'] = 0.0
			for rgid, step_value in zip([1, 2, 3], step_values):
				working.loc[working['RGID'] == rgid, 'incr'] = step_value

			working['wtrg'] = working['trg0']
			working['scale'] = 0.0
			working['minshare'] = 0.0
			for rgid, minimum in zip([1, 2, 3], scenario[indicator]):
				working.loc[working['RGID'] == rgid, 'minshare'] = minimum
			working.loc[working['geonetcap'] < working['trggrowth'], 'minshare'] = 0.0

			tod_mask = working['is_tod'] & ((100 - working['capshare']) < working['minshare'])
			working.loc[tod_mask, 'minshare'] = np.minimum(
				working.loc[tod_mask, 'minshare'],
				100 - working.loc[tod_mask, 'capshare'],
			)

		if indicator == 'HH':
			working['max.wtrg.pph'] = working['geotrg.pph'] + 0.5
			working = _update_hh_population(working)
			todshare[indicator] = _regional_share(working, indicator)
		else:
			todshare[indicator] = _regional_share(working, indicator)

		if indicator != 'HHPop':
			weights[indicator] = [_weights_snapshot(working, indicator, 0, todshare[indicator])]

		counter = 1
		df = working.copy()
		while indicator != 'HHPop' and todshare[indicator] < trgshare[indicator] and counter <= max_iterations:
			df['remcap'] = np.maximum(0, df['netcap'] - df['wtrg'])
			df['true_remcap'] = df['remcap']
			maxed_out = df['is_tod'] & (np.abs(100 - df['target.share'] - df['minshare']) <= 0.0001)
			df.loc[maxed_out, 'remcap'] = 0

			total_remaining = df.loc[df['is_tod'], 'remcap'].sum()
			df['todcap.share'] = 0.0
			if total_remaining > 0:
				df.loc[df['is_tod'], 'todcap.share'] = df.loc[df['is_tod'], 'remcap'] / total_remaining

			df.loc[df['is_tod'], 'scale'] = df.loc[df['is_tod'], 'scale'] + df.loc[df['is_tod'], 'incr'] * df.loc[df['is_tod'], 'todcap.share']
			max_allowed = np.minimum(1 - df.loc[df['is_tod'], 'minshare'] / 100, (1 + df.loc[df['is_tod'], 'scale']) * df.loc[df['is_tod'], 'capshare'] / 100)
			df.loc[df['is_tod'], 'wtrg'] = np.minimum(df.loc[df['is_tod'], 'netcap'], df.loc[df['is_tod'], 'trggrowth'] * max_allowed)
			df = _assign_non_tod_from_tod(df, 'wtrg')
			df = _apply_non_tod_capacity_overflow(df, 'wtrg')

			previous_share = todshare[indicator]
			if indicator == 'HH':
				df = _update_hh_population(df)
				todshare[indicator] = _regional_share(df, indicator)
				todshare['HH2'] = _regional_share(df.assign(**{'wtrg.pph': 1}), 'Emp')
			else:
				todshare[indicator] = _regional_share(df, indicator)

			df = _compute_growth_share(df, 'wtrg')
			weights[indicator].append(_weights_snapshot(df, indicator, counter, todshare[indicator]))
			df['true_remcap'] = np.maximum(0, df['netcap'] - df['wtrg'])

			if abs(previous_share - todshare[indicator]) <= 0.0001:
				break
			counter += 1

		if counter > max_iterations:
			warnings.warn(f'Max iterations reached for {indicator} before target share was achieved.')

		todshare_sub[indicator] = _regional_share_subset(df, aggr_geo)
		df['tottrg.final'] = df['base'] + df['wtrg']
		df.loc[~df['has_tod'], 'target.share'] = 100.0
		df['geotottarget.final'] = df.groupby('nosplit_geo_id')['tottrg.final'].transform('sum')
		df['trgdif'] = df['geotottarget.final'] - df['geotottarget.orig']
		df = df.sort_values(['nosplit_geo_id', 'is_tod'], ascending=[True, False]).reset_index(drop=True)

		rg_tod = df.loc[df['is_tod']].groupby('RGID', as_index=False)['wtrg'].sum()
		rg_total = df.loc[df['RGID'].isin([1, 2, 3])].groupby('RGID', as_index=False)['wtrg'].sum().rename(columns={'wtrg': 'total_wtrg'})
		tod_by_rg = rg_tod.merge(rg_total, on='RGID', how='left')
		tod_by_rg['share'] = _series_divide(tod_by_rg['wtrg'] * 100, tod_by_rg['total_wtrg'], default=0).fillna(0)

		checks[indicator] = tod_by_rg.copy()
		ct_df[indicator] = df

	hhres = ct_df['HH'][
		['split_geo_id', 'nosplit_geo_id', 'RGID', 'name', 'is_tod', 'totcap', 'netcap', 'capshare', 'target.share', 'trg0', 'wtrg', 'base', 'pph_base']
	].copy()
	hhres = hhres.rename(
		columns={
			'split_geo_id': 'subreg_id',
			'totcap': 'DUtotcapacity',
			'netcap': 'DUnetcapacity',
			'capshare': 'DUcapshare',
			'trg0': 'target_growth_ini',
			'wtrg': 'target_growth_final',
			'base': 'HHbase',
			'pph_base': 'PPHbase',
		}
	)
	hhres['is_tod'] = hhres['is_tod'].astype(int)
	hhres['HHtarget'] = np.rint(ct_df['HH']['tottrg.final']).astype(int)
	hhres['PPHtarget'] = _series_divide(ct_df['HHPop']['tottrg.final'], ct_df['HH']['tottrg.final'], default=0).fillna(0)
	hhres['DUtotcapacity'] = np.rint(hhres['DUtotcapacity']).astype(int)
	hhres['DUnetcapacity'] = np.rint(hhres['DUnetcapacity']).astype(int)
	hhres['DUcapshare'] = hhres['DUcapshare'].round(1)
	hhres['target_share'] = hhres['target.share'].round(1)
	hhres['target_growth_ini'] = np.rint(hhres['target_growth_ini']).astype(int)
	hhres['target_growth_final'] = np.rint(hhres['target_growth_final']).astype(int)
	hhres = hhres.drop(columns=['target.share'])

	popres = ct_df['HHPop'][['split_geo_id', 'nosplit_geo_id', 'RGID', 'name', 'is_tod', 'target.share', 'wtrg', 'base']].copy()
	popres = popres.rename(
		columns={
			'split_geo_id': 'subreg_id',
			'wtrg': 'target_growth_final',
			'base': 'HHPopbase',
		}
	)
	popres['is_tod'] = popres['is_tod'].astype(int)
	popres['target_share'] = popres['target.share'].round(1)
	popres['target_growth_final'] = np.rint(popres['target_growth_final']).astype(int)
	popres['HHPoptarget'] = np.rint(ct_df['HHPop']['tottrg.final']).astype(int)
	popres = popres.drop(columns=['target.share'])

	empres = ct_df['Emp'][['split_geo_id', 'nosplit_geo_id', 'RGID', 'name', 'is_tod', 'totcap', 'netcap', 'capshare', 'target.share', 'trg0', 'wtrg', 'base']].copy()
	empres = empres.rename(
		columns={
			'split_geo_id': 'subreg_id',
			'totcap': 'EMPtotcapacity',
			'netcap': 'EMPnetcapacity',
			'capshare': 'EMPcapshare',
			'trg0': 'target_growth_ini',
			'wtrg': 'target_growth_final',
			'base': 'Empbase',
		}
	)
	empres['is_tod'] = empres['is_tod'].astype(int)
	empres['Emptarget'] = np.rint(ct_df['Emp']['tottrg.final']).astype(int)
	empres['EMPtotcapacity'] = np.rint(empres['EMPtotcapacity']).astype(int)
	empres['EMPnetcapacity'] = np.rint(empres['EMPnetcapacity']).astype(int)
	empres['EMPcapshare'] = empres['EMPcapshare'].round(1)
	empres['target_share'] = empres['target.share'].round(1)
	empres['target_growth_ini'] = np.rint(empres['target_growth_ini']).astype(int)
	empres['target_growth_final'] = np.rint(empres['target_growth_final']).astype(int)
	empres = empres.drop(columns=['target.share'])

	checkdf = (
		hhres.groupby('nosplit_geo_id', as_index=False)['HHtarget'].sum().rename(columns={'HHtarget': 'HH'})
		.merge(popres.groupby('nosplit_geo_id', as_index=False)['HHPoptarget'].sum().rename(columns={'HHPoptarget': 'Pop'}), on='nosplit_geo_id')
		.merge(targets['HH'][['nosplit_geo_id', 'target']].rename(columns={'target': 'HHtrg'}), on='nosplit_geo_id')
		.merge(targets['HHPop'][['nosplit_geo_id', 'target']].rename(columns={'target': 'Poptrg'}), on='nosplit_geo_id')
	)

	check = (
		checks['HH'][['RGID', 'share']].rename(columns={'share': 'tod_share_hh'})
		.merge(checks['HHPop'][['RGID', 'share']].rename(columns={'share': 'tod_share_pop'}), on='RGID', how='outer')
		.merge(checks['Emp'][['RGID', 'share']].rename(columns={'share': 'tod_share_emp'}), on='RGID', how='outer')
	)
	check = check[check['RGID'].isin([1, 2, 3])].copy()
	check = pd.concat(
		[
			check,
			pd.DataFrame([
				{
					'RGID': -2,
					'tod_share_hh': round(todshare_sub['HH'], 2),
					'tod_share_pop': round(todshare_sub['HHPop'], 2),
					'tod_share_emp': round(todshare_sub['Emp'], 2),
				},
				{
					'RGID': -1,
					'tod_share_hh': round(todshare.get('HH2', 0.0), 2),
					'tod_share_pop': round(todshare['HHPop'], 2),
					'tod_share_emp': round(todshare['Emp'], 2),
				},
			]),
		],
		ignore_index=True,
	)
	check['RGID'] = check['RGID'].replace({-2: 'Tot within RG', -1: 'Total'}).astype(str)

	return {
		'hhres': hhres,
		'popres': popres,
		'empres': empres,
		'check': check,
		'checkdf': checkdf,
		'weights': {key: pd.concat(value, ignore_index=True) for key, value in weights.items()},
	}


def build_interpolated_outputs(hhres, popres, empres, check, base_year, base_year_in_targets, target_year,
							 round_interpolated=False, stepped_years=None):
	"""Interpolate split results into stepped and annual control-totals sheets.

	Produces interpolated control totals at the configured stepped years, an
	unrolled long-format table, and an annual regional summary.

	Args:
		hhres (pandas.DataFrame): Household split results.
		popres (pandas.DataFrame): Population split results.
		empres (pandas.DataFrame): Employment split results.
		check (pandas.DataFrame): TOD share check table.
		base_year (int): The base year.
		base_year_in_targets (int): The base year used in the targets.
		target_year (int): The target horizon year.
		round_interpolated (bool, optional): Whether to round interpolated
			values. Defaults to False.
		stepped_years (list[int], optional): Explicit list of years for
			stepped output. When ``None``, defaults to 5-year intervals
			from *base_year* through *target_year*, plus the base year
			used in targets.

	Returns:
		dict: Dictionary of DataFrames keyed by indicator name plus
			``'unrolled'`` and ``'unrolled regional'`` entries.
	"""
	hh_work = hhres.rename(
		columns={
			'HHbase': f'HH{base_year_in_targets}',
			'HHtarget': f'HH{target_year}',
			'PPHbase': f'PPH{base_year_in_targets}',
			'PPHtarget': f'PPH{target_year}',
		}
	)
	hhpop_work = popres.rename(columns={'HHPopbase': f'HHPop{base_year_in_targets}', 'HHPoptarget': f'HHPop{target_year}'})
	emp_work = empres.rename(columns={'Empbase': f'Emp{base_year_in_targets}', 'Emptarget': f'Emp{target_year}'})

	to_interpolate = {'HHPop': hhpop_work, 'HH': hh_work, 'Emp': emp_work}
	cts = {'HHwork': hh_work, 'HHPopwork': hhpop_work, 'EMPwork': emp_work, 'check': check}
	if stepped_years is None:
		stepped_years = [base_year_in_targets, *range(base_year, target_year + 1, 5)]
		if target_year not in stepped_years:
			stepped_years.append(target_year)
	stepped_years = sorted(set(stepped_years))

	unrolled = None
	for indicator, frame in to_interpolate.items():
		cts[indicator] = interpolate_controls_with_anchors(
			frame,
			indicator,
			anchor_years=sorted({base_year_in_targets, target_year}),
			years_to_fit=stepped_years,
			id_col='subreg_id',
			round_interpolated=round_interpolated,
		)
		current = unroll_controls(cts[indicator], indicator, new_id_col='subreg_id')
		unrolled = current if unrolled is None else unrolled.merge(current, on=['subreg_id', 'year'], how='outer')
	cts['unrolled'] = unrolled

	annual_years = list(range(base_year, target_year + 1))
	annual_regional = None
	anchors = sorted({base_year, base_year_in_targets, target_year})
	for indicator, frame in to_interpolate.items():
		regional_columns = [f'{indicator}{year}' for year in anchors]
		regional = frame[regional_columns].sum().to_frame().T
		regional.insert(0, 'subreg_id', -1)
		reg_intp = interpolate_controls_with_anchors(
			regional,
			indicator,
			anchor_years=anchors,
			years_to_fit=annual_years,
			id_col='subreg_id',
			round_interpolated=round_interpolated,
		)
		current = unroll_controls(reg_intp, indicator, new_id_col='subreg_id')
		annual_regional = current if annual_regional is None else annual_regional.merge(current, on=['subreg_id', 'year'], how='outer')
	cts['unrolled regional'] = annual_regional
	return cts


def write_workbook(sheets, output_path):
	"""Write a dictionary of DataFrames to an Excel workbook.

	Creates parent directories if they do not exist.

	Args:
		sheets (dict[str, pandas.DataFrame]): Mapping of sheet names to
			DataFrames.
		output_path (pathlib.Path): Destination file path for the workbook.
	"""
	output_path.parent.mkdir(parents=True, exist_ok=True)
	with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
		for sheet_name, frame in sheets.items():
			frame.to_excel(writer, sheet_name=sheet_name, index=False)


def save_pipeline_outputs(pipeline, cts, scenario_suffix):
	"""Persist split control-totals tables to the pipeline HDF5 store.

	Args:
		pipeline (Pipeline): The data pipeline providing the save interface.
		cts (dict): Interpolated control-totals DataFrames keyed by
			indicator name.
		scenario_suffix (str): Suffix appended to each table key to
			distinguish scenarios.
	"""
	suffix = '' if scenario_suffix == 'default' else f'_{scenario_suffix}'
	pipeline.save_table(f'split_ct_hh_work{suffix}', cts['HHwork'])
	pipeline.save_table(f'split_ct_hhpop_work{suffix}', cts['HHPopwork'])
	pipeline.save_table(f'split_ct_emp_work{suffix}', cts['EMPwork'])
	pipeline.save_table(f'split_ct_check{suffix}', cts['check'])
	pipeline.save_table(f'split_ct_hh{suffix}', cts['HH'])
	pipeline.save_table(f'split_ct_hhpop{suffix}', cts['HHPop'])
	pipeline.save_table(f'split_ct_emp{suffix}', cts['Emp'])
	pipeline.save_table(f'split_ct_unrolled{suffix}', cts['unrolled'])
	pipeline.save_table(f'split_ct_unrolled_regional{suffix}', cts['unrolled regional'])


def run_step(context):
	"""Execute the control-totals HCT split pipeline step.

	Loads targets and capacity data, prepares base data, runs the
	iterative TOD/non-TOD split for each configured scenario, interpolates
	results, writes Excel workbooks, and persists outputs to the pipeline.

	Expected settings.yaml block::

		split_hct:
		  base_year_in_targets: null
		  parcel_base_year: 2018
		  controls_file: Control-Totals-LUVit.xlsx
		  capacity_file: CapacityPclNoSampling_res50.csv
		  base_data_file: inputs/base_data_shares_2020.rda
		  creds_file: creds.txt
		  use_mysql: false
		  save_base_data_file: false
		  aggregate_no_growth_areas: false
		  round_interpolated: false
		  save_results: true
		  max_iterations: 2000
		  stepped_years: null
		  trgshare:
		    HH: 65
		    Emp: 75
		  scenarios:
		    - HH: [10, 10, 10]
		      Emp: [10, 10, 10]
		  step_values: [1, 0.5, 0.25]

	Args:
		context (dict): The pypyr context dictionary, expected to contain
			a ``'configs_dir'`` key.

	Returns:
		dict: The unchanged pypyr context dictionary.
	"""
	pipeline = Pipeline(settings_path=context['configs_dir'])
	settings = pipeline.settings
	cfg = settings.get('split_hct', {})

	base_year = int(settings.get('base_year', 2020))
	base_year_in_targets = int(cfg['base_year_in_targets']) if cfg.get('base_year_in_targets') else base_year
	target_year = int(settings.get('end_year', 2050))
	parcel_base_year = int(cfg.get('parcel_base_year', 2018))
	aggregate_no_growth_areas = bool(cfg.get('aggregate_no_growth_areas', False))
	round_interpolated = bool(cfg.get('round_interpolated', False))
	save_results = bool(cfg.get('save_results', True))
	max_iterations = int(cfg.get('max_iterations', 2000))
	stepped_years = cfg.get('stepped_years', None)

	data_dir = Path(pipeline.get_data_dir())
	output_dir = Path(pipeline.get_output_dir())

	workbook_path = output_dir / cfg.get('controls_file', 'Control-Totals-LUVit.xlsx')
	capacity_path = output_dir / cfg.get('capacity_file', 'CapacityPclNoSampling_res50.csv')
	base_data_path = data_dir / cfg.get('base_data_file', f'inputs/base_data_shares_{base_year}.rda')
	creds_path = data_dir / cfg.get('creds_file', 'creds.txt')

	trgshare_cfg = cfg.get('trgshare', {})
	trgshare = {'HH': trgshare_cfg.get('HH', 65), 'Emp': trgshare_cfg.get('Emp', 75), 'HHPop': None}
	scenarios_cfg = cfg.get('scenarios', [{'HH': [10, 10, 10], 'Emp': [10, 10, 10]}])
	scenarios = [{'HH': s.get('HH', [10, 10, 10]), 'Emp': s.get('Emp', [10, 10, 10]), 'HHPop': None} for s in scenarios_cfg]
	step_values = cfg.get('step_values', [1, 0.5, 0.25])

	targets, ct_sheets = load_targets(workbook_path, base_year_in_targets, target_year)
	_, geo_cap = load_capacity(capacity_path)

	use_mysql = bool(cfg.get('use_mysql', False))
	base_data = load_base_data(
		pipeline,
		base_year=base_year,
		use_mysql=use_mysql,
		parcel_base_year=parcel_base_year,
		creds_path=creds_path,
		legacy_base_data_path=base_data_path,
		save_legacy_file=bool(cfg.get('save_base_data_file', False)),
	)
	
	# parcels_hct = pipeline.load_geodataframe('parcels_hct')
	# base_data = update_control_hct_areas(base_data, parcels_hct)
	
	base_data = prepare_base_data(base_data, ct_sheets)
	ct_generators = create_ct_generators(base_data)
	ct_generators['HH'] = merge_with_capacity(ct_generators['HH'], geo_cap, 'DU')
	ct_generators['Emp'] = merge_with_capacity(ct_generators['Emp'], geo_cap, 'EMP')

	today = date.today().isoformat()
	for scenario in scenarios:
		split_result = split_targets_for_scenario(
			targets={key: value.copy() for key, value in targets.items()},
			ct_generators={key: value.copy() for key, value in ct_generators.items()},
			geo_cap=geo_cap,
			scenario=scenario,
			trgshare=trgshare,
			step_values=step_values,
			aggregate_no_growth_areas=aggregate_no_growth_areas,
			max_iterations=max_iterations,
		)

		cts = build_interpolated_outputs(
			split_result['hhres'],
			split_result['popres'],
			split_result['empres'],
			split_result['check'],
			base_year=base_year,
			base_year_in_targets=base_year_in_targets,
			target_year=target_year,
			round_interpolated=round_interpolated,
			stepped_years=stepped_years,
		)

		scenario_suffix = '-'.join(str(100 - int(value)) for value in scenario['HH'])
		if save_results:
			output_path = output_dir / f'LUVit_ct_by_tod_generator-{today}_{scenario_suffix}.xlsx'
			write_workbook(cts, output_path)

		save_pipeline_outputs(pipeline, cts, scenario_suffix if len(scenarios) > 1 else 'default')

	return context
