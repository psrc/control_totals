import re
from pathlib import Path

import numpy as np
import pandas as pd

from util import Pipeline


def _normalize_year(year_token):
	"""Normalize a two- or four-digit year token to a four-digit integer.

	Args:
		year_token (str or int): A year value that may be two or four digits.

	Returns:
		int: The four-digit year.
	"""
	year = int(year_token)
	return 2000 + year if year < 100 else year


def _extract_years(columns, pattern):
	"""Extract and normalize year values from column names matching a regex.

	Args:
		columns (pandas.Index or list[str]): Column names to scan.
		pattern (re.Pattern): Compiled regex with one capture group for a year.

	Returns:
		list[int]: Sorted unique four-digit years found in the column names.
	"""
	years = []
	for column in columns:
		match = pattern.match(column)
		if match:
			years.append(_normalize_year(match.group(1)))
	return sorted(set(years))


def _infer_years(city_data):
	"""Infer the REF base year, base year, target year, and intermediate years.

	Scans for ``Pop##`` (REF base year) and ``TotPop##`` (base and
	target years) columns in the input city data. Any years between the
	first and last ``TotPop##`` are returned as intermediate anchors.

	Args:
		city_data (pandas.DataFrame): The raw city-level control totals input.

	Returns:
		tuple[int, int, int, list[int]]: ``(ref_base_year, base_year,
		target_year, intermediate_target_years)``.

	Raises:
		ValueError: If the required year columns cannot be found.
	"""
	ref_years = _extract_years(city_data.columns, re.compile(r'^Pop(\d{2,4})$'))
	target_years = _extract_years(city_data.columns, re.compile(r'^TotPop(\d{2,4})$'))

	if not ref_years or len(target_years) < 2:
		raise ValueError('Unable to infer rebasing years from control totals input columns.')

	return ref_years[0], target_years[0], target_years[-1], target_years[1:-1]


def load_city_data(pipeline):
	"""Load the city-level control totals input file.

	Reads the filename from ``rebased_targets.input_file`` in settings
	(defaults to ``control_id_working.xlsx``) inside the pipeline data
	directory.

	Args:
		pipeline (Pipeline): The data pipeline providing path helpers
			and settings.

	Returns:
		pandas.DataFrame: The loaded city-level data.

	Raises:
		FileNotFoundError: If the input file does not exist.
	"""
	cfg = pipeline.settings.get('rebased_targets', {})
	input_name = cfg.get('input_file', 'control_id_working.xlsx')
	input_path = Path(pipeline.get_data_dir()) / input_name

	if not input_path.exists():
		raise FileNotFoundError(
			f'Rebased targets input not found: {input_path}. '
			'Run the create_controls step first or provide rebased_targets_input_file in context.'
		)

	if input_path.suffix.lower() == '.csv':
		return pd.read_csv(input_path)
	return pd.read_excel(input_path)


def build_rebased_targets(city_data, ref_base_year, base_year, target_year, intermediate_target_years=None):
	"""Build rebased growth targets from raw city-level control-totals input.

	Extracts and renames employment and population columns for the REF
	base year, base year, and target year, computes household population and
	households from PPH and GQ shares, and summarizes growth at the RG level.

	Args:
		city_data (pandas.DataFrame): The raw city-level control totals input.
		ref_base_year (int): The REF (Regional Economic Forecast) base year (e.g. 2018).
		base_year (int): The rebase base year (e.g. 2020).
		target_year (int): The target horizon year (e.g. 2050).

	Returns:
		dict: A dictionary with keys ``'RGs'``, ``'CityPop'``, ``'CityHH'``,
			and ``'CityEmp'``, each containing a pandas.DataFrame.
	"""
	ref_year_short = str(ref_base_year)[-2:]
	base_year_short = str(base_year)[-2:]
	target_year_short = str(target_year)[-2:]

	city_gro_emp = city_data[
		[
			'county_id',
			'RGID',
			'control_id',
			f'Emp{ref_year_short}',
			f'TotEmp{base_year_short}_wCRnoMil',
			'TotEmpTrg_wCRnoMil',
			f'TotEmp{target_year_short}_wCRnoMil',
		]
	].rename(
		columns={
			f'Emp{ref_year_short}': 'EmpBY',
			f'TotEmp{base_year_short}_wCRnoMil': 'EmpBase',
			'TotEmpTrg_wCRnoMil': 'EmpGro',
			f'TotEmp{target_year_short}_wCRnoMil': 'EmpTarget',
		}
	)

	pop_source_columns = [
		'county_id',
		'RGID',
		'control_id',
		f'Pop{ref_year_short}',
		f'TotPop{base_year_short}',
		f'HHpop{ref_year_short}',
		f'HHpop{base_year_short}',
		f'HH{ref_year_short}',
		f'HH{base_year_short}',
		'TotPopTrg',
		f'TotPop{target_year_short}',
		f'GQpct{target_year_short}',
		f'PPH{target_year_short}',
	]
	# Deduplicate while preserving order; needed when ref_base_year == base_year.
	pop_source_columns = list(dict.fromkeys(pop_source_columns))
	city_gro_pop = city_data[pop_source_columns].copy()

	# When the REF base year and rebase base year are the same, the same source
	# column maps to two output names (e.g. 'HHpop23' -> both 'HHPopBY' and 'HHPopBase').
	# Apply renames sequentially, duplicating columns as needed.
	rename_pairs = [
		(f'Pop{ref_year_short}', 'PopBY'),
		(f'TotPop{base_year_short}', 'PopBase'),
		(f'HHpop{ref_year_short}', 'HHPopBY'),
		(f'HHpop{base_year_short}', 'HHPopBase'),
		(f'HH{ref_year_short}', 'HHBY'),
		(f'HH{base_year_short}', 'HHBase'),
		('TotPopTrg', 'PopGro'),
		(f'TotPop{target_year_short}', 'PopTarget'),
		(f'GQpct{target_year_short}', 'GQpctTarget'),
		(f'PPH{target_year_short}', 'PPHTarget'),
	]
	already_renamed = {}
	for source, target in rename_pairs:
		if source in already_renamed:
			# Source has already been renamed; copy from its new name.
			city_gro_pop[target] = city_gro_pop[already_renamed[source]]
		else:
			city_gro_pop = city_gro_pop.rename(columns={source: target})
			already_renamed[source] = target

	city_gro_pop['HHPopTarget'] = city_gro_pop['PopTarget'] - city_gro_pop['PopTarget'] * city_gro_pop['GQpctTarget'] / 100
	city_gro_pop['HHTarget'] = city_gro_pop['HHPopTarget'] / city_gro_pop['PPHTarget']
	city_gro_pop['HHTarget'] = city_gro_pop['HHTarget'].replace([np.inf, -np.inf], np.nan).fillna(0)
	city_gro_pop['HHGro'] = city_gro_pop['HHTarget'] - city_gro_pop['HHBase']
	city_gro_pop['HHPopGro'] = city_gro_pop['HHPopTarget'] - city_gro_pop['HHPopBase']

	juris = city_data[['control_id', 'name']].drop_duplicates().rename(columns={'name': 'Juris'})
	city_gro_emp = city_gro_emp.merge(juris, on='control_id', how='left')
	city_gro_pop = city_gro_pop.merge(juris, on='control_id', how='left')

	rgs_target = city_gro_pop.groupby(['county_id', 'RGID'], as_index=False).agg(
		PopDelta=('PopTarget', lambda values: values.sum()),
		PopBase=('PopBase', 'sum'),
		PopTarget=('PopTarget', 'sum'),
		HHDelta=('HHTarget', lambda values: values.sum()),
		HHBase=('HHBase', 'sum'),
		HHTarget=('HHTarget', 'sum'),
	)
	rgs_target['PopDelta'] = rgs_target['PopTarget'] - rgs_target['PopBase']
	rgs_target['HHDelta'] = rgs_target['HHTarget'] - rgs_target['HHBase']
	rgs_target = rgs_target.drop(columns=['PopBase', 'HHBase'])

	emp_target = city_gro_emp.groupby(['county_id', 'RGID'], as_index=False).agg(
		EmpBase=('EmpBase', 'sum'),
		EmpTarget=('EmpTarget', 'sum'),
	)
	emp_target['EmpDelta'] = emp_target['EmpTarget'] - emp_target['EmpBase']
	emp_target = emp_target.drop(columns=['EmpBase'])

	rgs_target = rgs_target.merge(emp_target, on=['county_id', 'RGID'], how='inner')

	city_rgs_emp = city_gro_emp[['RGID', 'county_id', 'control_id', 'Juris', 'EmpBY', 'EmpBase', 'EmpGro', 'EmpTarget']].copy()
	city_rgs_pop = city_gro_pop[
		['RGID', 'county_id', 'control_id', 'Juris', 'PopBY', 'PopBase', 'PopGro', 'PopTarget', 'HHPopBY', 'HHPopBase', 'HHPopGro', 'HHPopTarget']
	].copy()
	city_rgs_hh = city_gro_pop[['RGID', 'county_id', 'control_id', 'Juris', 'HHBY', 'HHBase', 'HHGro', 'HHTarget']].copy()

	target_year_short = str(target_year)[-2:]
	growth_suffix = f"{str(base_year)[-2:]}{target_year_short}"

	# When ref_base_year == base_year, the BY and Base columns are identical;
	# drop the BY duplicates so the rename below doesn't produce duplicate column labels.
	if ref_base_year == base_year:
		city_rgs_pop = city_rgs_pop.drop(columns=['PopBY', 'HHPopBY'])
		city_rgs_emp = city_rgs_emp.drop(columns=['EmpBY'])
		city_rgs_hh = city_rgs_hh.drop(columns=['HHBY'])

	pop_rename = {
		'PopBase': f'Pop{base_year}',
		'PopGro': f'PopGro{growth_suffix}',
		'PopTarget': f'Pop{target_year}',
		'HHPopBase': f'HHPop{base_year}',
		'HHPopGro': f'HHPopGro{growth_suffix}',
		'HHPopTarget': f'HHPop{target_year}',
	}
	emp_rename = {
		'EmpBase': f'Emp{base_year}',
		'EmpGro': f'EmpGro{growth_suffix}',
		'EmpTarget': f'Emp{target_year}',
	}
	hh_rename = {
		'HHBase': f'HH{base_year}',
		'HHGro': f'HHGro{growth_suffix}',
		'HHTarget': f'HH{target_year}',
	}
	if ref_base_year != base_year:
		pop_rename['PopBY'] = f'Pop{ref_base_year}'
		pop_rename['HHPopBY'] = f'HHPop{ref_base_year}'
		emp_rename['EmpBY'] = f'Emp{ref_base_year}'
		hh_rename['HHBY'] = f'HH{ref_base_year}'

	city_rgs_pop = city_rgs_pop.rename(columns=pop_rename)
	city_rgs_emp = city_rgs_emp.rename(columns=emp_rename)
	city_rgs_hh = city_rgs_hh.rename(columns=hh_rename)

	# Append intermediate-anchor-year columns so interpolation honors them as anchors.
	# Values are taken directly from the input file (no recomputation from PPH/GQpct).
	for intermediate_year in (intermediate_target_years or []):
		short = str(intermediate_year)[-2:]
		extras = city_data[
			['control_id', f'TotPop{short}', f'HHpop{short}', f'HH{short}', f'TotEmp{short}_wCRnoMil']
		].rename(
			columns={
				f'TotPop{short}': f'Pop{intermediate_year}',
				f'HHpop{short}': f'HHPop{intermediate_year}',
				f'HH{short}': f'HH{intermediate_year}',
				f'TotEmp{short}_wCRnoMil': f'Emp{intermediate_year}',
			}
		)
		city_rgs_pop = city_rgs_pop.merge(
			extras[['control_id', f'Pop{intermediate_year}', f'HHPop{intermediate_year}']],
			on='control_id', how='left',
		)
		city_rgs_hh = city_rgs_hh.merge(
			extras[['control_id', f'HH{intermediate_year}']], on='control_id', how='left',
		)
		city_rgs_emp = city_rgs_emp.merge(
			extras[['control_id', f'Emp{intermediate_year}']], on='control_id', how='left',
		)
	rgs_target = rgs_target.rename(
		columns={
			'PopDelta': f'Pop{growth_suffix}',
			'PopTarget': f'Pop{target_year_short}',
			'HHDelta': f'HH{growth_suffix}',
			'HHTarget': f'HH{target_year_short}',
			'EmpDelta': f'Emp{growth_suffix}',
			'EmpTarget': f'Emp{target_year_short}',
		}
	)

	return {'RGs': rgs_target, 'CityPop': city_rgs_pop, 'CityHH': city_rgs_hh, 'CityEmp': city_rgs_emp}


REF_INDICATOR_MAP = {
	'Pop': 'Tot Pop',
	'HHPop': 'HH Pop',
	'HH': 'HH',
	'Emp': 'Total Emp w/o Enlisted',
}


def _resolve_scale_mode(value):
	"""Normalize the ``scale_to_ref`` setting to ``'none'``, ``'region'``, or ``'county'``.

	Accepts the new string enum (``none``/``region``/``county``) and falls
	back to the legacy boolean form (``True`` -> ``region``, ``False`` ->
	``none``) for backward compatibility.

	Args:
		value: The raw setting value.

	Returns:
		str: ``'none'``, ``'region'``, or ``'county'``.

	Raises:
		ValueError: If *value* is not one of the recognized options.
	"""
	if value is None or value is False:
		return 'none'
	if value is True:
		return 'region'
	if isinstance(value, str):
		normalized = value.strip().lower()
		if normalized in ('none', 'region', 'county'):
			return normalized
	raise ValueError(
		f"rebased_targets.scale_to_ref must be one of: none, region, county. Got {value!r}."
	)


def load_regional_totals(pipeline, base_year):
	"""Load Regional Economic Forecast (REF) projection totals for optional scaling.

	Returns ``None`` when scaling is disabled via
	``rebased_targets.scale_to_ref`` or no REF projection years are
	available. The returned shape depends on the configured scaling mode:

	* ``region`` -> ``dict[str, pandas.Series]`` keyed by indicator, each
	  Series indexed by year string. Reads from the ``ref_projection``
	  data table, which must be in the wide format (a ``variable`` column
	  plus one column per year).
	* ``county`` -> ``dict[str, pandas.DataFrame]`` keyed by indicator,
	  each DataFrame indexed by ``county_id`` with year-string columns.
	  Reads from the ``ref_projection_by_county`` data table, which must
	  be in the long format with ``county_id`` and ``year`` columns plus
	  one column per indicator.

	Args:
		pipeline (Pipeline): The data pipeline providing access to settings
			and stored tables.
		base_year (int): The rebase base year. The base year is retained in
			the loader output so that downstream interpolation has a left
			anchor; :func:`_expand_ref_totals` removes it from the result
			so it is not used to rescale base-year totals.

	Returns:
		dict or None: A dictionary keyed by indicator name (``'Pop'``,
		``'HHPop'``, ``'HH'``, ``'Emp'``), or ``None`` if scaling is
		disabled or no usable REF data is available.

	Raises:
		ValueError: If the configured mode is unrecognized, or if the REF
			table shape does not match the requested mode.
	"""
	cfg = pipeline.settings.get('rebased_targets', {})
	mode = _resolve_scale_mode(cfg.get('scale_to_ref'))
	if mode == 'none':
		return None

	table_name = 'ref_projection' if mode == 'region' else 'ref_projection_by_county'
	ref_projection = pipeline.get_table(table_name).copy()

	if mode == 'region':
		numeric_columns = [column for column in ref_projection.columns if str(column).isdigit()]
		if not numeric_columns:
			return None
		if 'variable' not in ref_projection.columns:
			raise ValueError(
				"scale_to_ref: region requires the ref_projection table to have a 'variable' column "
				"and one column per year (wide format)."
			)
		totals = {}
		for indicator, variable_name in REF_INDICATOR_MAP.items():
			matches = ref_projection.loc[ref_projection['variable'] == variable_name, numeric_columns]
			if matches.empty:
				continue
			series = matches.iloc[0].astype(float)
			series.index = series.index.astype(str)
			totals[indicator] = series
		return totals or None

	# mode == 'county'
	if 'county_id' not in ref_projection.columns or 'year' not in ref_projection.columns:
		raise ValueError(
			"scale_to_ref: county requires the ref_projection table to have 'county_id' and 'year' "
			"columns (long format)."
		)
	df = ref_projection.copy()
	df['year'] = df['year'].astype(int)
	if df.empty:
		return None
	totals = {}
	for indicator, column_name in REF_INDICATOR_MAP.items():
		if column_name not in df.columns:
			continue
		pivot = df.pivot_table(index='county_id', columns='year', values=column_name, aggfunc='first')
		pivot.columns = [str(column) for column in pivot.columns]
		totals[indicator] = pivot.astype(float)
	return totals or None


def _expand_ref_totals(totals, years_to_fit, base_year, scale_start_year=None):
	"""Linearly interpolate REF totals to every output year and drop the base year.

	The base year is removed from the output so that downstream scaling
	leaves base-year sums (which come directly from the input data)
	unchanged. When *scale_start_year* is supplied, years strictly less
	than it are also dropped, so only years ``>= scale_start_year`` are
	rescaled by downstream callers (earlier years keep their unscaled
	linear-interpolation values).

	Args:
		totals (dict or None): The mapping returned by
			:func:`load_regional_totals`.
		years_to_fit (list[int]): Years for which expanded totals are
			needed.
		base_year (int): The rebase base year; entries for this year are
			dropped from the expanded result.
		scale_start_year (int, optional): When supplied, drop entries for
			years strictly less than this value so that downstream scaling
			leaves earlier years untouched. Defaults to None (scale all
			available years).

	Returns:
		dict or None: A new mapping with the same keys as *totals* whose
		values are interpolated Series (region) or DataFrames (county)
		indexed/columned by year string. Returns ``None`` when *totals*
		is ``None``.
	"""
	if totals is None:
		return None

	base_year_str = str(base_year)
	expanded = {}
	for indicator, data in totals.items():
		if isinstance(data, pd.Series):
			anchor_years = sorted(int(year) for year in data.index)
			anchor_values = data.loc[[str(year) for year in anchor_years]].to_numpy(dtype=float)
			fit_years = [year for year in years_to_fit if year >= anchor_years[0]]
			if not fit_years:
				continue
			interpolated = np.interp(fit_years, anchor_years, anchor_values)
			series = pd.Series(interpolated, index=[str(year) for year in fit_years])
			if base_year_str in series.index:
				series = series.drop(base_year_str)
			if scale_start_year is not None:
				keep = [idx for idx in series.index if int(idx) >= scale_start_year]
				series = series.loc[keep]
			if not series.empty:
				expanded[indicator] = series
		elif isinstance(data, pd.DataFrame):
			anchor_years = sorted(int(column) for column in data.columns)
			anchor_columns = [str(year) for year in anchor_years]
			fit_years = [year for year in years_to_fit if year >= anchor_years[0]]
			if not fit_years:
				continue
			anchor_array = data[anchor_columns].to_numpy(dtype=float)
			interpolated = np.vstack([
				np.interp(fit_years, anchor_years, row) for row in anchor_array
			])
			frame = pd.DataFrame(
				interpolated,
				index=data.index,
				columns=[str(year) for year in fit_years],
			)
			if base_year_str in frame.columns:
				frame = frame.drop(columns=[base_year_str])
			if scale_start_year is not None:
				keep = [col for col in frame.columns if int(col) >= scale_start_year]
				frame = frame[keep]
			if not frame.empty:
				expanded[indicator] = frame
	return expanded or None


def _detect_totals_mode(regtot):
	"""Infer the scaling mode from the REF totals structure.

	Args:
		regtot (dict or None): The mapping returned by
			:func:`load_regional_totals`.

	Returns:
		str: ``'none'``, ``'region'``, or ``'county'``.
	"""
	if regtot is None:
		return 'none'
	sample = next(iter(regtot.values()), None)
	if isinstance(sample, pd.DataFrame):
		return 'county'
	return 'region'


def _align_county_index(regtot, target_county_ids):
	"""Reconcile REF county_id form with the form used in city tables.

	The REF by-county file typically uses full FIPS codes (e.g. ``53033``)
	while the city-level tables in this codebase may use the short county
	code (e.g. ``33``). This helper detects a uniform mismatch and remaps
	the REF DataFrames' index to match.

	Args:
		regtot (dict[str, pandas.DataFrame]): Per-indicator REF totals.
		target_county_ids (Iterable[int]): The county_id values used in
			the city-level tables.

	Returns:
		dict[str, pandas.DataFrame]: A new dict with aligned indexes. If
			no remap is necessary or possible, *regtot* is returned
			unchanged.
	"""
	target = set(int(v) for v in pd.Series(target_county_ids).dropna().unique())
	if not target:
		return regtot
	sample = next(iter(regtot.values()))
	source = set(int(v) for v in sample.index)
	if source == target:
		return regtot
	# Try modulo 1000 (full FIPS -> short).
	short_to_full = {v % 1000: v for v in source}
	if set(short_to_full.keys()) >= target:
		return {k: v.rename(index={full: full % 1000 for full in source}) for k, v in regtot.items()}
	# Try adding state prefix (short -> full FIPS) using the most common WA prefix in target.
	# Fall back to no change if neither alignment works.
	return regtot


def _apply_scaling(result, row_indices, years_to_fit, totals):
	"""Rescale a block of rows in *result* in place to match per-year *totals*.

	For each year (after the first), the difference between the year's
	current sum across *row_indices* and the target total is distributed
	in proportion to each row's year-over-year increment.

	Args:
		result (numpy.ndarray): The full interpolated array (modified in
			place).
		row_indices (numpy.ndarray): Integer positions of the rows to
			rescale.
		years_to_fit (list[int]): The years corresponding to *result*'s
			columns.
		totals (pandas.Series): Year-string-indexed target totals.
	"""
	if row_indices.size == 0:
		return
	block = result[row_indices]
	diffs = block[:, 1:] - block[:, :-1]
	for index in range(1, len(years_to_fit)):
		year = str(years_to_fit[index])
		if year not in totals.index:
			continue
		total_diff = float(totals.loc[year]) - block[:, index].sum()
		if total_diff == 0:
			continue
		denom = diffs[:, index - 1].sum()
		if denom == 0:
			shares = np.full(block.shape[0], 1 / block.shape[0])
		else:
			shares = diffs[:, index - 1] / denom
		block[:, index] = np.maximum(0, block[:, index] + shares * total_diff)
	result[row_indices] = block


def interpolate_controls_with_anchors(df, indicator, anchor_years, years_to_fit, totals=None,
									  id_col='control_id', group_col=None, round_interpolated=False):
	"""Interpolate control totals between anchor years and optionally scale to REF totals.

	Performs linear interpolation of an indicator between anchor years for
	each geography, then adjusts increments so that annual sums match the
	supplied REF totals. When *totals* is a Series, scaling is applied
	region-wide. When *totals* is a DataFrame keyed by *group_col*,
	scaling is applied independently within each group (e.g. per county).

	Args:
		df (pandas.DataFrame): DataFrame containing anchor-year columns
			named ``<indicator><year>``, an ID column, and optionally a
			group column.
		indicator (str): The indicator prefix (e.g. ``'HH'``, ``'Emp'``).
		anchor_years (list[int]): Sorted years for which values exist.
		years_to_fit (list[int]): Years to produce interpolated values for.
		totals (pandas.Series or pandas.DataFrame, optional): Either a
			Series of region-wide totals indexed by year string, or a
			DataFrame indexed by group key with year-string columns.
			Defaults to None.
		id_col (str, optional): Column name for the geography identifier.
			Defaults to ``'control_id'``.
		group_col (str, optional): Column name to group by when *totals*
			is a DataFrame (e.g. ``'county_id'``). Required for per-group
			scaling. Defaults to None.
		round_interpolated (bool, optional): Whether to round interpolated
			values to the nearest integer. Defaults to False.

	Returns:
		pandas.DataFrame: Wide DataFrame with the ID column (and group
		column when *group_col* is supplied) followed by one column per
		year in *years_to_fit*.
	"""
	anchor_columns = [f'{indicator}{year}' for year in anchor_years]
	keys = [id_col] if group_col is None else [id_col, group_col]
	grouped = df.groupby(keys, sort=False)[anchor_columns].sum().reset_index()

	if grouped.empty:
		result = np.empty((0, len(years_to_fit)))
	else:
		result = np.vstack([
			np.interp(years_to_fit, anchor_years, row[anchor_columns].to_numpy(dtype=float))
			for _, row in grouped.iterrows()
		])

	if totals is not None and result.size:
		if isinstance(totals, pd.DataFrame):
			if group_col is None:
				raise ValueError('group_col is required when totals is a DataFrame.')
			for group_value, indices in grouped.groupby(group_col, sort=False).indices.items():
				if group_value not in totals.index:
					continue
				_apply_scaling(result, np.asarray(indices), years_to_fit, totals.loc[group_value])
		else:
			_apply_scaling(result, np.arange(result.shape[0]), years_to_fit, totals)

	if round_interpolated:
		result = np.rint(result)

	interpolated = pd.DataFrame(result, columns=[str(year) for year in years_to_fit])
	interpolated.insert(0, id_col, grouped[id_col].to_numpy())
	if group_col is not None:
		interpolated.insert(1, group_col, grouped[group_col].to_numpy())
	return interpolated


def _distribute_difference(values, difference):
	"""Distribute an integer rounding difference across a series of values.

	Iteratively adds or subtracts 1 from the highest-weighted element until
	the total difference is resolved.

	Args:
		values (pandas.Series): Integer values to adjust.
		difference (int or float): The total amount to distribute (positive
			adds, negative subtracts).

	Returns:
		pandas.Series: Adjusted integer series summing to the original total
			plus *difference*.
	"""
	adjusted = values.astype(int).copy()
	if difference == 0 or adjusted.empty:
		return adjusted

	step = 1 if difference > 0 else -1
	remaining = abs(int(difference))

	while remaining > 0:
		if step > 0:
			weights = adjusted.clip(lower=0)
			idx = weights.idxmax() if weights.sum() > 0 else adjusted.idxmin()
			adjusted.loc[idx] += 1
		else:
			eligible = adjusted[adjusted > 0]
			if eligible.empty:
				break
			idx = eligible.idxmax()
			adjusted.loc[idx] -= 1
		remaining -= 1

	return adjusted


def unroll_controls(ct, indicator, totals=None, new_id_col='subreg_id', group_col=None):
	"""Melt a wide control-totals table into long format and optionally redistribute integer
	rounding deltas so per-year sums match the supplied REF totals.

	Converts the wide interpolated table (one column per year) into a long
	table with ``year`` and ``value`` columns, then redistributes any
	difference between the year sums and the REF totals. When *totals* is
	a DataFrame keyed by *group_col*, redistribution is performed
	independently within each group (e.g. per county).

	Args:
		ct (pandas.DataFrame): Wide control-totals DataFrame produced by
			:func:`interpolate_controls_with_anchors`. The first column is
			the ID column; if *group_col* is supplied it is the second
			column. Remaining columns are year-string values.
		indicator (str): The indicator name (e.g. ``'HH'``), used to name
			the value column as ``total_<indicator lower>``.
		totals (pandas.Series or pandas.DataFrame, optional): REF totals to
			scale to. Defaults to None.
		new_id_col (str, optional): Name for the geography ID column in
			the output. Defaults to ``'subreg_id'``.
		group_col (str, optional): Column name used to group rows for
			per-group redistribution when *totals* is a DataFrame.
			Defaults to None.

	Returns:
		pandas.DataFrame: Long-format DataFrame with columns
		``[new_id_col, 'year', 'total_<indicator>']``. The *group_col*
		is dropped from the output.
	"""
	wide = ct.copy()
	id_col = wide.columns[0]
	id_vars = [id_col] + ([group_col] if group_col is not None else [])
	long = wide.melt(id_vars=id_vars, var_name='year', value_name='value')
	long = long.rename(columns={id_col: new_id_col})
	long['value'] = long['value'].round().astype(int)
	long['year'] = long['year'].astype(str)

	if totals is not None:
		if isinstance(totals, pd.DataFrame):
			if group_col is None:
				raise ValueError('group_col is required when totals is a DataFrame.')
			for group_value, group_rows in long.groupby(group_col, sort=False):
				if group_value not in totals.index:
					continue
				_redistribute_block(long, group_rows.index, totals.loc[group_value])
		else:
			_redistribute_block(long, long.index, totals)

	if group_col is not None:
		long = long.drop(columns=[group_col])
	return long.rename(columns={'value': f'total_{indicator.lower()}'})


def _redistribute_block(long, row_indices, totals):
	"""Redistribute integer rounding deltas within a subset of rows so per-year
	sums match the supplied totals.

	Args:
		long (pandas.DataFrame): Long-format DataFrame with at least
			``'year'`` and ``'value'`` columns; modified in place.
		row_indices (pandas.Index): Index of the rows in *long* to
			operate on.
		totals (pandas.Series): Year-string-indexed target totals.
	"""
	subset = long.loc[row_indices]
	year_totals = subset.groupby('year', as_index=False)['value'].sum().rename(columns={'value': 'ct'})
	targets = totals.rename('should_be').reset_index()
	targets.columns = ['year', 'should_be']
	targets['year'] = targets['year'].astype(str)
	diffs = year_totals.merge(targets, on='year', how='inner')
	diffs['dif'] = (diffs['should_be'] - diffs['ct']).round().astype(int)

	for _, row in diffs.iterrows():
		year_mask = subset['year'] == row['year']
		year_indices = subset.index[year_mask]
		long.loc[year_indices, 'value'] = _distribute_difference(
			long.loc[year_indices, 'value'], row['dif']
		).to_numpy()


def build_control_totals_workbooks(outputs, regtot, ref_base_year, base_year, target_year,
								   round_interpolated=False, stepped_years=None,
								   scale_start_year=None, intermediate_target_years=None):
	"""Interpolate rebased targets into stepped and annual control-totals sheets.

	Produces interpolated control totals at the configured stepped years and
	an unrolled long-format table, plus an annual regional summary. When
	*regtot* is a per-county mapping, scaling and rounding redistribution
	are applied independently within each county.

	Args:
		outputs (dict): Dictionary of DataFrames returned by
			:func:`build_rebased_targets`.
		regtot (dict or None): Optional REF totals from
			:func:`load_regional_totals`. Series values trigger region-wide
			scaling; DataFrame values (indexed by ``county_id``) trigger
			per-county scaling.
		ref_base_year (int): The REF (Regional Economic Forecast) base year.
		base_year (int): The rebase base year.
		target_year (int): The target horizon year.
		round_interpolated (bool, optional): Whether to round interpolated
			values. Defaults to False.
		stepped_years (list[int], optional): Explicit list of years for
			stepped output. When ``None``, defaults to 5-year intervals
			from *base_year* through *target_year*, plus the REF
			base year.
		scale_start_year (int, optional): When supplied, only years
			``>= scale_start_year`` are rescaled to the REF totals;
			earlier years keep their unscaled linear-interpolation values
			between the input anchors. Defaults to None (scale all years).

	Returns:
		dict: A dictionary of DataFrames keyed by indicator name plus
			``'unrolled'`` and ``'unrolled_regional'`` entries.
	"""
	anchors = sorted({ref_base_year, base_year, target_year, *(intermediate_target_years or [])})
	if stepped_years is None:
		stepped_years = [ref_base_year, *range(base_year, target_year + 1, 5)]
		if target_year not in stepped_years:
			stepped_years.append(target_year)
	stepped_years = sorted(set(stepped_years))

	to_interpolate = {
		'HHPop': outputs['CityPop'],
		'HH': outputs['CityHH'],
		'Emp': outputs['CityEmp'],
		'Pop': outputs['CityPop'],
	}

	mode = _detect_totals_mode(regtot)
	group_col = 'county_id' if mode == 'county' else None

	if mode == 'county':
		# city_data county_id may be stored in short form (e.g. 33) while the
		# by-county REF table uses the full FIPS code (e.g. 53033). Reconcile
		# the REF index to whatever form the city tables use so per-county
		# lookups match.
		regtot = _align_county_index(regtot, outputs['CityPop']['county_id'])

	cts = {}
	unrolled = None
	stepped_totals = _expand_ref_totals(regtot, stepped_years, base_year, scale_start_year)
	for indicator, frame in to_interpolate.items():
		totals = None if stepped_totals is None else stepped_totals.get(indicator)
		wide_ct = interpolate_controls_with_anchors(
			frame.sort_values('control_id'),
			indicator,
			anchor_years=anchors,
			years_to_fit=stepped_years,
			totals=totals,
			group_col=group_col,
			round_interpolated=round_interpolated,
		)
		current_unrolled = unroll_controls(
			wide_ct, indicator, totals=totals, new_id_col='subreg_id', group_col=group_col
		)
		unrolled = current_unrolled if unrolled is None else unrolled.merge(
			current_unrolled, on=['subreg_id', 'year'], how='outer'
		)
		# Drop the group helper column from the wide CT so the per-indicator output
		# schema (control_id + year columns) is unchanged across modes.
		if group_col is not None:
			wide_ct = wide_ct.drop(columns=[group_col])
		cts[indicator] = wide_ct

	cts['unrolled'] = unrolled

	all_years = list(range(ref_base_year, target_year + 1))
	annual_totals = _expand_ref_totals(regtot, all_years, base_year, scale_start_year)
	unrolled_all = None
	for indicator, frame in to_interpolate.items():
		totals = None if annual_totals is None else annual_totals.get(indicator)
		ct_all = interpolate_controls_with_anchors(
			frame.sort_values('control_id'),
			indicator,
			anchor_years=anchors,
			years_to_fit=all_years,
			totals=totals,
			group_col=group_col,
			round_interpolated=round_interpolated,
		)
		current_unrolled = unroll_controls(
			ct_all, indicator, totals=totals, new_id_col='subreg_id', group_col=group_col
		)
		unrolled_all = current_unrolled if unrolled_all is None else unrolled_all.merge(
			current_unrolled, on=['subreg_id', 'year'], how='outer'
		)

	value_columns = [column for column in unrolled_all.columns if column not in {'year', 'subreg_id'}]
	unrolled_regional = unrolled_all.groupby('year', as_index=False)[value_columns].sum()
	cts['unrolled_regional'] = unrolled_regional
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


def save_pipeline_outputs(pipeline, outputs, cts):
	"""Persist rebased targets and control-totals tables to the pipeline HDF5 store.

	Args:
		pipeline (Pipeline): The data pipeline providing the save interface.
		outputs (dict): Rebased-target DataFrames keyed by ``'RGs'``,
			``'CityPop'``, ``'CityHH'``, ``'CityEmp'``.
		cts (dict): Interpolated control-totals DataFrames keyed by
			indicator name.
	"""
	pipeline.save_table('rebased_targets_rgs', outputs['RGs'])
	pipeline.save_table('rebased_targets_city_pop', outputs['CityPop'])
	pipeline.save_table('rebased_targets_city_hh', outputs['CityHH'])
	pipeline.save_table('rebased_targets_city_emp', outputs['CityEmp'])
	pipeline.save_table('rebased_control_totals_hhpop', cts['HHPop'])
	pipeline.save_table('rebased_control_totals_hh', cts['HH'])
	pipeline.save_table('rebased_control_totals_emp', cts['Emp'])
	pipeline.save_table('rebased_control_totals_pop', cts['Pop'])
	pipeline.save_table('rebased_control_totals_unrolled', cts['unrolled'])
	pipeline.save_table('rebased_control_totals_unrolled_regional', cts['unrolled_regional'])


def run_step(context):
	"""Execute the rebased control-totals pipeline step.

	Loads city-level input data, builds rebased targets, optionally scales
	to Regional Economic Forecast (REF) projections, interpolates into stepped control
	totals, writes Excel workbooks to the output directory, and persists
	results to the pipeline.

	Expected settings.yaml block::

		rebased_targets:
		  input_file: control_id_working.xlsx
		  scale_to_ref: none           # one of: none | region | county
		  scale_start_year: null       # optional; only rescale years >= this year
		  round_interpolated: false
		  stepped_years: [2018, 2020, 2025, 2030, 2035, 2040, 2044, 2050]
		  output_targets_file: TargetsRebasedOutput.xlsx
		  output_controls_file: Control-Totals-LUVit.xlsx

	When ``scale_to_ref`` is ``region`` the ``ref_projection`` data table
	must be in the wide format (a ``variable`` column plus one column per
	year). When ``scale_to_ref`` is ``county`` a separate
	``ref_projection_by_county`` data table must be registered in the
	long format with ``county_id`` and ``year`` columns plus one column
	per indicator. The legacy boolean form (``true``/``false``) is still
	accepted for backward compatibility (``true`` -> ``region``).

	Args:
		context (dict): The pypyr context dictionary, expected to contain
			a ``'configs_dir'`` key.

	Returns:
		dict: The unchanged pypyr context dictionary.
	"""
	pipeline = Pipeline(settings_path=context['configs_dir'])
	cfg = pipeline.settings.get('rebased_targets', {})

	city_data = load_city_data(pipeline)
	ref_base_year, base_year, target_year, intermediate_target_years = _infer_years(city_data)
	outputs = build_rebased_targets(
		city_data, ref_base_year, base_year, target_year,
		intermediate_target_years=intermediate_target_years,
	)

	regtot = load_regional_totals(pipeline, base_year)
	round_interpolated = cfg.get('round_interpolated', False)
	stepped_years = cfg.get('stepped_years', None)
	scale_start_year = cfg.get('scale_start_year', None)
	if scale_start_year is not None:
		scale_start_year = int(scale_start_year)
	cts = build_control_totals_workbooks(
		outputs,
		regtot,
		ref_base_year,
		base_year,
		target_year,
		round_interpolated=round_interpolated,
		stepped_years=stepped_years,
		scale_start_year=scale_start_year,
		intermediate_target_years=intermediate_target_years,
	)

	output_dir = Path(pipeline.get_output_dir())
	targets_file = cfg.get('output_targets_file', 'TargetsRebasedOutput.xlsx')
	controls_file = cfg.get('output_controls_file', 'Control-Totals-LUVit.xlsx')
	write_workbook(outputs, output_dir / targets_file)
	write_workbook(cts, output_dir / controls_file)
	save_pipeline_outputs(pipeline, outputs, cts)
	return context
