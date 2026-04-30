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
	"""Infer the REF base year, base year, and target year from column names.

	Scans for ``Pop##`` (REF base year) and ``TotPop##`` (base and
	target years) columns in the input city data.

	Args:
		city_data (pandas.DataFrame): The raw city-level control totals input.

	Returns:
		tuple[int, int, int]: ``(ref_base_year, base_year, target_year)``.

	Raises:
		ValueError: If the required year columns cannot be found.
	"""
	ref_years = _extract_years(city_data.columns, re.compile(r'^Pop(\d{2,4})$'))
	target_years = _extract_years(city_data.columns, re.compile(r'^TotPop(\d{2,4})$'))

	if not ref_years or len(target_years) < 2:
		raise ValueError('Unable to infer rebasing years from control totals input columns.')

	return ref_years[0], target_years[0], target_years[-1]


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


def build_rebased_targets(city_data, ref_base_year, base_year, target_year):
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


def load_regional_totals(pipeline, base_year):
	"""Load Regional Economic Forecast (REF) projection totals for optional scaling.

	Returns ``None`` when scaling is disabled via
	``rebased_targets.scale_to_ref`` or no REF projection years beyond
	the base year are available.

	Args:
		pipeline (Pipeline): The data pipeline providing access to settings
			and stored tables.
		base_year (int): The rebase base year; only projection years after
			this are considered.

	Returns:
		dict or None: A dictionary mapping indicator names (``'Pop'``,
			``'HHPop'``, ``'HH'``, ``'Emp'``) to pandas.Series of regional
			totals indexed by year string, or ``None`` if scaling is disabled.
	"""
	cfg = pipeline.settings.get('rebased_targets', {})
	if not cfg.get('scale_to_ref', False):
		return None

	ref_projection = pipeline.get_table('ref_projection').copy()
	numeric_columns = [column for column in ref_projection.columns if str(column).isdigit()]
	numeric_columns = [column for column in numeric_columns if int(column) > base_year]
	if not numeric_columns:
		return None

	indicator_map = {
		'Pop': 'Tot Pop',
		'HHPop': 'HH Pop',
		'HH': 'HH',
		'Emp': 'Total Emp w/o Enlisted',
	}

	totals = {}
	for indicator, variable_name in indicator_map.items():
		matches = ref_projection.loc[ref_projection['variable'] == variable_name, numeric_columns]
		if matches.empty:
			continue
		totals[indicator] = matches.iloc[0].astype(float)
		totals[indicator].index = totals[indicator].index.astype(str)
	return totals or None


def interpolate_controls_with_anchors(df, indicator, anchor_years, years_to_fit, totals=None, id_col='control_id', round_interpolated=False):
	"""Interpolate control totals between anchor years and optionally scale to regional totals.

	Performs linear interpolation of an indicator between anchor years for
	each geography, then adjusts increments so that annual sums match an
	optional regional-totals series.

	Args:
		df (pandas.DataFrame): DataFrame containing anchor-year columns
			named ``<indicator><year>`` and an ID column.
		indicator (str): The indicator prefix (e.g. ``'HH'``, ``'Emp'``).
		anchor_years (list[int]): Sorted years for which values exist.
		years_to_fit (list[int]): Years to produce interpolated values for.
		totals (pandas.Series, optional): Regional totals indexed by year
			string used to scale the interpolated values. Defaults to None.
		id_col (str, optional): Column name for the geography identifier.
			Defaults to ``'control_id'``.
		round_interpolated (bool, optional): Whether to round interpolated
			values to the nearest integer. Defaults to False.

	Returns:
		pandas.DataFrame: Wide DataFrame with the ID column and one column
			per year in *years_to_fit*.
	"""
	anchor_columns = [f'{indicator}{year}' for year in anchor_years]
	grouped = df.groupby(id_col, sort=False)[anchor_columns].sum().reset_index()

	series = []
	for _, row in grouped.iterrows():
		values = row[anchor_columns].to_numpy(dtype=float)
		series.append(np.interp(years_to_fit, anchor_years, values))

	result = np.vstack(series) if series else np.empty((0, len(years_to_fit)))

	if totals is not None and result.size:
		diffs = result[:, 1:] - result[:, :-1]
		for index in range(1, len(years_to_fit)):
			year = str(years_to_fit[index])
			if year not in totals.index:
				continue
			total_diff = float(totals.loc[year]) - result[:, index].sum()
			if total_diff == 0:
				continue
			denom = diffs[:, index - 1].sum()
			if denom == 0:
				shares = np.full(result.shape[0], 1 / result.shape[0])
			else:
				shares = diffs[:, index - 1] / denom
			result[:, index] = np.maximum(0, result[:, index] + shares * total_diff)

	if round_interpolated:
		result = np.rint(result)

	interpolated = pd.DataFrame(result, columns=[str(year) for year in years_to_fit])
	interpolated.insert(0, id_col, grouped[id_col].to_numpy())
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


def unroll_controls(ct, indicator, totals=None, new_id_col='subreg_id'):
	"""Melt a wide control-totals table into long format and optionally adjust to regional totals.

	Converts the wide interpolated table (one column per year) into a long
	table with ``year`` and ``value`` columns, then redistributes any
	difference between the year sums and the regional totals series.

	Args:
		ct (pandas.DataFrame): Wide control-totals DataFrame with an ID
			column and year columns.
		indicator (str): The indicator name (e.g. ``'HH'``), used to name
			the value column as ``total_<indicator lower>``.
		totals (pandas.Series, optional): Regional totals indexed by year
			string. Defaults to None.
		new_id_col (str, optional): Name for the geography ID column in
			the output. Defaults to ``'subreg_id'``.

	Returns:
		pandas.DataFrame: Long-format DataFrame with columns
			``[new_id_col, 'year', 'total_<indicator>']``.
	"""
	wide = ct.copy()
	long = wide.melt(id_vars=wide.columns[0], var_name='year', value_name='value')
	long = long.rename(columns={wide.columns[0]: new_id_col})
	long['value'] = long['value'].round().astype(int)
	long['year'] = long['year'].astype(str)

	if totals is not None:
		year_totals = long.groupby('year', as_index=False)['value'].sum().rename(columns={'value': 'ct'})
		targets = totals.rename('should_be').reset_index().rename(columns={'index': 'year'})
		diffs = year_totals.merge(targets, on='year', how='inner')
		diffs['dif'] = (diffs['should_be'] - diffs['ct']).round().astype(int)

		for _, row in diffs.iterrows():
			year_mask = long['year'] == row['year']
			long.loc[year_mask, 'value'] = _distribute_difference(long.loc[year_mask, 'value'], row['dif']).to_numpy()

	return long.rename(columns={'value': f'total_{indicator.lower()}'})


def build_control_totals_workbooks(outputs, regtot, ref_base_year, base_year, target_year,
								   round_interpolated=False, stepped_years=None):
	"""Interpolate rebased targets into stepped and annual control-totals sheets.

	Produces interpolated control totals at the configured stepped years and
	an unrolled long-format table, plus an annual regional summary.

	Args:
		outputs (dict): Dictionary of DataFrames returned by
			:func:`build_rebased_targets`.
		regtot (dict or None): Optional regional totals from
			:func:`load_regional_totals`.
		ref_base_year (int): The REF (Regional Economic Forecast) base year.
		base_year (int): The rebase base year.
		target_year (int): The target horizon year.
		round_interpolated (bool, optional): Whether to round interpolated
			values. Defaults to False.
		stepped_years (list[int], optional): Explicit list of years for
			stepped output. When ``None``, defaults to 5-year intervals
			from *base_year* through *target_year*, plus the REF
			base year.

	Returns:
		dict: A dictionary of DataFrames keyed by indicator name plus
			``'unrolled'`` and ``'unrolled_regional'`` entries.
	"""
	anchors = sorted({ref_base_year, base_year, target_year})
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

	cts = {}
	unrolled = None
	for indicator, frame in to_interpolate.items():
		totals = None if regtot is None else regtot.get(indicator)
		cts[indicator] = interpolate_controls_with_anchors(
			frame.sort_values('control_id'),
			indicator,
			anchor_years=anchors,
			years_to_fit=stepped_years,
			totals=totals,
			round_interpolated=round_interpolated,
		)
		current_unrolled = unroll_controls(cts[indicator], indicator, totals=totals, new_id_col='subreg_id')
		unrolled = current_unrolled if unrolled is None else unrolled.merge(current_unrolled, on=['subreg_id', 'year'], how='outer')

	cts['unrolled'] = unrolled

	all_years = list(range(ref_base_year, target_year + 1))
	unrolled_all = None
	for indicator, frame in to_interpolate.items():
		totals = None if regtot is None else regtot.get(indicator)
		ct_all = interpolate_controls_with_anchors(
			frame.sort_values('control_id'),
			indicator,
			anchor_years=anchors,
			years_to_fit=all_years,
			totals=totals,
			round_interpolated=round_interpolated,
		)
		current_unrolled = unroll_controls(ct_all, indicator, totals=totals, new_id_col='subreg_id')
		unrolled_all = current_unrolled if unrolled_all is None else unrolled_all.merge(current_unrolled, on=['subreg_id', 'year'], how='outer')

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
		  scale_to_ref: false
		  round_interpolated: false
		  stepped_years: [2018, 2020, 2025, 2030, 2035, 2040, 2044, 2050]
		  output_targets_file: TargetsRebasedOutput.xlsx
		  output_controls_file: Control-Totals-LUVit.xlsx

	Args:
		context (dict): The pypyr context dictionary, expected to contain
			a ``'configs_dir'`` key.

	Returns:
		dict: The unchanged pypyr context dictionary.
	"""
	pipeline = Pipeline(settings_path=context['configs_dir'])
	cfg = pipeline.settings.get('rebased_targets', {})

	city_data = load_city_data(pipeline)
	ref_base_year, base_year, target_year = _infer_years(city_data)
	outputs = build_rebased_targets(city_data, ref_base_year, base_year, target_year)

	regtot = load_regional_totals(pipeline, base_year)
	round_interpolated = cfg.get('round_interpolated', False)
	stepped_years = cfg.get('stepped_years', None)
	cts = build_control_totals_workbooks(
		outputs,
		regtot,
		ref_base_year,
		base_year,
		target_year,
		round_interpolated=round_interpolated,
		stepped_years=stepped_years,
	)

	output_dir = Path(pipeline.get_output_dir())
	targets_file = cfg.get('output_targets_file', 'TargetsRebasedOutput.xlsx')
	controls_file = cfg.get('output_controls_file', 'Control-Totals-LUVit.xlsx')
	write_workbook(outputs, output_dir / targets_file)
	write_workbook(cts, output_dir / controls_file)
	save_pipeline_outputs(pipeline, outputs, cts)
	return context
