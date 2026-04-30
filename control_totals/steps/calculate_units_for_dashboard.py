from control_totals.util import Pipeline


def calculate_units(p: Pipeline):
    """Convert household control totals into housing units for the dashboard.

    For future years, divides each control area's household counts by an
    occupancy rate (1 - vacancy_rate) computed from OFM parcelized estimates
    aggregated to the (county_id, rgid) level. The base-year column is then
    overwritten with the actual per-control-id OFM unit count so the
    dashboard's base-year units exactly match observed OFM data.

    Args:
        p (Pipeline): Pipeline providing access to settings and stored tables.

    Returns:
        pandas.DataFrame: ``rebased_control_totals_hh`` scaled to housing
            units, keyed by ``control_id`` with the year columns preserved.
    """
    baseyear = p.settings['base_year']
    baseyear_col = str(baseyear)
    xwalk = p.get_table('control_target_xwalk')[['control_id', 'county_id', 'rgid']]

    ofm_by_control = p.get_table(f'ofm_parcelized_{baseyear}_by_control_area')

    # Aggregate OFM households and units to (county, rgid) and derive the
    # occupancy rate. Aggregating before dividing avoids noisy per-control-area
    # rates where unit counts are small.
    ofm = (
        ofm_by_control
        .merge(xwalk, on='control_id', how='left')
        .groupby(['county_id', 'rgid'])[['ofm_hh', 'ofm_units']]
        .sum()
    )
    occupancy = ofm['ofm_hh'] / ofm['ofm_units']  # = 1 - vacancy_rate

    # Attach county_id and rgid to each control_id and broadcast the
    # (county_id, rgid)-indexed occupancy rate onto the HH table, then divide
    # HH by occupancy to get units for future years.
    df = p.get_table('rebased_control_totals_hh').merge(xwalk, on='control_id', how='left')
    occ_per_row = df.set_index(['county_id', 'rgid']).index.map(occupancy)
    value_cols = [c for c in df.columns if c not in {'control_id', 'county_id', 'rgid'}]
    df[value_cols] = df[value_cols].div(occ_per_row, axis=0)
    df = df.drop(columns=['county_id', 'rgid'])

    # Overwrite the base-year column with actual OFM units per control_id.
    if baseyear_col in df.columns:
        actual_units = ofm_by_control.set_index('control_id')['ofm_units']
        df[baseyear_col] = df['control_id'].map(actual_units).astype(df[baseyear_col].dtype)

    return df


def run_step(context: dict):
    """Pipeline step entry point: compute housing-unit control totals.

    Args:
        context (dict): pypyr context dictionary, expected to contain
            ``'configs_dir'``.

    Returns:
        dict: The unchanged context dictionary.
    """
    print("Calculating housing units for dashboard...")
    p = Pipeline(settings_path=context['configs_dir'])
    df = calculate_units(p)
    p.save_table('rebased_control_totals_units', df)
    return context