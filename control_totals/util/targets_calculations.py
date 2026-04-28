# shared calculations for units change and population change targets

def load_input_tables(pipeline,targets_type):
    # targets type: 'units' or 'total_pop'

    p = pipeline
    # load control to target lookup
    control_target_lookup = p.get_table('control_target_xwalk')

    # sum decennial data to target areas
    dec = (
        p.get_table('decennial_by_control_area')
        .merge(control_target_lookup, on='control_id', how='left')
        .groupby(['target_id','county_id','rgid']).sum().reset_index()
        .drop(columns=['control_id','name','target_name','control_name','exclude_from_target'], errors='ignore')
    )

    # merge decennial data with adjusted targets
    df = (
        p.get_table(f'adjusted_{targets_type}_change_targets')
        .merge(dec, on='target_id', how='left')
    )

    # calculate decennial household population, households and household size by target area
    df['dec_hhpop_by_target'] = df.groupby('target_id')['dec_hhpop'].transform('sum')
    df['dec_hh_by_target'] = df.groupby('target_id')['dec_hh'].transform('sum')
    df['dec_hhsz_by_target'] = df['dec_hhpop_by_target'] / df['dec_hh_by_target']

    # calculate decennial hhsz
    df['dec_hhsz'] = df['dec_hhpop'] / df['dec_hh']

    return df, dec

def get_base_data_column(base_data_source,col):
    if base_data_source == 'decennial':
        return f'dec_{col}'
    elif base_data_source == 'OFM':
        return f'ofm_{col}'
    else:
        raise ValueError("base_data_source must be either 'decennial' or 'OFM'")

def calc_gq(pipeline, df, region_df, horizon_year, base_data_source='decennial'):
    '''
    Calculate group quarters population for target areas in the horizon year based on
    the percentage of the region's GQ population in the decennial or OFM data and the
    regional GQ population in the horizon year from the Regional Economic Forecast.
    Parameters:
    pipeline: Pipeline object
    df: DataFrame with currently selected target areas and decennial or OFM GQ population
    region_df: DataFrame with regional decennial or OFM GQ population
    horizon_year: int, the year for which to calculate GQ population
    base_data_source: str, either 'decennial' or 'OFM' indicating the source of base data
    '''
    gq_col = get_base_data_column(base_data_source,'gq')
    gq_pct_col = get_base_data_column(base_data_source,'gq_pct')

    p = pipeline

    # load the Regional Economic Forecast table to get total GQ for the region in the horizon year
    ref = p.get_table('ref_projection')
    reg_gq_horizon = ref.loc[ref.variable == 'GQ Pop', str(horizon_year)].item()

    # calculate GQ percentage of the region based on decennial or OFM data
    reg_dec_gq_sum = region_df[gq_col].sum()
    df[gq_pct_col] = df[gq_col] / reg_dec_gq_sum

    # calculate target area GQ for horizon year as a percentage of the regional GQ from REF
    gq_horizon_col = f'gq_{horizon_year}'
    df[gq_horizon_col] = (df[gq_pct_col] * reg_gq_horizon).fillna(0).round(0).astype(int)
    return df


def load_base_year_emp(pipeline,emp_target_type):
    # emp_target_type: either 'includes res con' or 'excludes res con'
    p = pipeline
    base_year = p.settings['base_year']
    counties = p.settings['emp_target_types'][emp_target_type]

    emp = p.get_table(f'employment_{base_year}_by_control_area')

    control_area_lookup = p.get_table('control_target_xwalk')[['control_id','target_id','county_id']]

    emp = (
        emp.merge(control_area_lookup, on='control_id', how='left')
        .query(f'county_id in {counties}')
        .groupby(['target_id','county_id']).sum()
        .add_suffix(f'_{base_year}')
        .reset_index()
    )
    return emp