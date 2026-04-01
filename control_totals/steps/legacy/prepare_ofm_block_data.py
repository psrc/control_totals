import pandas as pd
from util import Pipeline

def get_ofm_block_years(pipeline):
    block_data_years = []
    for table in pipeline.settings['Elmer']:
        if 'block' in table['name']:
            year = int(table['name'].split('_')[-1])
            block_data_years.append(year)
    return block_data_years

def sum_ofm_block_by_control_area(pipeline, year):
    """Aggregate OFM block data to the control-area level.

    Merges block-level OFM data with the block-to-control-area
    crosswalk, sums OFM variables by control area and saves the 
    result to the pipeline.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.
    """
    p = pipeline
    ofm = p.get_table(f'ofm_block_{year}')
    ofm_block_id = p.get_id_col(f'ofm_block_{year}')
    blk = p.get_table('block_2010_control_area_xwalk')
    block_id = p.get_id_col('blocks_2010')

    # merge OFM data with block to control area crosswalk
    df = ofm.merge(blk, left_on=ofm_block_id, right_on=block_id)

    # rename OFM columns
    rename_cols = {
        'housing_units': 'ofm_units',
        'occupied_housing_units': 'ofm_hh',
        'group_quarters_population': 'ofm_gq',
        'household_population': 'ofm_hhpop',
    }

    # sum OFM data by control area
    ofm_by_control = (
        df.groupby('control_id')
        .sum()[rename_cols.keys()]
        .rename(columns=rename_cols)
        .astype(int)
        .reset_index()
    )

    # calculate total pop
    ofm_by_control['ofm_total_pop'] = (
        ofm_by_control['ofm_hhpop'] + ofm_by_control['ofm_gq']
    )

    # save to HDF5
    p.save_table(f'ofm_block_{year}_by_control_area', ofm_by_control)


def run_step(context):
    """Execute the OFM block data aggregation pipeline step.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Aggregating OFM data to control_area...")
    for year in get_ofm_block_years(p):
        sum_ofm_block_by_control_area(p,year)
    
    return context