import pandas as pd
from util import Pipeline


def sum_decennial_by_control_area(pipeline):
    """Aggregate decennial census block data to the control-area level.

    Merges block-level decennial data with the block-to-control-area
    crosswalk, sums census variables by control area, computes household
    population, and saves the result to the pipeline.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.
    """
    p = pipeline
    dec = p.get_table('dec_block_data')
    blk = p.get_table('block_control_area_xwalk')
    block_id = p.get_id_col('blocks')

    # merge decennial data with block to control area crosswalk
    df = dec.merge(blk, left_on='geoid', right_on=block_id)

    # get list of decennial census columns
    dec_cols = list(p.settings['census_variables'].keys())

    # sum decennial data by control area
    dec_by_control = (
        df.groupby('control_id')
        .sum()[dec_cols]
        .astype(int)
        .reset_index()
    )

    # calculate hhpop
    dec_by_control['dec_hhpop'] = (
        dec_by_control['dec_total_pop'] - dec_by_control['dec_gq']
    )

    # save to HDF5
    p.save_table('decennial_by_control_area', dec_by_control)


def run_step(context):
    """Execute the block data aggregation pipeline step.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Aggregating Decennial Census data to control_area...")
    sum_decennial_by_control_area(p)
    return context