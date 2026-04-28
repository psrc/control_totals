import pandas as pd
from util import Pipeline


def sum_ofm_by_control_area(pipeline):
    """Aggregate OFM parcelized estimate data to the control-area level.

    For each OFM parcelized table in settings, merges parcel-level data
    with the parcel-to-control-area crosswalk, validates that every parcel
    received a control ID, renames population columns with an ``ofm_``
    prefix, sums to control areas, and saves the result.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and stored tables.

    Raises:
        ValueError: If any parcel lacks a ``control_id`` after the merge.
    """
    p = pipeline

    # get ofm parcelized table list from settings
    table_list = []
    ofm_parcelized_tables = p.get_elmer_list()
    for table in ofm_parcelized_tables:
        if 'ofm_parcelized' in table['name']:
            table_list.append(table['name'])

    # load parcel to control area crosswalk
    xwalk = p.get_table('ofm_parcel_control_area_xwalk')

    # loop through each ofm parcelized table and merge with the parcel to control area xwalk
    for table in table_list:
        df = (
            p.get_table(table)
            .merge(xwalk, on='parcel_id', how='left')
        )

        # check to make sure each parcel has a control_id
        if df.loc[df.control_id.isna()].empty == False:
            raise ValueError("There are parcels that do not have a control_id \
                            after merging with the ofm_parcel_control_area_xwalk. \
                            Please investigate and fix the issue before proceeding.")
        
        # sum ofm data by control area
        df = df.rename(
            columns={
                'total_pop': 'ofm_total_pop',
                'household_pop': 'ofm_hhpop',
                'housing_units': 'ofm_units',
                'occupied_housing_units': 'ofm_hh',
                'group_quarters': 'ofm_gq'
            }
        )
        ofm_by_control = df[['control_id','ofm_total_pop', 'ofm_hhpop', 'ofm_units', 'ofm_hh', 'ofm_gq']].groupby('control_id').sum().reset_index()

        # save to HDF5
        p.save_table(f"{table}_by_control_area", ofm_by_control)

def run_step(context):
    """Execute the parcel data aggregation pipeline step.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Aggregating parcelized ofm data to control_area...")
    sum_ofm_by_control_area(p)
    return context