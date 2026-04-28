import pandas as pd
import geopandas as gpd
from util import Pipeline


def create_parcel_control_area_xwalk(parcel_pts, control_areas):
    """Create a crosswalk between parcels and control areas.

    Performs a spatial join between parcel point centroids and control-area
    polygons.  Uses ``sjoin_nearest`` as a fallback for parcels whose
    centroids fall just outside control-area boundaries.

    Args:
        parcel_pts (geopandas.GeoDataFrame): Parcel centroids with columns
            ``['parcel_id', 'geometry']``.
        control_areas (geopandas.GeoDataFrame): Control-area polygons with
            columns ``['control_id', 'geometry']``.

    Returns:
        pandas.DataFrame: DataFrame with columns ``['parcel_id', 'control_id']``
            linking every parcel to its control area.
    """
    # spatial join parcel centroids to get control_id for each parcel
    # uses sjoin_nearest to handle edge cases where centroids fall just outside control areas
    parcel_join = parcel_pts.sjoin(control_areas, how = 'left').drop(columns=['index_right'])
    # copy parcel points that didn't join to a control area
    if parcel_join['control_id'].isna().any():
        missing_control_id = parcel_join.loc[parcel_join['control_id'].isna()].copy().drop(columns=['control_id'])
        # use spatial join nearest to assign control_id based on nearest control area
        missing_control_id = missing_control_id.sjoin_nearest(control_areas).drop(columns=['index_right'])
        # drop parcels that didn't join to a control area
        parcel_join = parcel_join.loc[~parcel_join['control_id'].isna()].copy()
        # combine parcels that joined to a control area with those that were assigned a control area based on nearest
        parcel_out = pd.concat([parcel_join[['parcel_id','control_id']], missing_control_id[['parcel_id','control_id']]], ignore_index=True)
    parcel_out['control_id'] = parcel_out['control_id'].astype(int)

    return parcel_out


def run_step(context):
    """Execute the OFM parcel-to-control-area crosswalk pipeline step.

    Loads parcel centroids and control-area polygons from the HDF5 store,
    creates the spatial crosswalk, and saves the result.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    print("Creating parcel to control area crosswalks...")
    p = Pipeline(settings_path=context['configs_dir'])
    # load ofm year parcels geodataframe from h5
    parcel_pts_ofm = p.get_geodataframe('parcel_pts_ofm')
    # load control areas geodataframe from h5
    control_areas = p.get_geodataframe('control_areas')[['control_id', 'geometry']]
    # create crosswalk between ofm year parcels and control areas, then save to h5
    ofm_parcels = create_parcel_control_area_xwalk(parcel_pts_ofm, control_areas)
    p.save_table('ofm_parcel_control_area_xwalk', ofm_parcels)
    # save current year parcel crosswalk, control_id already spatially joined in hct step
    if p.check_table_exists('parcels_hct'):
        current_parcels = p.get_geodataframe('parcels_hct')
        p.save_table('current_parcel_control_area_xwalk', current_parcels[['parcel_id','subreg_id','control_id']])
    return context