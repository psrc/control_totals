import numpy as np
import geopandas as gpd
from util import Pipeline

def create_parcel_control_hct_xwalk(parcel_pts, control_hct):
    """Create a crosswalk between parcels and control HCT.

    Performs a spatial join between parcel point centroids and control HCT
    polygons.

    Args:
        parcel_pts (geopandas.GeoDataFrame): Parcel centroids with columns
            ``['parcel_id', 'geometry']``.
        control_hct (geopandas.GeoDataFrame): Control HCT polygons with
            columns ``['chct_id', 'geometry']``.
    Returns:
        pandas.DataFrame: DataFrame with columns ``['parcel_id', 'chct_id']``
            linking every parcel to its control HCT.
    """
    # Perform spatial join
    parcel_control_hct = gpd.sjoin(parcel_pts, control_hct, how="left", predicate="within")
    parcel_control_hct['control_id'] = np.where(parcel_control_hct['chct_id'] >= 1000, parcel_control_hct['chct_id'] - 1000, parcel_control_hct['chct_id'])
    parcel_control_hct = parcel_control_hct.rename(columns={'chct_id':'subreg_id'})

    return parcel_control_hct


def run_step(context):
    print("Creating parcel to control HCT crosswalks...")
    p = Pipeline(settings_path=context['configs_dir'])
    # load ofm year parcels geodataframe from h5
    parcel_pts = p.get_geodataframe('parcel_pts_current')
    # load control HCT geodataframe from h5
    control_hct = p.get_geodataframe('control_hct')[['chct_id', 'geometry']]
    # create crosswalk between ofm year parcels and control HCT, then save to h5
    parcels_control_hct = create_parcel_control_hct_xwalk(parcel_pts, control_hct)
    p.save_table('current_parcel_control_area_xwalk', parcels_control_hct[['parcel_id','subreg_id','control_id']])

    return context