import pandas as pd
import geopandas as gpd
from util import Pipeline


def create_block_control_area_xwalk(pipeline,gdf_name):
    """Create a crosswalk between census blocks and control areas.

    Loads block geometries from the pipeline, converts each block to its
    representative point, performs a nearest spatial join against control
    area polygons, and saves the resulting block-to-control-area lookup.

    Args:
        pipeline (Pipeline): The data pipeline providing access to geodataframes
            and settings.
    """
    p = pipeline
    
    # load blocks geodataframe from h5
    blk = p.get_geodataframe(gdf_name)
    blk_id = p.get_id_col(gdf_name)

    # load control areas geodataframe from h5
    control_areas = p.get_geodataframe('control_areas')
    
    # convert blocks to centroids
    blk_pts = blk.copy()
    blk_pts['geometry'] = blk_pts.representative_point()

    # spatial join block centroids to get rgid for each block
    # uses sjoin_nearest to handle edge cases where centroids fall just outside control areas
    # this shouldn't be a big issue since the edge cases mostly fell on waterways
    blk_pts = blk_pts.sjoin_nearest(control_areas, how = 'left').drop(columns=['index_right'])
    return blk_pts[[blk_id, 'control_id']]

def run_step(context):
    """Execute the block crosswalk creation pipeline step.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    # pypyr step
    print("Creating block to control area crosswalk...")
    p = Pipeline(settings_path=context['configs_dir'])
    blk = create_block_control_area_xwalk(p, 'blocks')
    p.save_table('block_control_area_xwalk', blk)
    if 'blocks_2010' in p.get_elmer_geo_names():
        blk_2010 = create_block_control_area_xwalk(p, 'blocks_2010')
        p.save_table('block_2010_control_area_xwalk', blk_2010)
    return context
