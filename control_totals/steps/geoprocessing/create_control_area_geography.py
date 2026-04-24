import pandas as pd
import geopandas as gpd
import numpy as np
from util import Pipeline

def union_dissolve(primary, secondary, id_col):
    """Union two GeoDataFrames and dissolve by an ID column.

    Overlays the primary geometries with the secondary layer, fills
    missing primary IDs from the secondary layer, and dissolves to
    produce one geometry per unique ID.

    Args:
        primary (geopandas.GeoDataFrame): The primary layer whose IDs
            take precedence.
        secondary (geopandas.GeoDataFrame): The secondary layer providing
            coverage where the primary has gaps.
        id_col (str): Column name for the geography identifier.

    Returns:
        geopandas.GeoDataFrame: Dissolved GeoDataFrame with columns
            ``[id_col, 'geometry']``.
    """
    primary = primary.rename(columns={id_col:'primary_id'})
    secondary = secondary.rename(columns={id_col:'secondary_id'})
    primary = primary.overlay(secondary, how='union',keep_geom_type=True)
    primary.loc[primary['primary_id'].isna(), 'primary_id'] = primary.loc[primary['primary_id'].isna(), 'secondary_id']
    primary = primary.dissolve(by='primary_id', as_index=False, dropna=False)
    return primary.rename(columns={'primary_id':id_col})[[id_col, 'geometry']]

def spatial_join_dissolve(gdf, gdf_to_join, gdf_to_join_id):
    """Spatial-join a GeoDataFrame to another and dissolve by the joined ID.

    Creates representative points for each feature, joins them to the
    target layer, merges back the joined ID, and dissolves.

    Args:
        gdf (geopandas.GeoDataFrame): Source geometries.
        gdf_to_join (geopandas.GeoDataFrame): Target layer for the spatial
            join.
        gdf_to_join_id (str): Column name in *gdf_to_join* to join on.

    Returns:
        geopandas.GeoDataFrame: Dissolved GeoDataFrame keyed by
            *gdf_to_join_id*.
    """
    gdf = gdf.reset_index(drop=True)
    gdf['temp_id'] = gdf.index + 1
    gdf_pts = gdf.copy()
    gdf_pts['geometry'] = gdf_pts.representative_point()
    gdf_pts = gdf_pts.sjoin(gdf_to_join, how='left')
    gdf = gdf.merge(gdf_pts[['temp_id', gdf_to_join_id]], on='temp_id', how='left')
    gdf = gdf.dissolve(by=gdf_to_join_id, as_index=False)
    return gdf

def prepare_counties(pipeline):
    """Prepare county-level rural control area geometries.

    Maps each PSRC county to its rural control-area ID and returns
    the resulting GeoDataFrame.

    Args:
        pipeline (Pipeline): The data pipeline providing access to geodataframes.

    Returns:
        geopandas.GeoDataFrame: County geometries with ``control_id``.
    """
    p = pipeline
    rural_control_id_map = {
        '033': 64,
        '035': 76,
        '053': 124,
        '061': 176
    }
    county = (
        p.get_geodataframe('county')
        .query("psrc == 1")
        .assign(control_id = lambda df: df['county_fip'].map(rural_control_id_map))
    )
    return county[['control_id','geometry']]


def prepare_military_bases(pipeline):
    """Prepare military-base control area geometries.

    Dissolves military base polygons by installation ID, joins them to
    the control-area crosswalk, and clips to the PSRC region.

    Args:
        pipeline (Pipeline): The data pipeline providing access to geodataframes
            and stored tables.

    Returns:
        geopandas.GeoDataFrame: Military base geometries with ``control_id``.
    """
    p = pipeline
    county = p.get_geodataframe('county').query("psrc == 1")
    military_xwalk = p.get_table('military_bases_xwalk')
    military = (
        p.get_geodataframe('military_bases')
        .dissolve('milspn_id')
        .merge(military_xwalk, on='milspn_id', how='inner')
        .clip(county.dissolve())
    )
    return military[['control_id', 'geometry']]

def prepare_tribal_areas(pipeline):
    """Prepare tribal-land control area geometries.

    Extracts the Tulalip Reservation polygon, clips it to the PSRC
    county boundaries, and assigns a fixed control ID.

    Args:
        pipeline (Pipeline): The data pipeline providing access to geodataframes.

    Returns:
        geopandas.GeoDataFrame: Tribal area geometry with ``control_id``.
    """
    p = pipeline
    county = p.get_geodataframe('county')
    tribal = p.get_geodataframe('tribal_land').clip(county.dissolve())
    tribal = tribal.loc[tribal.tribal_land=='Tulalip Reservation'].dissolve()
    tribal['control_id'] = 210
    return tribal[['control_id','geometry']]

def prepare_regional_geographies(pipeline):
    """Prepare regional-geography control area geometries.

    Joins regional geographies with the crosswalk to assign control IDs,
    then splits the Renton PAA into sub-areas using the old control areas
    as a template.

    Args:
        pipeline (Pipeline): The data pipeline providing access to geodataframes
            and stored tables.

    Returns:
        geopandas.GeoDataFrame: Regional geography polygons with
            ``control_id``.
    """
    p = pipeline
    reg = p.get_geodataframe('regional_geographies')
    reg_xwalk = p.get_table('regional_geographies_xwalk')
    reg['reg_id'] = reg['cnty_name'] + '_' + reg['juris']
    reg = reg.merge(reg_xwalk, on='reg_id', how='left')
    
    # split Renton PAA into the 3 seperate control areas (using old control areas for now)
    renton = reg.loc[reg['juris']=='Renton PAA'][['geometry']].copy()
    reg = reg.loc[reg['juris']!='Renton PAA'].copy()
    renton = renton.explode()
    old = p.get_geodataframe('old_control_areas')
    renton = spatial_join_dissolve(renton, old, 'control_id')
    reg = pd.concat([reg, renton], ignore_index=True)
    return reg[['control_id','geometry']]

def prepare_natural_resource_areas(pipeline):
    """Prepare natural-resource control area geometries.

    Combines national forest, national park, and natural resource polygons,
    dissolves them into a single layer, clips to the PSRC region, dissolves
    slivers and assigns control IDs.

    Args:
        pipeline (Pipeline): The data pipeline providing access to geodataframes.

    Returns:
        geopandas.GeoDataFrame: Natural resource area geometries with
            ``control_id``.
    """
    p = pipeline
    control_id_map = {
        '033': 301,
        '035': 302,
        '053': 303,
        '061': 304,
    }
    county = p.get_geodataframe('county').query("psrc == 1")
    # bring in natural resource layers and combine
    nat_forest = p.get_geodataframe('national_forest').dissolve()
    nat_park = p.get_geodataframe('national_park').dissolve()
    nat_resource = p.get_geodataframe('natural_resource').dissolve()
    nat = pd.concat([nat_forest, nat_park, nat_resource], ignore_index=True).dissolve()
    nat = nat.overlay(county, how='identity',keep_geom_type=True)
    nat = nat[nat['psrc'] == 1]

    # loop through each county to identify and dissolve slivers
    nat_resource_out = pd.DataFrame()
    for selected_county in county['county_fip'].unique():
        # Select the natural resources for the current county and perform a union overlay with the county geometry
        gdf = nat[nat['county_fip'] == selected_county]
        gdf = gdf.overlay(county[county['county_fip'] == selected_county], how='union',keep_geom_type=True)
        # Identify the slivers by exploding the geometries that do not have a resource attribute, 
        # creating representative points, and checking for intersections with buffered geometries
        # of the original natural resources layer
        explode = gdf[gdf['resource'].isna()].explode().reset_index(drop=True)
        explode['exp_id'] = explode.index + 1
        exp_pts = explode.copy()
        exp_pts['geometry'] = exp_pts.representative_point()
        buffer = gdf[~gdf['resource'].isna()]
        buffer['geometry'] = buffer.buffer(p.settings['nat_resource_sliver_buffer'])
        exp_pts = exp_pts.sjoin(buffer,how='left')
        # Extract the slivers and combine them with the original geometries that have
        #  a resource attribute, then dissolve
        slivers = exp_pts.loc[~exp_pts['resource_right'].isna(),'exp_id']
        sliver_gdf = explode.loc[explode['exp_id'].isin(slivers)]
        gdf_out = pd.concat([gdf.loc[~gdf['resource'].isna()], sliver_gdf], ignore_index=True)
        gdf_out = gdf_out.dissolve()
        gdf_out['county_fip'] = selected_county
        nat_resource_out = pd.concat([nat_resource_out, gdf_out[['county_fip', 'geometry']]], ignore_index=True)

    # assign control_ids
    nat_resource_out = (nat_resource_out
        .assign(control_id = lambda df: df['county_fip'].map(control_id_map))
    )
    return nat_resource_out[['control_id','geometry']]

def run_step(context):
    """Execute the control-area geography creation pipeline step.

    Prepares and unions county, regional, military, tribal, and
    natural-resource layers into a single control-area GeoDataFrame,
    adds crosswalk metadata, and saves to the pipeline.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Creating control area geography and saving to HDF5...")
    
    # prepare all layers for unioning
    counties = prepare_counties(p)
    reg = prepare_regional_geographies(p)
    military = prepare_military_bases(p)
    jblm_uga = reg.loc[reg['control_id'] == 405].copy()
    tribal = prepare_tribal_areas(p)
    nat_res = prepare_natural_resource_areas(p)

    # union all layers
    gdf = union_dissolve(reg,counties,'control_id')
    gdf = union_dissolve(military,gdf,'control_id')
    gdf = union_dissolve(jblm_uga,gdf,'control_id')
    gdf = union_dissolve(tribal,gdf,'control_id')
    gdf = union_dissolve(nat_res,gdf,'control_id')

    # add control names and target ids
    xwalk = p.get_table('control_target_xwalk')
    gdf = gdf.merge(xwalk,on='control_id',how='left')

    # save control areas to h5
    p.save_geodataframe('control_areas',gdf)
    # save control areas to geodatabase for use in ArcGIS
    gdf[['control_id','control_name','geometry']].to_file(p.get_output_path('control.gdb'),layer='control26', driver='OpenFileGDB', promote_to_multi=True)
    return context