import pandas as pd
import geopandas as gpd
import numpy as np
from util import Pipeline


def points_in_polygon(points_gdf, polygons_gdf, col_name, buffer=0):
    """
    Check if points intersect polygons and add a boolean column to the result.

    Determines whether each point in the points GeoDataFrame intersects with
    any polygon in the polygons GeoDataFrame. Optionally applies a buffer to
    the points before checking intersection.

    Args:
        points_gdf (geopandas.GeoDataFrame): A GeoDataFrame containing point
            geometries to check for intersection.
        polygons_gdf (geopandas.GeoDataFrame): A GeoDataFrame containing polygon
            geometries to check against.
        col_name (str): The name of the boolean column to add to the points
            GeoDataFrame indicating intersection status.
        buffer (float, optional): Buffer distance to apply to points before
            checking intersection, in the units of the coordinate reference
            system. Defaults to 0 (no buffer).

    Returns:
        geopandas.GeoDataFrame: The input points GeoDataFrame with an additional
            boolean column indicating whether each point intersects the polygons.

    Raises:
        Exception: If an error occurs during the intersection check.
    """
    try:
        if buffer > 0:
            buffered_points_gdf = points_gdf.copy()
            buffered_points_gdf.geometry = buffered_points_gdf.geometry.buffer(buffer)
            intersects = buffered_points_gdf.geometry.intersects(
                polygons_gdf.geometry.unary_union
            )
        else:
            intersects = points_gdf.geometry.intersects(
                polygons_gdf.geometry.unary_union
            )
        points_gdf[col_name] = intersects
        # Convert boolean intersection results to integer (1 = intersects, 0 = does not)
        points_gdf[col_name] = points_gdf[col_name].astype(int)

        return points_gdf

    except Exception as e:
        print(f"Error in points_in_polygon: {e}")
        raise


def flag_rural_parcels(pipeline, parcels):
    """Flag parcels that fall within rural areas.

    Retrieves the rural boundaries from the pipeline and marks each parcel
    with a boolean 'rural' column indicating whether it intersects a rural area.

    Args:
        pipeline (Pipeline): The data pipeline providing access to geodataframes
            and settings.
        parcels (geopandas.GeoDataFrame): A GeoDataFrame of parcel point geometries.

    Returns:
        geopandas.GeoDataFrame: The input parcels GeoDataFrame with an added
            'rural' column (1 if rural, 0 otherwise).
    """
    rural = pipeline.get_geodataframe("psrc_region")
    rural = rural[rural["feat_type"] == "rural"]
    parcels = points_in_polygon(parcels, rural, "rural")
    return parcels


def flag_urban_center_parcels(pipeline, parcels):
    """Flag parcels that fall within urban centers.

    Retrieves the urban center boundaries from the pipeline and marks each
    parcel with a boolean 'urban_center' column indicating whether it
    intersects an urban center.

    Args:
        pipeline (Pipeline): The data pipeline providing access to geodataframes
            and settings.
        parcels (geopandas.GeoDataFrame): A GeoDataFrame of parcel point geometries.

    Returns:
        geopandas.GeoDataFrame: The input parcels GeoDataFrame with an added
            'urban_center' column (1 if in an urban center, 0 otherwise).
    """
    urban_centers = pipeline.get_geodataframe("urban_centers")
    parcels = points_in_polygon(parcels, urban_centers, "urban_center")
    return parcels


def flag_hct_parcels(pipeline, parcels):
    """Flag parcels within high-capacity transit (HCT) stop buffers.

    For each HCT stop type defined in the pipeline settings (e.g., brt,
    light_rail, commuter_r, ferry), buffers the non-rural stops by the
    configured distance and flags parcels that fall within those buffers.

    Args:
        pipeline (Pipeline): The data pipeline providing access to geodataframes
            and settings, including 'hct_buffers' buffer distances.
        parcels (geopandas.GeoDataFrame): A GeoDataFrame of parcel point geometries.

    Returns:
        tuple:
            geopandas.GeoDataFrame: The input parcels GeoDataFrame with added
                boolean columns for each HCT stop type.
            geopandas.GeoDataFrame: A GeoDataFrame of the buffered HCT stop
                polygons used for the intersection checks.
    """
    hct_stops = pipeline.get_geodataframe("hct_stops")
    hct_stops = hct_stops[hct_stops["rural"] == 0]

    poly_list = []
    for stop_type, buffer_distance in pipeline.settings["hct_buffers"].items():
        stops = hct_stops[hct_stops[stop_type] == 1]
        stops.geometry = stops.geometry.buffer(buffer_distance)
        parcels = points_in_polygon(parcels, stops, stop_type)
        poly_list.append(stops)

    hct_buffers = gpd.GeoDataFrame(
        pd.concat(poly_list, ignore_index=True), crs=poly_list[0].crs
    )
    return parcels, hct_buffers


def assign_ids(parcels, control_areas):
    """
    Assign control_id, control_hct_id, and subreg_id to parcels based on spatial join with control areas.

    args:
        parcels (GeoDataFrame): GeoDataFrame containing parcel geometries that are already joined with HCT data (including 'tod' column).
        control_areas (GeoDataFrame): GeoDataFrame containing control area geometries and control_id.

    returns:
        GeoDataFrame: Updated parcels GeoDataFrame with assigned control_id, control_hct_id, and subreg_id.
    """

    parcels = parcels.sjoin(control_areas[["control_id", "geometry"]], how="left").drop(columns="index_right")
    if parcels['control_id'].isna().any():
        missing = parcels.loc[parcels['control_id'].isna()].copy().drop(columns="control_id")
        parcels = parcels.loc[~parcels['control_id'].isna()].copy()
        missing = missing.sjoin_nearest(control_areas[["control_id", "geometry"]]).drop(columns="index_right")
        parcels = pd.concat([parcels, missing], ignore_index=True)

    parcels['control_id'] = parcels['control_id'].astype(int)
    parcels['control_hct_id'] = np.where(parcels['tod'] != 0, parcels['control_id'] + 1000, parcels['control_id'])
    parcels['subreg_id'] = parcels['control_hct_id']
    return parcels


def run_step(context):
    """Execute the HCT buffer creation pipeline step.

    Loads parcel point data, flags each parcel as rural, urban center, or
    within various HCT buffer zones, then assigns a transit-oriented
    development (TOD) category code to each parcel. Results are saved back
    to the pipeline.

    TOD codes:
        0 - Non-HCT, non-urban-center (or rural)
        1 - BRT
        2 - Commuter rail
        4 - Light rail
        5 - Ferry
        6 - Urban center

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a 'configs_dir' key with the path to the configuration directory.

    Returns:
        dict: The updated pypyr context dictionary.
    """
    # Initialize pipeline and load parcel point geometries
    p = Pipeline(settings_path=context["configs_dir"])
    parcels = p.get_geodataframe("parcel_pts_current")
    
    # Flag each parcel by geographic category: rural, urban center, and HCT buffer zones
    parcels = flag_rural_parcels(p, parcels)
    parcels = flag_urban_center_parcels(p, parcels)
    parcels, hct_buffers = flag_hct_parcels(p, parcels)
    
    # Assign transit-oriented development (TOD) category codes to each parcel.
    # Order matters: later assignments override earlier ones, establishing priority.
    # Highest priority is rural (reset to 0), then light_rail (4), commuter_r (2),
    # ferry (5), brt (1), and urban_center (6). Lowest priority is the default (0).
    parcels["tod"] = 0                                                             
    parcels["tod"] = np.where(parcels["urban_center"] == 1, 6, parcels["tod"])       
    parcels["tod"] = np.where(parcels["brt"] == 1, 1, parcels["tod"])               
    parcels["tod"] = np.where(parcels["ferry"] == 1, 5, parcels["tod"])           
    parcels["tod"] = np.where(parcels["commuter_r"] == 1, 2, parcels["tod"])      
    parcels["tod"] = np.where(parcels["light_rail"] == 1, 4, parcels["tod"])         
    parcels["tod"] = np.where(parcels["rural"] == 1, 0, parcels["tod"])

    # Assign control area IDs to parcels based on spatial join with control area geometries.
    # Creates control_hct_id by adding 1000 to control_id for parcels in HCT zones, 
    # and subreg_id is set equal to control_hct_id.
    control_areas = p.get_geodataframe("control_areas")
    parcels = assign_ids(parcels, control_areas)
    
    # Persist flagged parcels and HCT buffer geometries to the pipeline
    p.save_geodataframe("parcels_hct", parcels)
    p.save_geodataframe("hct_buffers", hct_buffers)

    return context
