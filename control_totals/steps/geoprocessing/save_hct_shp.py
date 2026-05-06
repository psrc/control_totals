import pandas as pd
import geopandas as gpd
from util import Pipeline
from control_totals.util.elmer_helpers import read_from_elmer_geo

def dissovle_parcels_to_hct(pipeline):
    p = pipeline
    # read parcels from elmer geo directly (too large to save to pipeline)
    parcels_gdf = read_from_elmer_geo('PARCELS_URBANSIM_2023',['parcel_id'])
    # get parcel points already tagged with control_hct_id
    gdf = p.get_geodataframe('parcels_hct')[['parcel_id', 'control_id','control_hct_id']]
    # merge to get parcel polygons with control_hct_id, then dissolve to get hct polygons
    gdf = parcels_gdf.merge(gdf, on='parcel_id', how='left')
    diss_gdf = gdf.dissolve(by=['control_id','control_hct_id'])
    diss_gdf = diss_gdf.reset_index()[['control_id','control_hct_id','geometry']]
    # get control areas and merge to get control names for hct polygons
    control_areas = p.get_geodataframe('control_areas')[['control_id','control_name']]
    diss_gdf = (diss_gdf.merge(control_areas, on='control_id', how='left')
                .drop(columns='control_id')
                .rename(columns={'control_name':'chct_name','control_hct_id':'chct_id'}))
    return diss_gdf


def run_step(context):
    p = Pipeline(settings_path=context["configs_dir"])
    hct_gdf = dissovle_parcels_to_hct(p)
    p.save_geodataframe( 'hct_shp', hct_gdf)
    out_dir = p.get_output_dir()
    control_areas_year = str(p.settings['control_areas_year'])[-2:]
    hct_gdf.to_file(out_dir / 'control_hct.gdb', layer=f'control_hct{control_areas_year}', driver='OpenFileGDB', promote_to_multi=True)
    return context