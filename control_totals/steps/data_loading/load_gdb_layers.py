import pandas as pd
import geopandas as gpd
from util import Pipeline


def load_gdb_layers_to_hdf5(pipeline):
    """Load layers from a GDB in the data directory into the pipeline HDF5 store.

    Iterates over the ``gdb_layers`` list in settings, reads each layer,
    and saves it as a GeoDataFrame.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and the save interface.
    """
    # load layers in the gdb_layers list in settings.yaml
    p = pipeline
    gdb_layers = p.settings.get('gdb_layers', [])
    for layer in gdb_layers:
        layer_name = layer['name']
        file_path = layer['file']
        print(f"Loading {file_path}/{layer['layer']} into HDF5 as {layer_name}...")
        gdf = gpd.read_file(file_path, layer=layer['layer'])

        # save to HDF5
        p.save_geodataframe(layer_name, gdf)


def run_step(context):
    """Execute the GDB layer loading pipeline step.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    p = Pipeline(settings_path=context['configs_dir'])
    print("Loading layers from GDB into HDF5 store...")
    load_gdb_layers_to_hdf5(p)
    return context