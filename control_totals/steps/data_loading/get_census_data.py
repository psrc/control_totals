import os
from util import Pipeline, CensusApi


def get_dec_block_data(pipeline):
    """Fetch decennial census block-level data and save to the pipeline.

    Reads the Census API key from the environment, queries the decennial
    census PL file for the configured variables and counties, and persists
    the result as ``'dec_block_data'`` in the pipeline HDF5 store.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and the save interface.
    """
    p = pipeline
    api_key = os.getenv(p.settings['CensusKey'])
    c = CensusApi(api_key)
    census_year = p.settings.get('census_year')

    county_ids = p.settings['county_ids']
    state_id = p.settings['state_id']
    dec_cols_dict = p.settings['census_variables']

    dec = (
    c.get_dec_data(dec_cols_dict, census_year, 'block', 'pl', county_ids,state_id)
    .drop(columns='name')
    )

    p.save_table('dec_block_data', dec)

def run_step(context):
    """Execute the census data retrieval pipeline step.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Getting Decennial Census block data and saving to HDF5...")
    get_dec_block_data(p)
    return context