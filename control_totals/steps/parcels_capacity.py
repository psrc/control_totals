import pandas as pd
from pathlib import Path
from control_totals.util import Pipeline

BASE_CAPACITY_COLUMNS = [
    'DUbase', 'DUcapacity',
    'NRSQFbase', 'NRSQFcapacity',
    'JOBSPbase', 'JOBSPcapacity',
    'BLSQFbase', 'BLSQFcapacity',
]
REQUIRED_COLUMNS = ['parcel_id', 'control_id', 'subreg_id'] + BASE_CAPACITY_COLUMNS


def load_capacity(prop_path):
    """Load the parcel-level capacity CSV.

    The file holds one row per parcel with the parcel's geographic
    identifiers (``plan_type_id``, ``county_id``, ``city_id``,
    ``growth_center_id``, ``control_id``, ``tod_id``, ``subreg_id``,
    ``hb_hct_buffer``, ``hb_tier``) and base-year / capacity values for
    dwelling units (``DU``), non-residential sqft (``NRSQF``), job spaces
    (``JOBSP``) and building sqft (``BLSQF``).

    Args:
        prop_path (str or Path): Path to the capacity CSV.

    Returns:
        pandas.DataFrame: The capacity table.

    Raises:
        ValueError: If required columns are missing or ``parcel_id`` is
            not unique.
    """
    capacity = pd.read_csv(Path(prop_path), low_memory=False)
    missing = [c for c in REQUIRED_COLUMNS if c not in capacity.columns]
    if missing:
        raise ValueError(f'Capacity file {prop_path} is missing columns: {missing}')
    if capacity['parcel_id'].duplicated().any():
        raise ValueError(f'Capacity file {prop_path} has duplicate parcel_id values')
    return capacity


def update_ids(result, parcels_hct):
    """Update control_id and subreg_id."""
    result = result.drop(columns=['control_id', 'subreg_id'], errors='ignore')
    return result.merge(parcels_hct[['parcel_id', 'control_id', 'subreg_id']], on='parcel_id', how='left')


def run_step(context):
    """Execute the parcels capacity pipeline step.

    Reads the parcel-level capacity CSV configured as ``prop_path`` under
    ``parcels_capacity`` in settings.yaml, refreshes ``control_id`` and
    ``subreg_id`` from the ``parcels_hct`` table when available, and saves
    the result to CSV and the pipeline HDF5 store.

    Expected settings.yaml block::

        parcels_capacity:
          prop_path: "path/to/CapacityPcl.csv"
          save_csv: true
          file_prefix: "CapacityPclNoSampling_res50"

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    print('Computing parcel capacity...')
    p = Pipeline(settings_path=context['configs_dir'])

    cfg = p.settings['parcels_capacity']
    prop_path = cfg['prop_path']
    save_csv = cfg.get('save_csv', True)
    file_prefix = cfg.get('file_prefix', 'CapacityPclNoSampling_res50')

    result = load_capacity(prop_path)
    if p.check_table_exists('parcels_hct'):
        result = update_ids(result, p.get_geodataframe('parcels_hct'))
    if save_csv:
        out_path = Path(p.get_output_dir()) / f'{file_prefix}.csv'
        result.to_csv(out_path, index=False)
        print(f'  Saved capacity CSV to {out_path}')

    p.save_table('parcels_capacity', result)
    print(f'  {len(result):,} parcels with capacity data')
    return context
