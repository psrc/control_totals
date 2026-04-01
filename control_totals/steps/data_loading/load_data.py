from pathlib import Path
import shutil

import pandas as pd

from util import Pipeline


def get_required_input_files(pipeline):
    """Return a list of file names required by the configured data and target tables.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.

    Returns:
        list[str]: File names referenced in ``data_tables`` and
            ``targets_tables`` settings.
    """
    p = pipeline
    configured_tables = p.settings.get('data_tables', []) + p.settings.get('targets_tables', [])
    return [table['file'] for table in configured_tables if table.get('file')]


def ensure_required_input_files(pipeline):
    """Verify that all required input CSV files exist, copying from backup if needed.

    If a required file is missing from the data directory, attempts to copy
    it from the ``tables_backup_dir`` setting. Raises an error listing any
    files that could not be found.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and data directory paths.

    Raises:
        FileNotFoundError: If any required files are missing and cannot be
            copied from the backup directory.
    """
    p = pipeline
    data_dir = Path(p.get_data_dir())
    backup_dir_setting = p.settings.get('tables_backup_dir')
    backup_dir = Path(backup_dir_setting) if backup_dir_setting else None

    missing_from_backup = []
    for file_name in get_required_input_files(p):
        data_file = data_dir / file_name
        if data_file.exists():
            continue

        if backup_dir is None:
            missing_from_backup.append(str(data_file))
            continue

        backup_file = backup_dir / file_name
        if not backup_file.exists():
            missing_from_backup.append(str(data_file))
            continue

        data_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(backup_file, data_file)
        print(f"Copied missing input file from backup: {backup_file} -> {data_file}")

    if missing_from_backup:
        raise FileNotFoundError(
            "Required input files were not found in the data directory and could not be copied from "
            f"tables_backup_dir: {missing_from_backup}"
        )

def load_data_tables_to_hdf5(pipeline):
    """Load general data tables from CSV files into the pipeline HDF5 store.

    Reads each table listed in ``data_tables`` settings, validates its
    columns, and saves it to the HDF5 store.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and the save interface.
    """
    # load general data tables in the data_tables list in settings.yaml
    p = pipeline
    data_tables = p.settings.get('data_tables', [])
    for table in data_tables:
        table_name = table['name']
        file_path = f"{p.get_data_dir()}/{table['file']}"
        print(f"Loading {file_path} into HDF5 as {table_name}...")
        df = pd.read_csv(file_path)
        
        # check that the correct columns are present
        data_check_tables(df, table_name)

        # save to HDF5
        p.save_table(table_name, df)

def data_check_tables(df, table_name):
    """Validate required columns in a general data table.

    Args:
        df (pandas.DataFrame): The loaded DataFrame to validate.
        table_name (str): The name of the table for error messages.

    Raises:
        ValueError: If the ``control_areas`` table is missing a
            ``control_id`` column.
    """
    if table_name == 'control_areas':
        if 'control_id' not in df.columns:
            raise ValueError("control_areas table must have control_id column.")

def add_county_id(df, county_name):
    """Add a ``county_id`` FIPS code column based on the county name.

    Args:
        df (pandas.DataFrame): The DataFrame to augment.
        county_name (str): Lowercase county name (e.g. ``'king'``).

    Returns:
        pandas.DataFrame: The input DataFrame with an added ``county_id``
            column.
    """
    county_ids = {
        'king':53033,
        'kitsap':53035,
        'pierce':53053,
        'snohomish':53061,
    }
    return df.assign(county_id=county_ids[county_name])

def load_targets_to_hdf5(pipeline):
    """Load county growth-target CSV files into the pipeline HDF5 store.

    Reads each table listed in ``targets_tables`` settings, renames columns
    according to the configured mapping, validates required columns, adds
    a county ID, and saves to the HDF5 store.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings
            and the save interface.
    """
    # load target tables for each county
    p = pipeline
    targets_end_year = p.settings['targets_end_year']
    for table in p.settings['targets_tables']:
        table_name = table['name']
        file_path = f"{p.get_data_dir()}/{table['file']}"
        print(f"Loading {file_path} into HDF5 as {table_name}...")
        df = pd.read_csv(file_path)
        
        # rename columns based on settings
        for col in ['total_pop_chg_col', 'units_chg_col', 'emp_chg_col']:
            if col in table:
                df.rename(columns={table[f'{col}']: col.replace('_col', '')}, inplace=True, errors='ignore')

        for col in ['total_pop_col', 'units_col', 'emp_col']:
            if col in table:
                df.rename(columns={table[f'{col}']: col.replace('col', f'{targets_end_year}')}, inplace=True, errors='ignore')
        
        # check that base year data exists for years specified in targets table settings
        check_base_year_data_exists(pipeline,table)

        # check that the correct columns are present
        data_check_targets(df, table_name)
        
        # add county_id column
        county_name = table_name.lower().split("_")[0]
        df = add_county_id(df, county_name)

        # save to HDF5
        p.save_table(table_name, df)


def check_exists(chg_col,targets_table, type, data_table_names):
    """
    Checks if the required data table exists for a given change column and type.

    Parameters:
        chg_col (str): The column name to check in the targets_table.
        targets_table (dict): Dictionary containing table info from settings.yaml.
        type (str): The type of data to check ('emp' for employment, 'ofm' for OFM estimates).
        data_table_names (list): List of available data table names.

    Raises:
        ValueError: If the required data table for the specified type and start year is not found in data_table_names.
    """
    if chg_col in targets_table:
        start_year = targets_table[chg_col]
        if type == 'emp':
            table_name = f'employment_{start_year}_by_control_area'
        elif type == 'ofm':
            table_name = f'ofm_parcelized_{start_year}'
        if table_name not in data_table_names:
            raise ValueError(f"{type} data for start year {start_year} not found in data_tables in settings.yaml.")

def check_base_year_data_exists(pipeline, targets_table):
    """Verify that base-year employment and OFM data tables exist for a targets table.

    Checks that the required employment and OFM parcelized tables are
    listed in the pipeline settings for the start years referenced in
    the targets table.

    Args:
        pipeline (Pipeline): The data pipeline providing access to settings.
        targets_table (dict): A single targets-table entry from settings
            containing start-year keys.

    Raises:
        ValueError: If a required data table is missing from settings.
    """
    p = pipeline

    # check that employment data exists for emp_chg_start year
    data_table_names = [table['name'] for table in p.get_data_table_list()]
    check_exists('emp_chg_start',targets_table,'emp',data_table_names)

    # check that ofm data exists for total_pop_chg_start and units_chg_start years
    elmer_table_names = [table['name'] for table in p.get_elmer_list()]
    check_exists('total_pop_chg_start',targets_table,'ofm',elmer_table_names)
    check_exists('units_chg_start',targets_table,'ofm',elmer_table_names)


def data_check_targets(df, table_name):
    """Validate required columns in a county targets table.

    Each targets table must have ``emp_chg``, ``target_id``, and either
    ``units_chg`` or ``total_pop_chg``.

    Args:
        df (pandas.DataFrame): The loaded DataFrame to validate.
        table_name (str): The name of the table for error messages.

    Raises:
        ValueError: If any required column is missing.
    """
    # each targets table should have either units_chg or total_pop_chg
    # and each should have emp_chg and target_id
    if 'emp_chg' not in df.columns:
        raise ValueError(f"{table_name} must have emp_chg column.")
    if 'target_id' not in df.columns:
        raise ValueError(f"{table_name} must have target_id column.")
    if 'units_chg' not in df.columns and 'total_pop_chg' not in df.columns:
        raise ValueError(f"{table_name} must have either units_chg or total_pop_chg column.")
    

def run_step(context):
    """Execute the data-loading pipeline step.

    Ensures required input files are present, then loads data tables and
    county growth targets from CSV files into the pipeline HDF5 store.

    Args:
        context (dict): The pypyr context dictionary, expected to contain
            a ``'configs_dir'`` key.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Loading data tables from CSV files into HDF5...")
    ensure_required_input_files(p)
    load_data_tables_to_hdf5(p)
    load_targets_to_hdf5(p)
    return context