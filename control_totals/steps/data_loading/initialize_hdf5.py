from pathlib import Path

from util import Pipeline


def run_step(context):
	"""Execute the HDF5 initialization pipeline step.

	Deletes any existing pipeline HDF5 store so that subsequent steps
	start with a clean file.

	Args:
		context (dict): The pypyr context dictionary, expected to contain
			a ``'configs_dir'`` key.

	Returns:
		dict: The unchanged pypyr context dictionary.
	"""
	p = Pipeline(settings_path=context['configs_dir'])
	hdf5_path = Path(p.get_data_dir()) / 'pipeline.h5'

	if hdf5_path.exists():
		print(f"Deleting existing HDF5 store: {hdf5_path}")
		hdf5_path.unlink()
	else:
		print(f"No existing HDF5 store found at: {hdf5_path}")

	return context
