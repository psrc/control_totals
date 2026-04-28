import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[2]


def run_r_script(script_path):
    """Run an R script via ``Rscript`` and stream its output.

    Resolves the script path relative to the package base directory,
    launches it with ``subprocess.Popen``, and prints stdout/stderr
    line-by-line.  Reports non-zero exit codes to stderr.

    Args:
        script_path (str): Path to the R script, relative to the
            package base directory.
    """
    resolved_script_path = (BASE_DIR / script_path).resolve()
    command = ['Rscript', str(resolved_script_path)]

    try:
        with subprocess.Popen(
            command,
            cwd=BASE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        ) as process:
            if process.stdout is not None:
                for line in process.stdout:
                    print(line, end='', flush=True)

            return_code = process.wait()

        if return_code != 0:
            print(
                f'R script failed with exit code {return_code}: {resolved_script_path}',
                file=sys.stderr,
                flush=True,
            )
    except FileNotFoundError:
        print(
            "Error: Rscript not found. Make sure R is installed and in your system's PATH.",
            file=sys.stderr,
            flush=True,
        )


def run_step(context):
    """Execute the R-based parcel capacity pipeline step.

    Runs ``parcels_capacity.R``.

    Args:
        context (dict): The pypyr context dictionary.

    Returns:
        dict: The unchanged pypyr context dictionary.
    """
    print("Running parcels_capacity.R...")
    run_r_script('r_scripts/parcels_capacity.R')
    
    return context