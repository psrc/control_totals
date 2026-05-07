"""Pipeline step: execute validation notebooks and render the Quarto book."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


VALIDATION_DIR = Path(__file__).resolve().parent.parent / 'validation'
SUMMARY_SCRIPTS_DIR = VALIDATION_DIR / 'summary_scripts'


def _execute_notebooks() -> None:
    """Execute every .ipynb in validation/summary_scripts in place."""
    notebooks = sorted(SUMMARY_SCRIPTS_DIR.glob('*.ipynb'))
    if not notebooks:
        print(f"No notebooks found in {SUMMARY_SCRIPTS_DIR}.")
        return

    for nb in notebooks:
        print(f"Executing notebook: {nb.relative_to(VALIDATION_DIR)}")
        subprocess.run(
            [
                sys.executable,
                '-m', 'nbconvert',
                '--to', 'notebook',
                '--execute',
                '--inplace',
                str(nb),
            ],
            cwd=str(VALIDATION_DIR),
            check=True,
        )


def _quarto_render() -> None:
    """Run `quarto render` from the validation directory."""
    quarto = shutil.which('quarto')
    if quarto is None:
        raise RuntimeError(
            "Could not find the 'quarto' CLI on PATH. Install Quarto from "
            "https://quarto.org/docs/get-started/ before running this step."
        )
    print(f"Running 'quarto render' in {VALIDATION_DIR}")
    subprocess.run([quarto, 'render'], cwd=str(VALIDATION_DIR), check=True)


def run_step(context: dict):
    """Pipeline step entry point: build the validation dashboard.

    Executes all notebooks in ``control_totals/validation/summary_scripts``
    (in place) and then runs ``quarto render`` against
    ``control_totals/validation``.

    Args:
        context (dict): pypyr context dictionary.

    Returns:
        dict: The unchanged context dictionary.
    """
    print("Building validation dashboard...")
    _execute_notebooks()
    _quarto_render()
    return context
