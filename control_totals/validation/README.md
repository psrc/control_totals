# Validation Dashboard

Quarto book that summarizes and validates the output of the `control_totals`
pipeline. Modeled on the [psrc/soundcast validation
dashboard](https://github.com/psrc/soundcast/tree/master/scripts/summarize/validation).

## Layout

```
validation/
├── _quarto.yml              book config + sidebar
├── index.qmd                landing / usage page
├── config.yaml              which run to validate (paths to example output/)
├── styling/custom.scss      light/dark theme tweaks
├── PSRClogo2016path.png     PSRC logo (sidebar)
└── summary_scripts/
    ├── util.py                       plotly + pandas helpers
    ├── validation_data_input.py      cached data loaders
    └── *.ipynb                       one chapter per notebook
```

## First-time setup

1. Install [Quarto](https://quarto.org/docs/get-started/).
2. From the repo root, install the Python deps with `uv sync` (the dashboard
   uses `pandas`, `plotly`, `nbformat`, `nbclient`, `nbconvert`, `pyyaml`).
3. Edit `validation/config.yaml` to point at the example whose `output/` you
   want to validate.

## Render the book

```bash
cd control_totals/validation
quarto render
```

Output lands in `validation-dashboard/index.html`. Notebooks ship with their
committed outputs (`execute.enabled: false` in `_quarto.yml`); render is
fast and does not need access to the source data.

## Re-execute notebooks

After changing data, helpers, or notebook code, re-execute everything before
rendering:

```bash
jupyter nbconvert --to notebook --execute summary_scripts/*.ipynb --inplace
quarto render
```

To re-execute a single chapter:

```bash
jupyter nbconvert --to notebook --execute summary_scripts/population.ipynb --inplace
```

## Add a new chapter

1. Drop a new `.ipynb` into `summary_scripts/`. Use any of the existing
   notebooks as a template — the first code cell is shared boilerplate that
   imports `validation_data_input` and `util`.
2. Execute the notebook so its outputs are committed.
3. Add the path to the `chapters:` list in `_quarto.yml`.
4. `quarto render`.

## Re-generate example notebooks

The four example notebooks are produced by `_build_notebooks.py` (kept in
this folder for reproducibility). Run it from the repo root if you ever
want to regenerate them from scratch:

```bash
.venv\Scripts\python.exe control_totals\validation\_build_notebooks.py
```

Once you start hand-editing the notebooks, **do not re-run that script** —
it overwrites them.

## Publishing to GitHub Pages

The site is published to a `gh-pages` branch via the
[`.github/workflows/publish-validation.yml`](../../.github/workflows/publish-validation.yml)
workflow. It runs automatically on pushes to `main` that touch
`control_totals/validation/**`, and can also be triggered manually from the
**Actions** tab. Because notebooks are pre-executed, the workflow only needs
Quarto — no Python data access required.

**One-time setup** (after the first successful workflow run creates the
`gh-pages` branch):

1. Repo Settings → Pages
2. **Source**: *Deploy from a branch*
3. **Branch**: `gh-pages`, **Folder**: `/ (root)` → Save

The site will be available at `https://<owner>.github.io/<repo>/`. Update
[`_publish.yml`](_publish.yml) to record the final URL.

**To publish manually from your machine** instead:

```bash
cd control_totals/validation
quarto publish gh-pages
```

## Pointing at a different run

Edit `config.yaml`:

```yaml
example_path: ../../examples/summer_2026   # required
legacy_path:  ../../examples/legacy_luvit  # optional, comparison source
```

Paths are resolved relative to this folder. Set `legacy_path: null` to skip
legacy-LUVit comparisons.
