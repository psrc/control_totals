# Control Totals

This repository uses county growth targets to create Control Totals for PSRC's land use model Urbansim.


## Installation
1. Install UV package manager 

    `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"` 

2. Connect to PSRC VPN before running so the pipeline can load data from Elmer and network drives

#### Current control totals
3. Create a new example by copying examples/summer_2026 or just make modifications to settings in examples/summer_2026/configs/settings.yaml

4. Run the control totals creation pipeline using -c "<configs_dir>" cmd line arg

    `.venv\Scripts\Activate`
    
    `python control_totals\run.py -c "<path to control_totals repo>\examples\summer_2026\configs"`

#### Legacy control totals

3. Add your census api key to your systems env variables and name it CENSUS_KEY

4. Download and install R: https://cloud.r-project.org/

5. Install needed R packages from the R terminal:

    `install.packages("data.table","openxlsx","RMySQL","ggplot2","raster")`

6. Update file paths in the .R files in r_scripts/

7. Copy creds.txt into r_scripts (creds.txt contains username and password for urbansim base year mysql database)

8. Run the control totals creation pipeline using -c "<configs_dir>" cmd line arg

    `.venv\Scripts\Activate`
    
    `python control_totals\run.py -c "<path to control_totals repo>\examples\legacy_luvit\configs"`


