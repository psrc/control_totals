
# Where is this script
script.dir <- 'C:/Users/JKolberg/PythonProjects/control_totals/control_totals/r_scripts'
#script.dir <- '~/psrc/R/control-total-vision2050'

# Where do the data tables live
data.dir <- 'C:/Users/JKolberg/PythonProjects/control_totals/examples/legacy_luvit/data'
#data.dir <- file.path(script.dir, "data")

# Should interpolated numbers be rounded
round.interpolated <- FALSE

# Setting of the time
base.year <- 2020      # from which year to consider the main target deltas
target.year <- 2050    # end year
ref.base.year <- 2018  # first year in the dataset

# INPUTS 
# (if the files are not in data.dir, use a relative path to data.dir)
# name of the file with regional controls with intermediate years
#REFCTtable.name <- '2018_PSRC_Macroeconomic_Forecast_rev.xlsx' # used for scaling
REFCTtable.name <- NULL # use this if no scaling is desired

# name of the file with all targets (can be Excel or csv file)
target.file <- "control_id_working.xlsx"
# if the above file is an Excel file, which sheet contains the city-level targets
#target.sheet <- "control_id_working"
target.sheet <- 1

# OUTPUTS
# name of the output file
output.file.suffix <- if(is.null(REFCTtable.name)) "NoScale" else ""
output.file.name <- paste0("TargetsRebasedOutput", output.file.suffix, ".xlsx")
# name of the output file with the interpolated control totals
#ct.output.file.name <- paste0("Control-Totals-LUVit", output.file.suffix, "-", Sys.Date(), ".xlsx")
ct.output.file.name <- paste0("Control-Totals-LUVit", ".xlsx")

# for running on Hana's Mac; normally comment out
#source("hanas_paths.R") 

setwd(script.dir)
source("create_control_totals_luv3_rebased_targets.R")
