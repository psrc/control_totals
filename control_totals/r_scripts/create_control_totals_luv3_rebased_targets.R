# Creates a dataset with jurisdictional control totals.
# Given input targets, it does the following:
#  - sets the end year values for HHPop and HH and interpolates to intermediate years
#  - unrolls the totals into a long format
# The script should be called from run_creating_control_totals_from_targets.R
# Hana Sevcikova, PSRC
# 11/08/2022

library(data.table)
library(openxlsx)
library(tools)

# read city-level target file
CityData <- if(file_ext(target.file) == "csv") fread(file.path(data.dir, target.file)) else data.table(read.xlsx(file.path(data.dir, target.file), 
                                                                                            sheet = target.sheet, startRow = 1))

# select columns needed (make sure the column names are correctly aligned with the BY and target names)
CityGroEmp <- CityData[, .(county_id, RGID, control_id, EmpBY = Emp18, EmpBase = TotEmp20_wCRnoMil,
                             EmpGro = TotEmpTrg_wCRnoMil, EmpTarget = TotEmp50_wCRnoMil)]
CityGroPop <- CityData[, .(county_id, RGID, control_id, 
                                      PopBY = Pop18, PopBase = TotPop20,
                                      HHPopBY =  HHpop18, HHPopBase = HHpop20,
                                      HHBY = HH18, HHBase = HH20,
                                      PopGro = TotPopTrg, PopTarget = TotPop50, 
                                      GQpctTarget = GQpct50, PPHTarget = PPH50
                                    )] 

# compute end points for HHPop and HH
CityGroPop[, HHPopTarget := PopTarget - PopTarget * GQpctTarget/100]
CityGroPop[, HHTarget := HHPopTarget/PPHTarget]
CityGroPop[is.na(HHTarget), HHTarget := 0]

# add HH ad HHPop growth column
CityGroPop[, HHGro := HHTarget - HHBase]
CityGroPop[, HHPopGro := HHPopTarget - HHPopBase]

# add Juris name
CityGroEmp[CityData, Juris := i.name, on = "control_id"]
CityGroPop[CityData, Juris := i.name, on = "control_id"]

#Sum over RGs
RGSTarget <- merge(CityGroPop[, .(PopDelta = sum(PopTarget - PopBase), PopTarget = sum(PopTarget), 
                                HHDelta = sum(HHTarget - HHBase), HHTarget = sum(HHTarget)), by = .(county_id, RGID)], 
               CityGroEmp[, .(EmpDelta = sum(EmpTarget - EmpBase), EmpTarget = sum(EmpTarget)), by = .(county_id, RGID)], 
              by = c("county_id", "RGID"))

# Select the right columns in the right order for outputs
CityRGSEmp <- CityGroEmp[, .(RGID, county_id, control_id, Juris, EmpBY, EmpBase, EmpGro, EmpTarget)]
CityRGSPop <- CityGroPop[, .(RGID, county_id, control_id, Juris, PopBY, PopBase, PopGro, PopTarget, HHPopBY, HHPopBase, HHPopGro, HHPopTarget)]
CityRGSHH <- CityGroPop[, .(RGID, county_id, control_id, Juris, HHBY, HHBase, HHGro, HHTarget)]

# Rename time columns
target.year.short <- substr(as.character(target.year), 3,4)
growth.suffix <- paste0(substr(as.character(base.year), 3,4), target.year.short)

setnames(CityRGSPop, c("PopBY", "PopBase", "PopGro", "PopTarget", "HHPopBY", "HHPopBase", "HHPopGro", "HHPopTarget"),
         c(paste0("Pop", ref.base.year), paste0("Pop", base.year), paste0("PopGro", growth.suffix), paste0("Pop", target.year), 
           paste0("HHPop", ref.base.year), paste0("HHPop", base.year), paste0("HHPopGro", growth.suffix), paste0("HHPop", target.year)))
setnames(CityRGSEmp, c("EmpBY", "EmpBase", "EmpGro", "EmpTarget"),
         c(paste0("Emp", ref.base.year), paste0("Emp", base.year), paste0("EmpGro", growth.suffix), paste0("Emp", target.year)))
setnames(CityRGSHH, c("HHBY", "HHBase", "HHGro", "HHTarget"),
         c(paste0("HH", ref.base.year), paste0("HH", base.year), paste0("HHGro", growth.suffix), paste0("HH", target.year)))
setnames(RGSTarget, c("PopDelta", "PopTarget", "HHDelta", "HHTarget", "EmpDelta", "EmpTarget"),
         c(paste0("Pop", growth.suffix), paste0("Pop", target.year.short),
           paste0("HH", growth.suffix), paste0("HH", target.year.short),
           paste0("Emp", growth.suffix), paste0("Emp", target.year.short)))

#Export 
if(!is.null(output.file.name)) {
    output <- list("RGs" = RGSTarget, "CityPop" = CityRGSPop, "CityHH" = CityRGSHH,  "CityEmp" = CityRGSEmp)
    write.xlsx(output, output.file.name, colNames = TRUE)
}

# Interpolate (this could go into a separate R script)
source("interpolate.R")

ankers <- c(ref.base.year, base.year, target.year)
years.to.fit <- c(ankers[1], seq(base.year, 2040, by = 5), 2044, 2050) 

if(!is.null(REFCTtable.name)) {
    # read regional totals for adjustments
    if(file_ext(REFCTtable.name) == "xlsx"){ # assuming reading directly from the REF final product sheet, thus needs some pre-processing
        refall <- data.table(read.xlsx(file.path(data.dir, REFCTtable.name), sheet = "Forecast"))
        cols <- paste0(years.to.fit[years.to.fit > base.year], "Q2")
        regtot <- data.frame(Pop = t(refall[X1 == "Population", cols, with = FALSE]),
                            HHPop = t(refall[X2 == "Household Population", cols, with = FALSE]),
                            HH = t(refall[X1 == "Households", cols, with = FALSE]),
                            Emp = t(refall[X1 == "Estimated Total Employment", cols, with = FALSE]))
        rownames(regtot) <- substr(cols, 1, 4)
    } else {
        regtot <- read.csv(file.path(data.dir, REFCTtable.name), header = TRUE)
        rt.years <- regtot[, "Year"]
        rownames(regtot) <- rt.years
        regtot <- subset(regtot, Year > base.year)
        regtot$Year <- NULL
    }
} else regtot <- NULL

to.interpolate <- list(HHPop = CityRGSPop, HH = CityRGSHH, Emp = CityRGSEmp, Pop = CityRGSPop)
CTs <- list()
unrolled <- NULL

for (indicator in names(to.interpolate)) {
    RCT <- if(is.null(regtot)) NULL else regtot[, indicator]
    names(RCT) <- rownames(regtot)
    CTs[[indicator]] <- interpolate.controls.with.ankers(to.interpolate[[indicator]][order(control_id)], indicator, 
                                             anker.years = ankers, years.to.fit = years.to.fit,
                                             totals = RCT)
    this.unrolled <- unroll(CTs[[indicator]], indicator, totals = RCT, new.id.col = "subreg_id")
    unrolled <- if(is.null(unrolled)) this.unrolled else merge(unrolled, this.unrolled, all = TRUE)
}

CTs[["unrolled"]] <- unrolled

# interpolate all years and aggregate to regional totals
years.to.fit <- ankers[1]:2050 
unrolled.all <- NULL

for (indicator in names(to.interpolate)) {
    RCT <- if(is.null(regtot)) NULL else regtot[, indicator]
    names(RCT) <- rownames(regtot)
    ct.all <- interpolate.controls.with.ankers(to.interpolate[[indicator]][order(control_id)], indicator, 
                                                         anker.years = ankers, years.to.fit = years.to.fit,
                                                         totals = RCT)
    this.unrolled <- unroll(ct.all, indicator, totals = RCT, new.id.col = "subreg_id")
    unrolled.all <- if(is.null(unrolled.all)) this.unrolled else merge(unrolled.all, this.unrolled, all = TRUE)
}
# aggregate
cols <- setdiff(colnames(unrolled.all), c("year", "subreg_id"))
unrolled.reg <- unrolled.all[, lapply(.SD, sum), by = .(year), .SDcols = cols] 

CTs[["unrolled_regional"]] <- unrolled.reg

if(!is.null(ct.output.file.name)) 
    write.xlsx(CTs, ct.output.file.name, colNames = TRUE)

