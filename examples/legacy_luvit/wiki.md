= Legacy LUVit Control Totals Process =

This page documents the '''legacy_luvit''' control totals pipeline, which transforms census data, employment estimates, and county growth targets into final control totals for PSRC's Land Use Vision (LUVit) model. The pipeline is configured via <code>settings.yaml</code> and executed as a series of ordered steps.

== Overview ==

The pipeline follows five phases:

# '''Data Loading''' — Fetch external data from PSRC's Elmer database, Census API, and local CSV files; store in an HDF5 pipeline cache.
# '''Geoprocessing''' — Build spatial crosswalks between census blocks, parcels, and control areas.
# '''Target Adjustment''' — Calibrate county growth targets to the base year (2020) using observed estimates.
# '''Extrapolation & Control Totals''' — Project targets to the control totals horizon year (2050) and assemble the final control totals table.
# '''R Script Processing''' — Run R scripts to interpolate interim years, compute parcel capacity, and split control totals into HCT/non-HCT components.

=== Data Flow Diagram ===

<pre>
                         ┌──────────────┐
                         │  Census API  │
                         └──────┬───────┘
                                │
                                ▼
                     ┌─────────────────────┐
                     │  dec_block_data      │
                     └──────────┬──────────┘
                                │ aggregate by blocks
                                ▼
┌──────────────┐     ┌─────────────────────┐     ┌────────────────────────┐
│  Elmer DB    │────▶│  OFM / Employment   │     │  County Targets (CSV)  │
│  (ElmerGeo)  │     │  by Control Area    │     └───────────┬────────────┘
└──────────────┘     └──────────┬──────────┘                 │
                                │                            │ adjust to base year
        aggregate by parcels    │                            ▼
                                │              ┌─────────────────────────┐
                                │              │  Adjusted Targets       │
                                │              └───────────┬─────────────┘
                                │                          │ extrapolate to 2050
                                │                          ▼
                                │              ┌─────────────────────────┐
                                └─────────────▶│  Control Totals Table   │
                                               └───────────┬─────────────┘
                                                           │ export to Excel
                                                           ▼
                                               ┌─────────────────────────┐
                                               │  R Scripts              │
                                               │  • Interpolation        │
                                               │  • Parcel capacity      │
                                               │  • HCT split            │
                                               └─────────────────────────┘
</pre>

== Configuration ==

Key settings from <code>settings.yaml</code>:

{| class="wikitable"
! Setting !! Value !! Description
|-
| <code>base_year</code> || 2020 || Reference year; estimates are anchored here
|-
| <code>targets_end_year</code> || 2044 || Horizon year for county growth targets
|-
| <code>end_year</code> || 2050 || Final year for control totals output
|-
| <code>target_types</code> || — || County assignments by methodology (unit change, total pop change, King County method)
|-
| <code>emp_target_types</code> || — || Employment target variants: <code>res_con</code> (Kitsap, Pierce) vs. <code>no_res_con</code> (King, Snohomish)
|-
| <code>king_cnty_method</code> || True || Use King County–specific household size and vacancy rate method
|}

----

== Phase 1: Data Loading ==

=== Step 1: Initialize HDF5 ===

<code>control_totals.steps.data_loading.initialize_hdf5</code>

Deletes any existing <code>pipeline.h5</code> file to ensure a clean slate for the run. All subsequent steps write their outputs into this HDF5 store.

=== Step 2: Get Elmer Data ===

<code>control_totals.steps.data_loading.get_elmer_data</code>

Connects to PSRC's '''Elmer''' and '''ElmerGeo''' SQL Server databases and fetches the tables defined in settings:

* '''ElmerGeo''' (geospatial): Control area polygons (<code>CONTROL18_DASHBOARD</code>), census block polygons (<code>BLOCK2020</code>, <code>BLOCK2010</code>), parcel point centroids (<code>PARCELS_URBANSIM_2018_PTS</code>). Geometry is converted to WKT for HDF5 storage.
* '''Elmer''' (tabular): OFM parcelized estimates (<code>ofm.parcelized_saep</code> for 2018, 2019, 2020), OFM block estimates (<code>ofm.v_estimates_2019</code>).

All tables are saved to the HDF5 store with ID columns cast to <code>int64</code>.

=== Step 3: Load Data ===

<code>control_totals.steps.data_loading.load_data</code>

Loads CSV files from the <code>data/</code> directory into the HDF5 store. Two categories of tables are loaded:

* '''General data tables''' — <code>control_target_xwalk</code>, <code>target_rgid_xwalk</code>, <code>ref_projection</code>, employment by control area for 2018/2019/2020.
* '''County growth targets''' — King, Kitsap, Pierce, and Snohomish target files. Column names are standardized (e.g., the configured <code>total_pop_chg_col</code> is renamed to <code>total_pop_chg</code>), and a <code>county_id</code> FIPS code is added.

Validation checks confirm that <code>control_areas</code> has a <code>control_id</code> column and that required base-year data exists for all target start years. Missing files are optionally copied from the network backup directory.

=== Step 4: Get Census Data ===

<code>control_totals.steps.data_loading.get_census_data</code>

Queries the U.S. Census Bureau API for '''Decennial PL''' (Public Law) block-level data for the four PSRC counties (King, Kitsap, Pierce, Snohomish). Fetches the following variables:

{| class="wikitable"
! Pipeline Name !! Census Variable !! Description
|-
| <code>dec_total_pop</code> || P1_001N || Total population
|-
| <code>dec_units</code> || H1_001N || Housing units
|-
| <code>dec_hh</code> || H1_002N || Occupied housing units (households)
|-
| <code>dec_gq</code> || P5_001N || Group quarters population
|}

Results are saved as <code>dec_block_data</code> in the HDF5 store.

----

== Phase 2: Geoprocessing ==

=== Step 5: Block–Control Area Crosswalk ===

<code>control_totals.steps.geoprocessing.block_control_area_xwalk</code>

Creates a spatial crosswalk between census blocks and control areas. Block polygons are converted to representative points (centroids), then spatially joined to control area polygons using <code>sjoin_nearest</code>. This handles edge cases where block centroids fall over water or outside control area boundaries.

Outputs: <code>block_control_area_xwalk</code> (and optionally <code>block_2010_control_area_xwalk</code> for historical 2010 blocks).

=== Step 6: Parcel–Control Area Crosswalk ===

<code>control_totals.steps.geoprocessing.parcel_control_area_xwalks</code>

Creates a spatial crosswalk between parcel point centroids and control areas. Uses a two-pass approach:

# Standard spatial join (<code>sjoin</code>) for parcels within control area polygons.
# <code>sjoin_nearest</code> for any unmatched parcels, assigning them to the nearest control area.

Output: <code>ofm_parcel_control_area_xwalk</code>.

----

== Phase 3: Data Preparation ==

=== Step 7: Prepare Parcel Data ===

<code>control_totals.steps.data_loading.prepare_parcel_data</code>

Aggregates parcel-level OFM estimates to the control-area level using the parcel–control area crosswalk. For each OFM vintage year (2018, 2019, 2020):

* Merges parcel data with the crosswalk (validates that every parcel has a <code>control_id</code>).
* Renames OFM columns to standardized prefixes: <code>total_pop</code>→<code>ofm_total_pop</code>, <code>household_pop</code>→<code>ofm_hhpop</code>, <code>housing_units</code>→<code>ofm_units</code>, <code>occupied_housing_units</code>→<code>ofm_hh</code>, <code>group_quarters</code>→<code>ofm_gq</code>.
* Sums by <code>control_id</code>.

Outputs: <code>ofm_parcelized_[year]_by_control_area</code> tables.

=== Step 8: Prepare Block Data ===

<code>control_totals.steps.data_loading.prepare_block_data</code>

Aggregates decennial census block data to the control-area level using the block–control area crosswalk:

* Sums census population, housing units, households, and group quarters by <code>control_id</code>.
* Derives <code>dec_hhpop = dec_total_pop − dec_gq</code>.

Output: <code>decennial_by_control_area</code>.

=== Step 9: Prepare OFM Block Data (Legacy) ===

<code>control_totals.steps.legacy.prepare_ofm_block_data</code>

Aggregates OFM block-level estimates (e.g., 2019 vintage) to the control-area level using the 2010 block crosswalk. Renames columns to <code>ofm_*</code> prefixes and computes <code>ofm_total_pop = ofm_hhpop + ofm_gq</code>.

Output: <code>ofm_block_[year]_by_control_area</code>.

----

== Phase 4: Target Calculations ==

=== Step 10: Adjust Targets to Decennial ===

<code>control_totals.steps.legacy.adjust_targets_to_decennial</code>

Adjusts raw county growth-change targets so they are relative to the 2020 base year instead of each county's original target start year. For each target:

# Calculates the estimated change between the target start year and the base year using OFM/employment estimates. '''Note:''' 2020 decennial data is substituted for OFM 2020 for population/housing indicators.
# Subtracts that observed change from the raw target: <code>adjusted_change = raw_target_change − (base_year_estimate − start_year_estimate)</code>.
# Population and housing targets are clipped to zero (no negative growth). Employment targets may remain negative.

Outputs: <code>adjusted_[type]_change_targets</code> tables (population, units, employment with resource/construction).

=== Step 11: Adjust Employment Targets (No Resource/Construction) ===

<code>control_totals.steps.legacy.adjust_emp_targets_no_res_con_to_base_year</code>

For King and Snohomish counties, whose employment targets '''exclude''' resource and construction sectors, this step performs a more complex base-year adjustment:

# Calculates employment change (excluding military) from the target start year to the base year.
# Estimates the resource/construction share of total employment at each control area.
# Computes resource/construction job change and normalizes to county-level hard-coded targets from settings (<code>resource_construction_emp_targets</code>).
# If the adjusted employment goes negative, falls back to the original unadjusted target.

Output: <code>adjusted_emp_change_targets_no_res_con</code>.

=== Step 12: King County Targets ===

<code>control_totals.steps.king_cnty_targets</code>

Implements the King County–specific methodology, which uses '''housing unit change''' targets combined with hard-coded household sizes, vacancy rates, and a regional household population control total:

# Aggregates unit-change and population-change targets by RGID (Regional Growth ID).
# Applies per-RGID vacancy rates (<code>king_vac</code>) to derive households from housing units: <code>hh = units × (1 − vacancy_rate)</code>.
# Applies per-RGID household sizes (<code>king_hhsz</code>) to derive initial household population.
# '''Regional factoring:''' Scales all RGIDs so that total King County household population matches the configured <code>king_hhpop_2044</code> target.
# Distributes RGID-level totals back to individual target areas using decennial household-size ratios.
# Household sizes are capped at 5.0 (falls back to RGID-level value if exceeded).
# Calculates group quarters using regional REF projection GQ shares, then derives total population.

Output: <code>adjusted_king_targets</code>.

=== Step 13: Total Population Change Targets ===

<code>control_totals.steps.total_pop_chg_targets</code>

Calculates population and housing targets for counties using the '''total population change''' methodology (Kitsap, Pierce, Snohomish):

# <code>total_pop = dec_total_pop + total_pop_chg_adj</code>
# Calculates group quarters using the regional REF projection GQ percentage.
# <code>hhpop = total_pop − gq</code>
# Computes a target-area household size using the ratio of REF projection household size to decennial regional household size, capped at 5.0.
# <code>hh = hhpop / hhsz</code>

Output: <code>adjusted_total_pop_change_targets</code>.

=== Step 14: Employment Change Targets ===

<code>control_totals.steps.legacy.emp_chg_targets</code>

Combines employment targets from both target types (with and without resource/construction) and calculates horizon-year employment:

<code>emp_[targets_end_year] = emp_[base_year] + emp_chg_adj</code>

Merges both the <code>res_con</code> and <code>no_res_con</code> employment targets into a single table with the base-year and target-year employment columns.

Output: <code>adjusted_emp_change_targets_calculations</code>.

=== Step 15: Adjust Snohomish Employment by RGID ===

<code>control_totals.steps.legacy.adjust_snohomish_emp_targets_by_rgid</code>

Applies hard-coded Snohomish County employment totals by RGID to ensure consistency with externally set targets:

# Isolates Snohomish County (FIPS 53061) records.
# Computes preliminary employment sums by RGID.
# Calculates an adjustment ratio: <code>hard-coded RGID target ÷ sum of preliminary estimates</code>.
# Multiplies each control area's employment by its RGID ratio.
# Leaves other counties unchanged.

Hard-coded RGID targets from settings (<code>snohomish_emp_target_totals</code>):
{| class="wikitable"
! RGID !! Target Employment
|-
| 1 || 164,981
|-
| 2 || 78,069
|-
| 3 || 152,580
|-
| 4 || 42,888
|-
| 5 || 22,350
|-
| 6 || 30,260
|}

Output: Updated <code>adjusted_emp_change_targets_calculations</code>.

----

== Phase 5: Extrapolation & Final Controls ==

=== Step 16: Extrapolate to Controls Year ===

<code>control_totals.steps.legacy.extrapolate_to_controls_year</code>

Extends targets from the targets horizon year (2044) to the final control totals year (2050) using linear extrapolation:

# Merges population, housing, and employment target tables.
# For each indicator (hh, total_pop, emp):
#* Computes annual change: <code>(target_year_value − base_year_value) / years_elapsed</code>
#* Extends to 2050: <code>base_year_value + annual_change × years_to_2050</code>
# Calculates group quarters for the control year using REF projection shares.
# Derives household population and household size for the extrapolated year.

Output: <code>extrapolated_targets</code>.

=== Step 17: Create Controls ===

<code>control_totals.steps.legacy.create_controls</code>

Assembles the final control totals table and exports it to Excel for the downstream R scripts:

# '''Merges''' extrapolated targets with base-year decennial census, OFM estimates, and employment data by <code>control_id</code>.
# '''Handles excluded areas''' (e.g., military bases flagged with <code>exclude_from_target == 1</code>):
#* Resets horizon-year values to base-year values for excluded areas.
#* Subtracts excluded area values from sibling control areas within the same target group.
# '''Applies employment overrides''' from settings (<code>emp_target_overrides</code>), extrapolating overridden values to the control year.
# '''Renames columns''' to legacy R-compatible names (e.g., <code>dec_total_pop</code>→<code>TotPop20</code>, <code>hh_2050</code>→<code>HH50</code>).
# '''Derives additional fields:'''
#* <code>TotEmpTrg_wCRnoMil = emp_2050 − emp_2020</code>
#* <code>TotPopTrg = total_pop_2050 − total_pop_2020</code>
#* <code>GQpct50 = GQ50 / TotPop50</code>
#* <code>PPH50 = HHpop50 / HH50</code>
# '''Exports''' to <code>control_id_working.xlsx</code> in the output directory.

Output: <code>control_totals</code> table (HDF5) and <code>control_id_working.xlsx</code> (Excel).

----

== Phase 6: R Script Processing ==

=== Step 18: Run Parcel Capacity R Script ===

<code>control_totals.steps.legacy.run_parcel_capacity_r_script</code>

Invokes <code>parcels_capacity.R</code> via <code>Rscript</code> to calculate development capacity at the parcel level:

* '''Inputs:''' Base-year buildings and parcels, UrbanSim development project proposals and components, lookup tables (constraints, templates, sqft-per-job).
* '''Logic:'''
** Filters proposals with meaningful capacity increases above base-year stock.
** Separates parcels into residential-only, non-residential-only, and mixed-use categories.
** For mixed-use parcels, applies a configurable residential ratio (default 50%).
** Selects the maximum proposal per parcel for each use type.
* '''Output:''' <code>CapacityPclNoSampling_res50.csv</code> with columns for base-year and capacity values for dwelling units, non-residential sqft, job spaces, and building sqft, along with geographic identifiers (<code>control_id</code>, <code>subreg_id</code>, <code>tod_id</code>).

=== Step 19: Run Control Totals R Scripts ===

<code>control_totals.steps.legacy.run_r_scripts</code>

Runs two R scripts sequentially:

==== 19a: Create Control Totals from Targets ====

<code>run_creating_control_totals_from_targets.R</code> → <code>create_control_totals_luv3_rebased_targets.R</code>

* '''Input:''' <code>control_id_working.xlsx</code> (from Step 17).
* '''Logic:'''
** Reads jurisdiction-level base (2018, 2020) and target (2050) values for households, population, and employment.
** Computes growth deltas and aggregates by RGID.
** '''Linearly interpolates''' between anchor years (2018 → 2020 → 2050) to fill in all interim years.
** Optionally scales interpolated values to match a regional reference projection.
** Applies rounding and row-sum balance corrections.
** Outputs data in both wide format (one row per geography) and unrolled long format (one row per geography-year).
* '''Outputs:'''
** <code>TargetsRebasedOutput.xlsx</code> — Rebased targets with sheets for RGs, CityPop, CityHH, CityEmp.
** <code>Control-Totals-LUVit.xlsx</code> — Interpolated control totals for all years (2018–2050).

==== 19b: Split Control Totals to HCT ====

<code>split_ct_to_hct.R</code>

* '''Inputs:''' <code>Control-Totals-LUVit.xlsx</code>, <code>CapacityPclNoSampling_res50.csv</code>, base-year data (HH, jobs, persons by subreg_id split into HCT/non-HCT).
* '''Logic:'''
** Computes HCT capacity shares for each jurisdiction from parcel capacity data.
** '''Iteratively scales''' HCT growth targets to achieve regional share goals:
*** Households: 65% in HCT areas.
*** Employment: 75% in HCT areas.
** Uses weighted increments by Regional Growth classification (metro fastest, HCT Communities slowest).
** Enforces minimum non-HCT growth shares as constraints for each RG.
** Handles capacity overflow: if non-HCT areas hit capacity limits, remainder is redirected to HCT.
** Adjusts persons-per-household (PPH) to maintain appropriate density relationships between HCT and non-HCT areas.
** Generates diagnostic PDF plots showing iteration convergence and final shares by city.
* '''Output:''' Excel workbook with HH/HHPop/Emp worksheets, unrolled stepped years (2020, 2025, 2030, 2035, 2040, 2044, 2050), regional aggregations, and QA check sheets.

----

== Pipeline Step Summary ==

{| class="wikitable"
! # !! Step Module !! Phase !! Description
|-
| 1 || <code>data_loading.initialize_hdf5</code> || Data Loading || Delete existing HDF5 store
|-
| 2 || <code>data_loading.get_elmer_data</code> || Data Loading || Fetch geospatial and tabular data from Elmer
|-
| 3 || <code>data_loading.load_data</code> || Data Loading || Load CSV reference tables and county targets
|-
| 4 || <code>data_loading.get_census_data</code> || Data Loading || Fetch decennial census block data from API
|-
| 5 || <code>geoprocessing.block_control_area_xwalk</code> || Geoprocessing || Spatially join blocks to control areas
|-
| 6 || <code>geoprocessing.parcel_control_area_xwalks</code> || Geoprocessing || Spatially join parcels to control areas
|-
| 7 || <code>data_loading.prepare_parcel_data</code> || Preparation || Aggregate OFM parcel data to control areas
|-
| 8 || <code>data_loading.prepare_block_data</code> || Preparation || Aggregate census block data to control areas
|-
| 9 || <code>legacy.prepare_ofm_block_data</code> || Preparation || Aggregate OFM block data to control areas
|-
| 10 || <code>legacy.adjust_targets_to_decennial</code> || Targets || Adjust growth targets to 2020 base year
|-
| 11 || <code>legacy.adjust_emp_targets_no_res_con_to_base_year</code> || Targets || Adjust employment targets (excl. resource/construction)
|-
| 12 || <code>king_cnty_targets</code> || Targets || King County unit-change methodology
|-
| 13 || <code>total_pop_chg_targets</code> || Targets || Population-change methodology (Kitsap, Pierce, Snohomish)
|-
| 14 || <code>legacy.emp_chg_targets</code> || Targets || Combine employment targets and compute horizon-year values
|-
| 15 || <code>legacy.adjust_snohomish_emp_targets_by_rgid</code> || Targets || Apply hard-coded Snohomish employment by RGID
|-
| 16 || <code>legacy.extrapolate_to_controls_year</code> || Extrapolation || Linear extrapolation from 2044 to 2050
|-
| 17 || <code>legacy.create_controls</code> || Controls || Assemble final table, handle exclusions, export to Excel
|-
| 18 || <code>legacy.run_parcel_capacity_r_script</code> || R Scripts || Compute parcel-level development capacity
|-
| 19 || <code>legacy.run_r_scripts</code> || R Scripts || Interpolate interim years and split HCT/non-HCT
|}

----

== Key Concepts ==

=== Control Areas ===
The fundamental geographic unit for this pipeline. Each control area has a unique <code>control_id</code>. Growth targets are set at the target-area level (groups of control areas), and results are distributed back to individual control areas.

=== Target Methodologies ===
Different counties use different approaches to define growth targets:
* '''Unit Change''' (<code>unit_chg</code>): Targets specify change in housing units. Currently unused (<code>[]</code> in settings).
* '''Total Population Change''' (<code>total_pop_chg</code>): Targets specify total population change. Used by Kitsap (53035), Pierce (53053), and Snohomish (53061).
* '''King County Method''' (<code>king_cnty_method</code>): Uses housing unit change targets but applies county-specific household sizes, vacancy rates, and a regional household population control total.

=== RGID (Regional Growth ID) ===
A classification of target areas into Regional Growth categories. Used for aggregating and distributing employment and population targets, and for applying per-category parameters (e.g., King County household sizes).

=== Resource and Construction Employment ===
King and Snohomish county employment targets '''exclude''' resource and construction sectors. These sectors are added back separately using hard-coded county totals and proportional distribution. Kitsap and Pierce targets '''include''' these sectors.

=== HCT Split ===
The final processing phase divides control totals into High Capacity Transit (HCT) and non-HCT components for each jurisdiction, targeting regional policy goals for transit-oriented growth (65% of households, 75% of employment in HCT areas).

[[Category:Control Totals]]
[[Category:LUVit]]
[[Category:PSRC Planning Tools]]
