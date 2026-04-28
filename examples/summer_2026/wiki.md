= Summer 2026 Control Totals Process =

This page documents the '''summer_2026''' control totals pipeline, which transforms census data, employment estimates, and county growth targets into final control totals for PSRC's Land Use Vision (LUVit) model. Unlike the legacy pipeline, this version replaces all R script processing with native Python steps for interpolation, parcel capacity, and HCT splitting. The pipeline is configured via <code>settings.yaml</code> and executed as a series of ordered steps.

== Overview ==

The pipeline follows six phases:

# '''Data Loading''' — Fetch external data from PSRC's Elmer database, Census API, and local CSV files; store in an HDF5 pipeline cache.
# '''Geoprocessing''' — Build the control-area geography from regional geographies, create HCT transit buffers, and produce spatial crosswalks between census blocks, parcels, and control areas.
# '''Data Preparation''' — Aggregate parcel-level and block-level data to the control-area level.
# '''Target Adjustment''' — Calibrate county growth targets to the base year (2023) using observed OFM and employment estimates.
# '''Extrapolation & Control Totals''' — Project targets to the control totals horizon year (2050), assemble the final control totals table, and export to Excel.
# '''Python Post-Processing''' — Interpolate interim years, compute parcel capacity, and split control totals into HCT/non-HCT components — all in Python.

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
                                               ┌─────────────────────────────┐
                                               │  Python Post-Processing     │
                                               │  • Rebased targets          │
                                               │  • Interpolation            │
                                               │  • Parcel capacity          │
                                               │  • HCT split               │
                                               └─────────────────────────────┘
</pre>

== Configuration ==

Key settings from <code>settings.yaml</code>:

{| class="wikitable"
! Setting !! Value !! Description
|-
| <code>base_year</code> || 2023 || Reference year; estimates are anchored here
|-
| <code>targets_end_year</code> || 2044 || Horizon year for county growth targets
|-
| <code>end_year</code> || 2050 || Final year for control totals output
|-
| <code>ref_base_year</code> || 2018 || REF (Regional Economic Forecast) base year used for 2018 employment and OFM data
|-
| <code>target_types</code> || — || All three non-King counties (Kitsap, Pierce, Snohomish) now use <code>unit_chg</code>; <code>total_pop_chg</code> is empty
|-
| <code>emp_target_types</code> || — || Employment target variants: <code>res_con</code> (Kitsap, Pierce) vs. <code>no_res_con</code> (King, Snohomish)
|-
| <code>king_cnty_method</code> || True || Use King County–specific household size and vacancy rate method
|-
| <code>hct_buffers</code> || — || Buffer distances (in feet) by transit type: BRT 1320, commuter rail 2640, light rail 2640, ferry 2640
|}

=== Key Differences from Legacy Pipeline ===

{| class="wikitable"
! Area !! Legacy !! Summer 2026
|-
| Base year || 2020 || 2023
|-
| Non-King target methodology || Total population change (Kitsap, Pierce, Snohomish) || Housing unit change (all three counties)
|-
| Control area geography || Static shapefile from database || Dynamically built from regional geographies, military bases, tribal areas, and natural resources
|-
| HCT parcel flagging || External / manual || Automated transit buffer creation and parcel flagging step
|-
| Kitsap unincorporated targets || Handled within R || Dedicated Python step to fill missing targets
|-
| R scripts || Interpolation, parcel capacity, and HCT split all run via R || Fully replaced by Python steps
|-
| Parcel capacity || R script (<code>parcels_capacity.R</code>) || Python step (<code>steps.parcels_capacity</code>)
|-
| Rebased targets & interpolation || R script (<code>create_control_totals_luv3_rebased_targets.R</code>) || Python step (<code>steps.create_control_totals_rebased_targets</code>)
|-
| HCT split || R script (<code>split_ct_to_hct.R</code>) || Python step (<code>steps.split_ct_to_hct</code>)
|}

----

== Phase 1: Data Loading ==

=== Step 1: Initialize HDF5 ===

<code>control_totals.steps.data_loading.initialize_hdf5</code>

Deletes any existing <code>pipeline.h5</code> file to ensure a clean slate for the run. All subsequent steps write their outputs into this HDF5 store.

=== Step 2: Get Elmer Data ===

<code>control_totals.steps.data_loading.get_elmer_data</code>

Connects to PSRC's '''Elmer''' and '''ElmerGeo''' SQL Server databases and fetches the tables defined in settings:

* '''ElmerGeo''' (geospatial):
** Regional geographies (<code>REGIONAL_GEOGRAPHIES</code>) — jurisdictions with city name, county, feature type, and RG class
** County boundaries (<code>COUNTY_BACKGROUND</code>)
** Military bases (<code>MILITARY_BASES</code>)
** Tribal land (<code>TRIBAL_LAND</code>)
** Natural resource areas (<code>NAT_RESOURCE</code>), national forests (<code>NATIONAL_FOREST</code>), national parks (<code>NATIONAL_PARK</code>)
** Old control areas (<code>CONTROL18_DASHBOARD</code>) — used as a template for splitting the Renton PAA
** Census block polygons (<code>BLOCK2020</code>)
** Parcel point centroids: OFM vintage (<code>PARCELS_URBANSIM_2018_PTS</code>) and current (<code>PARCELS_URBANSIM_2023_PTS</code>)
** HCT stops (<code>hct_vision_pts</code>) — transit stop points with binary flags for BRT, commuter rail, light rail, ferry, and rural
** Urban centers (<code>urban_centers</code>)
** Urban growth area (<code>urban_growth_area</code>)
** PSRC region boundary (<code>psrc_region</code>)
* '''Elmer''' (tabular): OFM parcelized estimates (<code>ofm.parcelized_saep</code>) for 2018, 2019, 2020, and 2023.

All tables are saved to the HDF5 store with ID columns cast to <code>int64</code>.

=== Step 3: Load Data ===

<code>control_totals.steps.data_loading.load_data</code>

Loads CSV files from the <code>data/</code> directory into the HDF5 store. Two categories of tables are loaded:

* '''General data tables''' — <code>control_target_xwalk</code>, <code>regional_geographies_xwalk</code>, <code>military_bases_xwalk</code>, <code>ref_projection</code>, employment by control area for 2018/2019/2020/2023.
* '''County growth targets''' — King, Kitsap, Pierce, and Snohomish target files. Column names are standardized (e.g., the configured <code>units_chg_col</code> is renamed to <code>units_chg</code>), and a <code>county_id</code> FIPS code is added.

Validation checks confirm that required base-year data exists for all target start years referenced in the targets tables. Missing files are optionally copied from the network backup directory (<code>tables_backup_dir</code>).

----

== Phase 2: Geoprocessing ==

=== Step 4: Create Control Area Geography ===

<code>control_totals.steps.geoprocessing.create_control_area_geography</code>

'''New in summer_2026.''' Dynamically builds the control-area geography by unioning and dissolving multiple spatial layers:

# '''Regional geographies''' — Jurisdictions from ElmerGeo joined with the control-area crosswalk. The Renton PAA is split into three sub-areas using the old control area polygons as a template via spatial join and dissolve.
# '''County rural areas''' — Each PSRC county is mapped to a rural control-area ID (King→64, Kitsap→76, Pierce→124, Snohomish→176).
# '''Military bases''' — Dissolved by installation ID, joined to the military bases crosswalk, and clipped to the PSRC region.
# '''Tribal land''' — The Tulalip Reservation is extracted, clipped to county boundaries, and assigned control ID 210.
# '''Natural resource areas''' — National forests, national parks, and natural resource polygons are buffered, dissolved into a single layer, clipped to the PSRC region, and assigned per-county control IDs (King→301, Kitsap→302, Pierce→303, Snohomish→304).

All layers are combined using iterative <code>union_dissolve</code> operations, with higher-priority layers (military, tribal, natural resource) overriding lower-priority layers. The resulting GeoDataFrame is enriched with control names and target IDs from the crosswalk, then saved as <code>control_areas</code>.

=== Step 5: Flag HCT Parcels ===

<code>control_totals.steps.geoprocessing.flag_hct_parcels</code>

'''New in summer_2026.''' Creates high-capacity transit (HCT) buffers around transit stops and flags parcels that fall within those buffers:

# Loads current-year parcel point geometries (<code>parcel_pts_current</code>).
# Flags each parcel as '''rural''' (within rural areas of the PSRC region).
# Flags each parcel as '''urban center''' (within urban center polygons).
# For each HCT stop type (BRT, commuter rail, light rail, ferry), buffers non-rural stops by the configured distance and flags intersecting parcels.
# Assigns a '''TOD code''' to each parcel based on priority order (highest to lowest):
** 0 — Rural (resets to non-HCT regardless of other buffers)
** 4 — Light rail
** 2 — Commuter rail
** 5 — Ferry
** 1 — BRT
** 6 — Urban center
** 0 — Default (non-HCT, non-urban-center)
# Assigns geographic IDs by spatial-joining parcels to control areas:
** <code>control_id</code> — The base control area
** <code>control_hct_id</code> — <code>control_id + 1000</code> for parcels in HCT zones; equals <code>control_id</code> otherwise
** <code>subreg_id</code> — Set equal to <code>control_hct_id</code>

Output: <code>parcels_hct</code> (flagged parcels) and <code>hct_buffers</code> (buffer polygons).

=== Step 6: Block–Control Area Crosswalk ===

<code>control_totals.steps.geoprocessing.block_control_area_xwalk</code>

Creates a spatial crosswalk between census blocks and control areas. Block polygons are converted to representative points (centroids), then spatially joined to control area polygons using <code>sjoin_nearest</code>. This handles edge cases where block centroids fall over water or outside control area boundaries.

Output: <code>block_control_area_xwalk</code>.

=== Step 7: Parcel–Control Area Crosswalks ===

<code>control_totals.steps.geoprocessing.parcel_control_area_xwalks</code>

Creates spatial crosswalks between parcel point centroids and control areas. Uses a two-pass approach:

# Standard spatial join (<code>sjoin</code>) for parcels within control area polygons.
# <code>sjoin_nearest</code> for any unmatched parcels, assigning them to the nearest control area.

Two crosswalks are produced:
* <code>ofm_parcel_control_area_xwalk</code> — Using the OFM-vintage parcel points (2018).
* <code>current_parcel_control_area_xwalk</code> — Using the current-year parcel points (2023) with HCT <code>subreg_id</code> and <code>control_id</code> from the parcels_hct step.

----

== Phase 3: Data Preparation ==

=== Step 8: Get Census Data ===

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

=== Step 9: Prepare Parcel Data ===

<code>control_totals.steps.data_loading.prepare_parcel_data</code>

Aggregates parcel-level OFM estimates to the control-area level using the OFM parcel–control area crosswalk. For each OFM vintage year (2018, 2019, 2020, 2023):

* Merges parcel data with the crosswalk (validates that every parcel has a <code>control_id</code>).
* Renames OFM columns to standardized prefixes: <code>total_pop</code>→<code>ofm_total_pop</code>, <code>household_pop</code>→<code>ofm_hhpop</code>, <code>housing_units</code>→<code>ofm_units</code>, <code>occupied_housing_units</code>→<code>ofm_hh</code>, <code>group_quarters</code>→<code>ofm_gq</code>.
* Sums by <code>control_id</code>.

Outputs: <code>ofm_parcelized_[year]_by_control_area</code> tables.

=== Step 10: Prepare Block Data ===

<code>control_totals.steps.data_loading.prepare_block_data</code>

Aggregates decennial census block data to the control-area level using the block–control area crosswalk:

* Sums census population, housing units, households, and group quarters by <code>control_id</code>.
* Derives <code>dec_hhpop = dec_total_pop − dec_gq</code>.

Output: <code>decennial_by_control_area</code>.

----

== Phase 4: Target Calculations ==

=== Step 11: Split Unincorporated Kitsap Housing Targets ===

<code>control_totals.steps.split_unincorporated_kitsap_housing_targets</code>

'''New in summer_2026.''' Fills in missing housing-unit growth targets for unincorporated Kitsap target areas. The Kitsap Growth Targets file provides total population targets for unincorporated areas but the housing-unit allocations may be missing at the sub-area level. This step:

# Loads Kitsap targets and merges with base-year OFM estimates aggregated to target areas.
# Splits targets into incorporated and unincorporated subsets.
# For unincorporated areas, derives household-population targets from total-population targets using group-quarters shares.
# Estimates household sizes using ratios of target-year to start-year regional household sizes.
# Computes preliminary household counts and normalises to match the overall unincorporated target total.
# Allocates housing units using vacancy rates, rounds using <code>saferound</code>, and computes unit change from start year.
# Recombines incorporated and unincorporated subsets.

Output: Updated <code>kitsap_targets</code> table.

=== Step 12: Adjust Targets to Base Year ===

<code>control_totals.steps.adjust_targets_to_base_year</code>

Adjusts raw county growth-change targets so they are relative to the 2023 base year instead of each county's original target start year. For each target type (units, total population, employment):

# '''Combines''' targets from all county files for a given type.
# '''Sums''' base-year estimates (OFM for population/units, employment tables for jobs) to the target-area level using the control-target crosswalk.
# '''Calculates''' the estimated change between the target start year and the base year.
# '''Subtracts''' that change from the raw target: <code>adjusted_change = raw_target − estimated_base_year_change</code>.
# Clipped to zero (no negative growth) for OFM-based estimates.

Outputs: <code>adjusted_units_change_targets</code>, <code>adjusted_total_pop_change_targets</code>, <code>adjusted_emp_change_targets</code>.

=== Step 13: King County Targets ===

<code>control_totals.steps.king_cnty_targets</code>

Implements the King County–specific methodology, which uses '''housing unit change''' targets combined with hard-coded household sizes, vacancy rates, and a regional household population control total:

# Loads input tables (decennial estimates merged with adjusted unit-change targets), filtered to King County (FIPS 53033).
# Aggregates unit-change and population-change targets by RGID (Regional Growth ID).
# Applies per-RGID vacancy rates (<code>king_vac</code>) to derive households from housing units: <code>hh = units × (1 − vacancy_rate)</code>.
# Applies per-RGID household sizes (<code>king_hhsz</code>) to derive initial household population.
# '''Regional factoring:''' Scales all RGIDs so that total King County household population matches the configured <code>king_hhpop_2044</code> target (2,828,620).
# Distributes RGID-level totals back to individual target areas using decennial household-size ratios.
# Uses adjusted metro household size (<code>king_metro_adj_hhsz</code> = 2.00) for target-area-level calculations in metro RGID areas.
# Household sizes are capped at 5.0 (falls back to RGID-level value if exceeded).
# Calculates group quarters using regional REF projection GQ shares, then derives total population.

Output: <code>adjusted_king_targets</code>.

=== Step 14: Housing Unit Change Targets ===

<code>control_totals.steps.units_chg_targets</code>

'''New in summer_2026.''' Calculates targets for counties using the housing-unit change methodology (Kitsap, Pierce, Snohomish — all non-King counties in this pipeline):

# Merges adjusted unit-change and population-change targets with base-year OFM estimates aggregated to target areas.
# Calculates vacancy rates by RGID from OFM data.
# Computes group quarters using REF projection GQ shares (using OFM as base data source).
# For the targets horizon year (2044):
#* <code>total_pop = ofm_total_pop + total_pop_chg_adj</code>
#* <code>hhpop = total_pop − gq</code>
#* <code>units = ofm_units + units_chg_adj</code>
#* <code>hh = units × (1 − vacancy_rate_by_rgid)</code>
#* <code>hhsz = hhpop / hh</code>
# Filters to only counties configured under <code>target_types.unit_chg</code>.

Output: <code>adjusted_units_change_targets</code>.

=== Step 15: Employment Targets (with Resource/Construction) ===

<code>control_totals.steps.emp_chg_targets_res_con</code>

Calculates employment targets for counties that '''include''' resource and construction employment (Kitsap 53035, Pierce 53053):

# Loads adjusted employment change targets and base-year employment totals (excluding military).
# Adds the adjusted employment change directly to the base-year employment total: <code>emp_2044 = Emp_TotNoMil_2023 + emp_chg_adj</code>.

Output: <code>adjusted_emp_change_targets_res_con</code>.

=== Step 16: Employment Targets (without Resource/Construction) ===

<code>control_totals.steps.emp_chg_targets_no_res_con</code>

Calculates employment targets for counties that '''exclude''' resource and construction employment (King 53033, Snohomish 53061):

# Loads adjusted employment change targets and base-year employment totals.
# Calculates the resource-and-construction share of employment for each target area.
# Computes a county-level resource/construction growth allocation using the configured growth percentage (<code>res_con_emp_growth_pct</code> = 2.8%) and distributes it back to target areas proportionally.
# Adds the resource/construction growth allocation to the adjusted change: <code>emp_chg_adj_res_con = emp_chg_adj + res_con_emp_chg_target</code>.
# Computes horizon-year employment: <code>emp_2044 = Emp_TotNoMil_2023 + emp_chg_adj_res_con</code>.

Output: <code>adjusted_emp_change_targets_no_res_con</code>.

----

== Phase 5: Extrapolation & Final Controls ==

=== Step 17: Extrapolate to Controls Year ===

<code>control_totals.steps.extrapolate_to_controls_year</code>

Extends targets from the targets horizon year (2044) to the final control totals year (2050) using linear extrapolation:

# '''Loads''' all adjusted target tables — population/housing (from unit-change and King County steps) and employment (from both res_con and no_res_con steps) — and merges them into a single DataFrame.
# For each indicator (hh, total_pop, emp):
#* Computes annual change: <code>(target_year_value − base_year_value) / years_elapsed</code>
#* Annual change is clipped to zero (no negative growth).
#* Extends to 2050: <code>base_year_value + annual_change × years_to_2050</code>
# Calculates group quarters for the control year using REF projection GQ shares with OFM as the base data source.
# Derives household population: <code>hhpop = gq + total_pop</code>.
# Calculates implied household size: <code>hhsz = hhpop / hh</code>.

Output: <code>extrapolated_targets</code>.

=== Step 18: Create Controls ===

<code>control_totals.steps.create_controls</code>

Assembles the final control totals table and exports it to Excel for the downstream Python processing steps:

# '''Merges''' extrapolated targets with the control-target crosswalk, base-year (2023) OFM estimates, and base-year employment data by <code>control_id</code>.
# '''Handles excluded areas''' (e.g., military bases flagged with <code>exclude_from_target == 1</code>):
#* Resets horizon-year values to base-year values for excluded areas.
#* Subtracts excluded area values from sibling control areas within the same target group.
# '''Applies employment overrides''' from settings (<code>emp_target_overrides</code>), extrapolating overridden values to the control year.
# '''Merges''' 2018 REF base-year OFM and employment data.
# '''Renames columns''' to legacy-compatible names (e.g., <code>ofm_total_pop</code>→<code>TotPop23</code>, <code>hh_2050</code>→<code>HH50</code>).
# '''Derives additional fields:'''
#* <code>TotEmpTrg_wCRnoMil = Emp44 − Emp23</code>
#* <code>TotPopTrg = TotPop44 − TotPop23</code>
#* <code>GQpct50 = GQ50 / TotPop50</code>
#* <code>PPH50 = HHpop50 / HH50</code>
# '''Exports''' to <code>control_id_working.xlsx</code> in the data directory.

Output: <code>control_totals</code> table (HDF5) and <code>control_id_working.xlsx</code> (Excel).

----

== Phase 6: Python Post-Processing ==

=== Step 19: Parcel Capacity (Optional) ===

<code>control_totals.steps.parcels_capacity</code>

'''New in summer_2026.''' Python replacement for the legacy R script <code>parcels_capacity.R</code>. Currently commented out in the pipeline steps (capacity CSV is expected to already exist or be generated externally).

Computes development capacity at the parcel level from UrbanSim proposal and base-year building data:

* '''Inputs:''' Base-year buildings and parcels from <code>lookup_path</code>, UrbanSim development project proposals and components from <code>prop_path</code>, building-sqft-per-job lookup.
* '''Logic:'''
** Imputes missing sqft_per_unit values for residential buildings (type 19 → 1000; others → 500).
** Aggregates existing building stock (units, non-res sqft, building sqft, job capacity) to the parcel level.
** Filters proposals: excludes MPD proposals (status_id=3), removes proposals smaller than existing stock.
** Splits proposals into residential-only, non-residential-only, and mixed-use categories.
** For mixed-use parcels, either samples one proposal per parcel (when <code>mu_sampling=true</code>) or applies the <code>res_ratio</code> (default 50%) to scale both residential and non-residential components.
** Selects the maximum proposal per parcel for each use type.
** Final capacity equals the proposed value for parcels with proposals; base-year stock for all others.
** Updates <code>control_id</code> and <code>subreg_id</code> from the HCT parcel flags.

{| class="wikitable"
! Setting !! Value !! Description
|-
| <code>prop_path</code> || (network path) || Directory containing UrbanSim proposal CSVs from an unlimited run
|-
| <code>lookup_path</code> || (network path) || Directory containing base-year building and parcel CSVs
|-
| <code>res_ratio</code> || 50 || Residential share percentage for mixed-use parcels (0–100)
|-
| <code>mu_sampling</code> || false || Sample parcels (true) or apply ratio to units (false)
|-
| <code>rng_seed</code> || 1 || Random seed for reproducibility
|-
| <code>file_prefix</code> || CapacityPclNoSampling_res50 || Prefix of the output CSV file name
|}

Output: <code>CapacityPclNoSampling_res50.csv</code> with columns for base-year and capacity values for dwelling units, non-residential sqft, job spaces, and building sqft, along with geographic identifiers (<code>control_id</code>, <code>subreg_id</code>, <code>tod_id</code>).

=== Step 20: Create Rebased Targets & Interpolated Control Totals ===

<code>control_totals.steps.create_control_totals_rebased_targets</code>

'''New in summer_2026.''' Python replacement for the legacy R script <code>create_control_totals_luv3_rebased_targets.R</code>.

* '''Input:''' <code>control_id_working.xlsx</code> (from Step 18).
* '''Logic:'''
** Reads city-level base (2018, 2023) and target (2050) values for households, population, and employment.
** Infers the REF base year, base year, and target year from column naming patterns.
** Computes growth deltas and summarises by RGID.
** Derives household population and households from PPH (persons per household) and GQ (group quarters) shares.
** '''Linearly interpolates''' between anchor years (2018 → 2023 → 2050) to fill in all stepped years (2018, 2023, 2025, 2030, 2035, 2040, 2044, 2050).
** Optionally scales interpolated values to match a regional REF projection (<code>scale_to_ref: false</code> in this configuration).
** Produces unrolled long-format output and annual regional summaries.
* '''Outputs:'''
** <code>TargetsRebasedOutput.xlsx</code> — Rebased targets with sheets for RGs, CityPop, CityHH, CityEmp.
** <code>Control-Totals-LUVit.xlsx</code> — Interpolated control totals for all stepped years.
** Pipeline HDF5 tables for all indicator sheets and unrolled data.

=== Step 21: Load Split HCT Base Data ===

<code>control_totals.steps.load_split_hct_base_data</code>

Loads household, person, and job base data from a MySQL parcel base-year database for use by the HCT split step:

# Connects to a MySQL database (e.g. <code>2018_parcel_baseyear</code>) using credentials from <code>creds.txt</code>.
# Queries household/person counts and job counts grouped by parcel from the <code>households</code>, <code>buildings</code>, and <code>jobs</code> tables.
# Aggregates base data to the <code>subreg_id</code> / <code>control_id</code> geography using the <code>current_parcel_control_area_xwalk</code> from the HCT parcel flagging step.
# Joins control-area names and RGID from the <code>control_target_xwalk</code>.
# Saves to the pipeline HDF5 store as <code>split_hct_base_data_2023</code>.

Output: <code>split_hct_base_data_2023</code>.

=== Step 22: Split Control Totals to HCT ===

<code>control_totals.steps.split_ct_to_hct</code>

'''New in summer_2026.''' Python replacement for the legacy R script <code>split_ct_to_hct.R</code>. Splits control totals into HCT (High Capacity Transit) and non-HCT components for each jurisdiction.

* '''Inputs:''' <code>Control-Totals-LUVit.xlsx</code> (from Step 20), <code>CapacityPclNoSampling_res50.csv</code> (parcel capacity), base-year split data (from Step 21).
* '''Logic:'''
** '''Loads targets''' — Reads HH, Emp, and HHPop sheets from the control-totals workbook and computes derived columns (persons-per-household ratios, population growth).
** '''Loads capacity''' — Reads parcel-level capacity CSV, computes total capacity (max of base and proposed), and aggregates to the split/no-split geography level.
** '''Prepares base data''' — Enriches base data with within-geography group totals, merges base-year values from the control-totals sheets, and flags TOD areas (<code>is_tod = split_geo_id ≠ nosplit_geo_id</code>).
** '''Creates generators''' — Builds per-indicator generator DataFrames with PPH ratios for HH, and merges geography-level capacity data for HH and Emp.
** '''Iterative split algorithm''' — For each indicator (HH, Emp, HHPop):
**# Computes initial TOD growth based on capacity shares.
**# Sets non-TOD growth as the residual.
**# Redirects overflow when non-TOD growth exceeds capacity.
**# '''Iteratively scales''' TOD growth by adjusting a per-RGID scale factor, weighted by remaining capacity.
**# Repeats until the regional TOD share target is met or max iterations (2000) reached.
**# For HH, additionally adjusts persons-per-household to maintain appropriate density relationships.
**# Growth values are clipped so that no sub-area's result goes below zero.
** '''Regional share targets:'''
*** Households: 65% in HCT areas
*** Employment: 75% in HCT areas
** '''Per-RG step values:''' Metro = 1.0, Core Cities = 0.5, HCT Communities = 0.25.
** '''Per-RG minimum non-HCT shares:''' Configurable via <code>scenarios</code> (default [10, 10, 10] for both HH and Emp).
** '''Interpolates''' split results into stepped years and produces annual regional summaries.
* '''Outputs:''' Excel workbook with HH/HHPop/Emp worksheets, unrolled stepped years (2023, 2025, 2030, 2035, 2040, 2044, 2050), regional aggregations, and QA check sheets.

{| class="wikitable"
! Setting !! Value !! Description
|-
| <code>trgshare.HH</code> || 65 || Regional target for percentage of household growth in HCT areas
|-
| <code>trgshare.Emp</code> || 75 || Regional target for percentage of employment growth in HCT areas
|-
| <code>scenarios</code> || [HH: [10,10,10], Emp: [10,10,10]] || Minimum non-HCT share by RG [metro, core, HCT communities]
|-
| <code>step_values</code> || [1, 0.5, 0.25] || Per-RG iteration step sizes for scaling TOD growth
|-
| <code>max_iterations</code> || 2000 || Maximum scaling iterations per indicator
|-
| <code>aggregate_no_growth_areas</code> || false || Whether to collapse no-growth geographies before splitting
|-
| <code>round_interpolated</code> || false || Whether to round interpolated values to integers
|}

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
| 4 || <code>geoprocessing.create_control_area_geography</code> || Geoprocessing || Build control-area geography from regional layers
|-
| 5 || <code>geoprocessing.flag_hct_parcels</code> || Geoprocessing || Create HCT buffers and flag parcels by transit type
|-
| 6 || <code>geoprocessing.block_control_area_xwalk</code> || Geoprocessing || Spatially join blocks to control areas
|-
| 7 || <code>geoprocessing.parcel_control_area_xwalks</code> || Geoprocessing || Spatially join parcels to control areas
|-
| 8 || <code>data_loading.get_census_data</code> || Preparation || Fetch decennial census block data from API
|-
| 9 || <code>data_loading.prepare_parcel_data</code> || Preparation || Aggregate OFM parcel data to control areas
|-
| 10 || <code>data_loading.prepare_block_data</code> || Preparation || Aggregate census block data to control areas
|-
| 11 || <code>split_unincorporated_kitsap_housing_targets</code> || Targets || Fill missing Kitsap unincorporated housing targets
|-
| 12 || <code>adjust_targets_to_base_year</code> || Targets || Adjust growth targets to 2023 base year
|-
| 13 || <code>king_cnty_targets</code> || Targets || King County unit-change methodology
|-
| 14 || <code>units_chg_targets</code> || Targets || Housing unit-change targets (Kitsap, Pierce, Snohomish)
|-
| 15 || <code>emp_chg_targets_res_con</code> || Targets || Employment targets including resource/construction (Kitsap, Pierce)
|-
| 16 || <code>emp_chg_targets_no_res_con</code> || Targets || Employment targets excluding resource/construction (King, Snohomish)
|-
| 17 || <code>extrapolate_to_controls_year</code> || Extrapolation || Linear extrapolation from 2044 to 2050
|-
| 18 || <code>create_controls</code> || Controls || Assemble final table, handle exclusions, export to Excel
|-
| — || <code>parcels_capacity</code> || Python Post-Processing || Compute parcel-level development capacity (currently disabled)
|-
| 19 || <code>create_control_totals_rebased_targets</code> || Python Post-Processing || Interpolate rebased targets and produce control totals workbook
|-
| 20 || <code>load_split_hct_base_data</code> || Python Post-Processing || Load split HCT base data from MySQL
|-
| 21 || <code>split_ct_to_hct</code> || Python Post-Processing || Split control totals into HCT and non-HCT components
|}

----

== Key Concepts ==

=== Control Areas ===
The fundamental geographic unit for this pipeline. Each control area has a unique <code>control_id</code>. In this pipeline, control areas are dynamically built from regional geographies, military bases, tribal areas, and natural resource areas rather than loaded from a static shapefile. Growth targets are set at the target-area level (groups of control areas), and results are distributed back to individual control areas.

=== Target Methodologies ===
Different counties use different approaches to define growth targets:
* '''Housing Unit Change''' (<code>unit_chg</code>): Targets specify change in housing units. Used by Kitsap (53035), Pierce (53053), and Snohomish (53061). Population, households, and household size are derived from housing unit changes combined with OFM vacancy rates and decennial household-size data.
* '''King County Method''' (<code>king_cnty_method</code>): Uses housing unit change targets but applies county-specific household sizes, vacancy rates, and a forced regional household population control total (2,828,620 for 2044).

=== RGID (Regional Growth ID) ===
A classification of target areas into Regional Growth categories. Used for aggregating and distributing employment and population targets, and for applying per-category parameters (e.g., King County household sizes, HCT split step values).

=== Resource and Construction Employment ===
King and Snohomish county employment targets '''exclude''' resource and construction sectors. These sectors are added back using a configured growth percentage (<code>res_con_emp_growth_pct</code> = 2.8%) distributed proportionally based on each target area's existing resource/construction share. Kitsap and Pierce targets '''include''' these sectors.

=== HCT Split ===
The final processing phase divides control totals into High Capacity Transit (HCT) and non-HCT components for each jurisdiction, targeting regional policy goals for transit-oriented growth (65% of households, 75% of employment in HCT areas). In this pipeline, the HCT geography is determined dynamically by buffering transit stops and flagging parcels, rather than using a static HCT layer.

=== TOD Codes ===
Each parcel is assigned a transit-oriented development code based on which HCT buffer zone it falls within. The <code>subreg_id</code> for HCT parcels equals <code>control_id + 1000</code>, creating a split geography where growth can be separately allocated to HCT and non-HCT sub-areas within each control area.

[[Category:Control Totals]]
[[Category:LUVit]]
[[Category:PSRC Planning Tools]]
