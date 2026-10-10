# Seismic Pulse: Earthquake Monitoring & Risk Analytics Pipeline

DS-3001 Data Analysis & Visualisation, FAST NUCES Lahore
Authors: Abdullah Farhan (23L-2643), Hassan Tuyyab (23L-2548)

An end-to-end Spark pipeline on **Databricks Free Edition (serverless)** that ingests global earthquake data from the USGS Earthquake Hazards Program and moves it through the Medallion Architecture (Bronze, Silver, Gold) into a Power BI dashboard.

**Status:** Phase 2 complete (Bronze and Silver). Gold layer and dashboard are Phase 3.

> **Platform note:** the Phase 2 brief lists Community Edition or Azure. Our Phase 1 feedback directed us to Databricks Free Edition (serverless-only), which is what this project uses.
>
> **Data loading note:** the USGS API was reachable from Databricks Free Edition (HTTP 200), so data is fetched directly with `download_usgs.py`. No manual download or upload was needed.

---

## 1. Repository structure

```
dav-project/
├── README.md
├── full_load_sample.geojson        # Phase 1 sample (Jan-Jun 2024 slice of the full load)
├── incremental_load_sample.geojson # Phase 1 sample (M2.5+ daily feed pull)
├── download_usgs.py                # fetches full load (monthly chunks) and incremental sample
├── 01_raw_to_bronze.py             # raw GeoJSON -> bronze_earthquakes
├── 02_bronze_to_silver.py          # bronze -> silver_earthquakes (+ quarantine)
├── 03_drift_test.py                # builds a deliberately broken file to prove drift handling
├── drift_test.png                  # evidence screenshot
└── setup/
    └── 00_setup.py                 # creates schema and all tables (run once)
```

The Gold layer notebooks will be added in Phase 3.

The `.py` files use the Databricks notebook source format. Import them into a Databricks workspace as notebooks (Workspace > Import).

## 2. Data currently loaded

| Table | Rows |
|---|---|
| `bronze_earthquakes` | 79,631 |
| `silver_earthquakes` | 79,631 |
| `silver_quarantine` | 0 (test rows removed after the drift test) |
| `pipeline_execution_logs` | 157 |
| `schema_drift_log` | 6 |

Loaded so far: 2015 to 2017 (monthly files), January and February 2024, plus the incremental sample. The remaining historical months load with the same parameterized call (see section 6). The full 335,689-event, 240.7 MB load is deferred to Phase 3 to conserve Free Edition compute. Bronze and Silver row counts match because no event has yet been revised.

## 3. Pipeline overview

```
USGS GeoJSON files (Volume)
        |  01_raw_to_bronze   explicit schema, flatten, drift checks, MERGE
        v
bronze_earthquakes   (all values as text, one row per event revision)
        |  02_bronze_to_silver   try_cast, validate, clean, MERGE
        v
silver_earthquakes   (typed, one row per event = latest revision)
        +--> silver_quarantine   (rows that fail validation)

Every file processed writes a row to pipeline_execution_logs.
Detected schema changes write rows to schema_drift_log.
```

Raw files live in a Databricks Volume: `/Volumes/workspace/default/seismic/` (monthly files in `full_load_monthly/`). All tables live in `workspace.seismic_pulse`.

## 4. Data models

### 4.1 Bronze: `bronze_earthquakes`

Raw copy of every event revision. **Primary key: (`event_id`, `updated_ms`).** All USGS values are stored as `STRING`, so a type change in the source can never crash ingestion. Casting happens in Silver.

| Column | Type | Source (GeoJSON) | Description |
|---|---|---|---|
| event_id | STRING | `id` | USGS event identifier (key part 1) |
| mag | STRING | `properties.mag` | Magnitude |
| place | STRING | `properties.place` | Free-text location |
| time_ms | STRING | `properties.time` | Event time, Unix epoch milliseconds |
| updated_ms | STRING | `properties.updated` | Last revision time, epoch ms (key part 2) |
| tz | STRING | `properties.tz` | Timezone offset (usually null) |
| url | STRING | `properties.url` | Event page |
| detail | STRING | `properties.detail` | Detail feed URL |
| felt | STRING | `properties.felt` | Number of "felt" reports |
| cdi | STRING | `properties.cdi` | Community intensity |
| mmi | STRING | `properties.mmi` | Instrumental intensity |
| alert | STRING | `properties.alert` | PAGER alert level |
| status | STRING | `properties.status` | `automatic` or `reviewed` |
| tsunami | STRING | `properties.tsunami` | Tsunami flag (0/1) |
| sig | STRING | `properties.sig` | Significance score |
| net | STRING | `properties.net` | Contributing network |
| code | STRING | `properties.code` | Network event code |
| ids | STRING | `properties.ids` | Associated event ids |
| sources | STRING | `properties.sources` | Contributing sources |
| types | STRING | `properties.types` | Available product types |
| nst | STRING | `properties.nst` | Number of stations used |
| dmin | STRING | `properties.dmin` | Distance to nearest station |
| rms | STRING | `properties.rms` | RMS travel-time residual |
| gap | STRING | `properties.gap` | Azimuthal gap |
| mag_type | STRING | `properties.magType` | Magnitude method (ml, mb, mww...) |
| event_type | STRING | `properties.type` | `earthquake`, `explosion`, ... |
| title | STRING | `properties.title` | Display title |
| longitude | STRING | `geometry.coordinates[0]` | Longitude |
| latitude | STRING | `geometry.coordinates[1]` | Latitude |
| depth | STRING | `geometry.coordinates[2]` | Depth in km |
| extra_properties | MAP<STRING,STRING> | any other `properties` key | Captures new source columns (schema drift) |
| source_file | STRING | metadata | Full path of the raw file |
| batch_id | STRING | metadata | Run label (parameter) |
| load_type | STRING | metadata | `full` or `incremental` |
| load_timestamp | TIMESTAMP | metadata | When this record was ingested |

### 4.2 Silver: `silver_earthquakes`

Cleaned, typed, **one row per event (latest revision)**. **Primary key: `event_id`.**

| Column | Type | Derived from | Rule |
|---|---|---|---|
| event_id | STRING | event_id | Primary key |
| event_time | TIMESTAMP | time_ms | Epoch ms to UTC timestamp |
| event_date | DATE | time_ms | Date part of event_time |
| event_hour | INT | time_ms | Hour of day (0 to 23) |
| updated_time | TIMESTAMP | updated_ms | Revision time, used to pick the latest version |
| magnitude | DOUBLE | mag | `try_cast`; null if USGS gave none |
| mag_type | STRING | mag_type | Label standardized (e.g. mwr/mww/mwc to `Mw`, ml to `ML`); kept as its own column. Types are not converted to one scale |
| latitude | DOUBLE | latitude | Must be between -90 and 90 |
| longitude | DOUBLE | longitude | Must be between -180 and 180 |
| depth_km | DOUBLE | depth | `try_cast` |
| depth_category | STRING | depth | `shallow` (<70 km), `intermediate` (70 to <300), `deep` (300+), `unknown` |
| place | STRING | place | As published |
| region | STRING | place | Text after the last comma; US state codes shown as `USA (CA)`; no comma means the whole text |
| review_status | STRING | status | Lower-cased `automatic` / `reviewed` flag. Events are kept, not dropped |
| significance | INT | sig | `try_cast` |
| felt | INT | felt | `try_cast` |
| cdi | DOUBLE | cdi | `try_cast` |
| mmi | DOUBLE | mmi | `try_cast` |
| alert | STRING | alert | As published |
| tsunami | INT | tsunami | `try_cast` |
| net | STRING | net | As published |
| event_type | STRING | event_type | As published |
| nst | INT | nst | `try_cast` |
| gap | DOUBLE | gap | `try_cast` |
| rms | DOUBLE | rms | `try_cast` |
| dmin | DOUBLE | dmin | `try_cast` |
| title | STRING | title | As published |
| batch_id | STRING | batch_id | Batch that brought this revision |
| load_timestamp | TIMESTAMP | generated | When this row was written or last updated in Silver |

### 4.3 Quarantine and operational tables

**`silver_quarantine`**: key (`event_id`, `updated_ms`)

| Column | Type | Description |
|---|---|---|
| event_id | STRING | Event id |
| updated_ms | STRING | Revision time (text) |
| reject_reason | STRING | One or more of `MISSING_OR_BAD:<col>`, `BAD_TYPE:<col>`, `LATITUDE_OUT_OF_RANGE`, `LONGITUDE_OUT_OF_RANGE` |
| raw_record | STRING | The Bronze row as JSON, for inspection |
| batch_id | STRING | Batch label |
| load_timestamp | TIMESTAMP | When the row was quarantined |

**`pipeline_execution_logs`**: one row per file processed, per layer

| Column | Type | Description |
|---|---|---|
| run_id | STRING | One id per notebook run |
| layer | STRING | `Raw-to-Bronze` or `Bronze-to-Silver` |
| load_type | STRING | `full` or `incremental` |
| parameter | STRING | File processed (plus date range for Silver) |
| start_time / end_time | TIMESTAMP | Execution window |
| status | STRING | `Success` or `Failure` |
| rows_inserted | BIGINT | New rows written |
| rows_updated | BIGINT | Existing rows updated |
| rows_quarantined | BIGINT | Rows sent to quarantine |
| error_message | STRING | Error text on failure |

**`schema_drift_log`**

| Column | Type | Description |
|---|---|---|
| detected_at | TIMESTAMP | When the change was detected |
| batch_id | STRING | Batch label |
| source_file | STRING | File where it was seen |
| drift_type | STRING | `NEW_COLUMN`, `TYPE_DRIFT`, or `NULL_KEY` |
| column_name | STRING | Column affected |
| detail | STRING | What happened |

## 5. How the requirements are met

| Requirement | Implementation |
|---|---|
| No schema inference | `FILE_SCHEMA` / `FEATURE_SCHEMA` are explicit `StructType` objects; the reader is given `.schema(...)` |
| Strict types and casting | Bronze is text. Silver converts with `try_cast` (serverless runs in ANSI mode, where a plain cast could crash) |
| `load_timestamp` everywhere | Present in Bronze, Silver and quarantine. Log tables carry start/end times, the drift log carries `detected_at` |
| Idempotency with `MERGE INTO` | Bronze: insert only if (`event_id`, `updated_ms`) is new. Silver: update only if the incoming `updated_time` is newer. Re-running the same data adds and changes nothing, and processing order does not matter |
| Parameterized backfills | `run_raw_to_bronze(source_path, file_pattern, load_type, batch_id)` and `run_bronze_to_silver(batch_id, start_date, end_date)` (section 6) |
| Schema drift | New columns are kept in `extra_properties` and logged. Non-numeric values in numeric fields are logged in Bronze and quarantined in Silver. Rows with no id are dropped and logged. The batch never crashes |
| Fault tolerance | Each file runs in its own `try/except`. A failure is logged and the remaining files continue |
| Audit logging | Every file at every layer writes to `pipeline_execution_logs` with layer, parameter, start/end, status and row counts |

## 6. Execution guide

### One-time setup

1. Import the notebooks into Databricks.
2. Run `setup/00_setup.py` once. It creates schema `workspace.seismic_pulse` and all tables. It is safe to re-run.
3. Make sure the raw files are in the Volume (`download_usgs.py` writes them; on Databricks point its output folders at `/Volumes/workspace/default/seismic/...`).

### Standard incremental load (daily)

Fetch the M2.5+ daily feed with `python download_usgs.py incremental`, then call the two functions. Add each call in a new cell at the bottom of the matching notebook (run the notebook once first so the functions are defined), and run only that cell:

```python
# Notebook 01 (Raw to Bronze)
run_raw_to_bronze("/Volumes/workspace/default/seismic",
                  "incremental_load_sample.geojson", "incremental", "incr_2026-10-10")

# Notebook 02 (Bronze to Silver): only this batch
run_bronze_to_silver("incr_2026-10-10", "", "")
```

### Backfill (any historical timeframe)

Same functions, different parameters. Nothing is hard-coded to "today".

```python
BASE = "/Volumes/workspace/default/seismic/full_load_monthly"

# Raw -> Bronze: one month
run_raw_to_bronze(BASE, "usgs_2021-03-01.geojson", "full", "backfill_2021_03")

# Raw -> Bronze: a whole year
run_raw_to_bronze(BASE, "usgs_2019-*.geojson", "full", "backfill_2019")

# Raw -> Bronze: the entire history
run_raw_to_bronze(BASE, "usgs_*.geojson", "full", "full_load")

# Bronze -> Silver: everything in Bronze
run_bronze_to_silver("", "", "")

# Bronze -> Silver: one batch
run_bronze_to_silver("backfill_2019", "", "")

# Bronze -> Silver: only events from a date range
run_bronze_to_silver("", "2019-01-01", "2019-06-30")
```

Parameters:

| Function | Parameter | Meaning |
|---|---|---|
| `run_raw_to_bronze` | `source_path` | Folder containing raw files |
| | `file_pattern` | Which files (glob, e.g. `usgs_2020-0[1-6]-01.geojson`) |
| | `load_type` | `full` or `incremental`, stored on every row and in the log |
| | `batch_id` | Label for the run; empty generates one |
| `run_bronze_to_silver` | `batch_id` | Only Bronze rows from this batch (empty = all) |
| | `start_date`, `end_date` | `YYYY-MM-DD` limits on event date (empty = no limit) |

The notebooks also expose these as Databricks widgets. Widget defaults only apply the first time a widget is created, so calling the functions directly is the most reliable way to pass parameters.

### Safe re-runs

Running any call twice is safe: the second run reports `0 inserted` and Silver row counts do not change. Check results with:

```sql
SELECT * FROM workspace.seismic_pulse.pipeline_execution_logs ORDER BY start_time DESC;
SELECT * FROM workspace.seismic_pulse.schema_drift_log ORDER BY detected_at DESC;
SELECT * FROM workspace.seismic_pulse.silver_quarantine;
```

## 7. Evidence: schema drift test

`03_drift_test.py` writes a file with a new column, a text value in `mag`, an out-of-range latitude, and a record with no id. After running Raw-to-Bronze and Bronze-to-Silver on it:

* Bronze inserted 5 rows (the record with no id was dropped and logged as `NULL_KEY`)
* `schema_drift_log` recorded `NEW_COLUMN`, `TYPE_DRIFT` and `NULL_KEY`
* Silver inserted 3 rows and quarantined 2 (`BAD_TYPE:mag` and `LATITUDE_OUT_OF_RANGE`)
* Later re-runs inserted 0 rows, showing idempotency

![Drift test results](drift_test.png)

## 8. FinOps

* Runs on Databricks Free Edition serverless compute, so there are no clusters to manage.
* Development and tests used small slices (two months) before larger loads.
* The pipeline is idempotent and resumable: a long load can be split over several sessions without duplicating data.
* The full historical load is run in chunks of one to a few years to stay inside daily quotas.

## 9. Known limitations

* `region` comes from simple string parsing of `place` (no coordinate-based lookup).
* The drift log records a detection every time a file is processed, so re-processing the same file adds repeat entries.
* `silver_earthquakes` keeps non-earthquake event types (e.g. explosions) in the `event_type` column. The Gold layer will filter on it.
* Only part of the historical range is loaded (section 2). The rest uses the same calls.

## 10. Source data

| Load type | Endpoint | Scope |
|---|---|---|
| Full load | `https://earthquake.usgs.gov/fdsnws/event/1/query` | M2.5+, 2015 onward, one request per month (FDSN caps a query at 20,000 events) |
| Incremental load | `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson` | M2.5+ events from the past day |

Measured volumes: full load 335,689 events, 240.7 MB; Phase 1 samples 12,756 events (9.9 MB) and 36 events (25.2 KB). The dataset contains no personally identifiable information.
