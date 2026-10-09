# Seismic Pulse: Earthquake Monitoring & Risk Analytics Pipeline

DS-3001 Data Analysis & Visualisation, FAST NUCES Lahore
Authors: Abdullah Farhan (23L-2643), Hassan Tuyyab (23L-2548)

An end-to-end data pipeline on Apache Spark (Databricks Free Edition, serverless) that ingests global earthquake data from the USGS Earthquake Hazards Program, processes it through Bronze, Silver and Gold layers (Medallion Architecture), and serves it in a Power BI dashboard.

## Data source

| Load type | Endpoint | Scope |
|---|---|---|
| Full load | `https://earthquake.usgs.gov/fdsnws/event/1/query` (FDSN Event API) | M2.5+, 2015-01-01 onward, pulled in monthly chunks (FDSN caps a query at 20,000 events) |
| Incremental load | `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson` | M2.5+ events from the past day, matching the full-load threshold |

No API key is needed, and the data is public domain. Reachability from Databricks Free Edition was tested (HTTP 200), so ingestion runs directly inside Databricks.

## Measured data volumes

| Item | Events | Size |
|---|---|---|
| Full load (2015 to date of run, M2.5+) | 335,689 | 240.7 MB (about 140 monthly files) |
| Full-load sample in this repo (Jan to Jun 2024) | 12,756 | 9.9 MB |
| Incremental sample (one daily feed pull) | 36 | 25.2 KB |

Each event is about 0.7 KB of GeoJSON. The long-run average is roughly 78 events per day.

## Repository structure

```
dav-project/
├── README.md
├── samples/
│   ├── full_load_sample.geojson          # Jan-Jun 2024 slice of the full load
│   └── incremental_load_sample.geojson   # M2.5+ daily feed pull
├── scripts/
│   └── download_usgs.py                  # regenerates the full load and incremental sample
├── bronze/                               # raw ingestion notebooks (Phase 2)
├── silver/                               # cleaning and typing notebooks (Phase 2)
└── gold/                                 # star schema and aggregates (Phase 3)
```

The full 240.7 MB load is not stored in the repo. Regenerate it with the script below.

## Regenerating the data

```bash
pip install requests
python scripts/download_usgs.py full          # about 140 monthly files, 240.7 MB
python scripts/download_usgs.py incremental   # daily-feed sample
```

On Databricks, change `FULL_OUT_DIR` and `INCR_OUT_DIR` in the script to a Volume path such as `/Volumes/workspace/default/seismic/...`.

## PII

The dataset contains no personally identifiable information. Every field describes a physical event (magnitude, depth, coordinates, time, event ID).

## Planned layers

- **Bronze:** raw GeoJSON features flattened into Spark DataFrames, untouched.
- **Silver:** typed timestamps, deduplication on event ID (keeping the latest revision), a `status` flag for automatic vs reviewed events, `magType` kept as a column, depth bands, and region parsed from the `place` field.
- **Gold:** star schema (`fact_earthquake_events`, `dim_region`, `dim_time`) plus aggregates (daily counts by region, magnitude frequency, rolling 30-day activity).
- **Dashboard:** Power BI with a world map, regional trends, and a magnitude-frequency view.
