"""
download_usgs.py - Seismic Pulse data acquisition

Downloads earthquake data from the USGS Earthquake Hazards Program.

  python download_usgs.py full         # full load: monthly FDSN chunks, M2.5+, 2015 onward
  python download_usgs.py incremental  # incremental sample: M2.5+ daily feed

On Databricks, change the OUT_DIR values below to a Volume path, e.g.
/Volumes/workspace/default/seismic/full_load_monthly
"""
import os
import sys
import time
from datetime import date

import requests

FDSN_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
DAILY_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"

MIN_MAG = 2.5
START_YEAR = 2015
FULL_OUT_DIR = "data/full_load_monthly"
INCR_OUT_DIR = "samples"


def month_ranges(start_year):
    """Yield (first day of month, first day of next month) up to the current month."""
    today = date.today()
    y, m = start_year, 1
    while (y, m) <= (today.year, today.month):
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        yield date(y, m, 1), date(ny, nm, 1)
        y, m = ny, nm


def full_load():
    os.makedirs(FULL_OUT_DIR, exist_ok=True)
    total_bytes, total_events = 0, 0

    for start, end in month_ranges(START_YEAR):
        params = {
            "format": "geojson",
            "starttime": start.isoformat(),
            "endtime": end.isoformat(),
            "minmagnitude": MIN_MAG,
            "orderby": "time-asc",
        }
        for _ in range(3):  # retry on failure
            try:
                r = requests.get(FDSN_URL, params=params, timeout=60)
                r.raise_for_status()
                break
            except Exception as e:
                print("retry", start, e)
                time.sleep(3)
        else:
            print("FAILED", start)
            continue

        n = len(r.json()["features"])
        if n >= 20000:  # FDSN caps a single query at 20,000 events
            print("WARNING: hit the 20,000 cap for", start, "- use smaller chunks")

        with open(f"{FULL_OUT_DIR}/usgs_{start.isoformat()}.geojson", "wb") as f:
            f.write(r.content)

        total_bytes += len(r.content)
        total_events += n
        print(start, n, "events", round(len(r.content) / 1e6, 2), "MB")
        time.sleep(0.5)  # be polite to the server

    print("\nTOTAL EVENTS:", total_events)
    print("TOTAL SIZE (MB):", round(total_bytes / 1e6, 1))


def incremental_sample():
    os.makedirs(INCR_OUT_DIR, exist_ok=True)
    r = requests.get(DAILY_FEED_URL, timeout=60)
    r.raise_for_status()
    path = f"{INCR_OUT_DIR}/incremental_load_sample.geojson"
    with open(path, "wb") as f:
        f.write(r.content)
    print("Events:", len(r.json()["features"]))
    print("Size (KB):", round(len(r.content) / 1024, 1))


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "full":
        full_load()
    elif mode == "incremental":
        incremental_sample()
    else:
        print("Usage: python download_usgs.py [full|incremental]")
