"""
Transport optimisation for Festival Ecosystem using local JSON events file.
Includes ingestion, processing, recommendation engine and API endpoints.
"""

import os
import logging
#import requests  # Commented for offline use
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from math import ceil
from typing import List, Dict
from geopy.distance import geodesic
from datetime import datetime
from datetime import timezone
import pandas as pd


# Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# Config / Constants
STOP_RADIUS_METERS = 400
DEFAULT_FILL = 0.6
AVG_BUS_SEAT_CAPACITY = 40
INGRESS_BUFFER_MINUTES = 60
EGRESS_BUFFER_MINUTES = 60
PRIME_TIME_START_HOUR = 18
PRIME_TIME_END_HOUR = 23


# TransportAPI client (offline dummy)
class TransportAPIClient:
    #BASE_URL = "https://transportapi.com/v3/uk/bus"  # Commented for offline

    def __init__(self, app_id: str = "", app_key: str = ""):
        self.app_id = app_id
        self.app_key = app_key
        #self.session = requests.Session()  # Commented

    def fetch_stop_timetable(self, stop_code: str) -> dict:
        """Offline dummy timetable"""
        return {
            "stops": [
                {"time": "17:00", "name": "Demo Stop 1", "locality": "City Center"},
                {"time": "17:15", "name": "Demo Stop 2", "locality": "City Center"}
            ]
        }

    def fetch_stops(self, lat: float, lon: float, radius: int = 2000) -> pd.DataFrame:
        """Offline dummy stops"""
        return pd.DataFrame([
            {"stopId": "demo_stop_1", "name": "Demo Stop 1", "latitude": lat+0.001, "longitude": lon+0.001},
            {"stopId": "demo_stop_2", "name": "Demo Stop 2", "latitude": lat+0.002, "longitude": lon-0.001}
        ])

# Helpers
def _coerce_datetime(x) -> datetime:
    if isinstance(x, datetime):
        return x
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except:
        return datetime.utcnow()

def is_prime_time(dt: datetime) -> bool:
    return PRIME_TIME_START_HOUR <= dt.hour < PRIME_TIME_END_HOUR

def _coerce_datetime(x) -> datetime:
    if isinstance(x, datetime):
        dt = x
    else:
        try:
            dt = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
        except:
            dt = datetime.utcnow().replace(tzinfo=timezone.utc)
    # make sure it’s aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

# Event ingestion
def load_events_from_json(json_file: str, start_date=None, end_date=None) -> pd.DataFrame:
    """Load events, optionally filter by date range."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        logger.info("Events JSON not found, using dummy data")
        raw = {
            "events": [
                {
                    "event_id": "e1",
                    "name": "Demo Festival 1",
                    "ranking_level": 2,
                    "ranking_in_level": 1,
                    "schedules": [
                        {"start_ts": datetime.utcnow().isoformat(),
                         "place": {"place_id": "p1", "name": "Demo Venue 1", "latitude": 53.001, "longitude": -2.001},
                         "performances": [{"ts": datetime.utcnow().isoformat(), "duration": 120, "sold_out": False}]}
                    ]
                }
            ]
        }


    # Build DataFrame
    records = []
    for e in raw.get("events", []):
        for sched in e.get("schedules", []):
            place = sched.get("place", {})
            lat = float(place.get("latitude") or 0.0)
            lon = float(place.get("longitude") or 0.0)
            capacity = int((place.get("properties") or {}).get("place.capacity.max") or 500)
            perfs = sched.get("performances") or [{"ts": sched.get("start_ts"), "duration": 120, "sold_out": False}]
            for perf in perfs:
                ts = _coerce_datetime(perf.get("ts") or sched.get("start_ts"))
                # Filter by date if requested
                if start_date and ts < start_date:
                    continue
                if end_date and ts > end_date:
                    continue
                duration = int(perf.get("duration") or 120)
                sold_out = bool(perf.get("sold_out") or False)
                records.append({
                    "event_id": e.get("event_id"),
                    "event_name": e.get("name"),
                    "place_id": place.get("place_id"),
                    "place_name": place.get("name"),
                    "lat": lat,
                    "lon": lon,
                    "capacity_max": capacity,
                    "ranking_level": e.get("ranking_level", 3),
                    "ranking_in_level": e.get("ranking_in_level", 1),
                    "ts": ts,
                    "duration": duration,
                    "sold_out": sold_out
                })

    df = pd.DataFrame(records)
    logger.info(f"Loaded {len(df)} events after filtering by date")
    return df

# Estimate attendance
def estimate_attendance(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["base_attendance"] = np.where(df["sold_out"], df["capacity_max"], df["capacity_max"] * DEFAULT_FILL)
    df["ranking_mult"] = 1.0 + ((4 - df["ranking_level"]) * 0.15) + ((4 - df["ranking_in_level"]) * 0.05)
    df["time_mult"] = df["ts"].apply(lambda x: 1.1 if is_prime_time(x) else 1.0)
    df["estimated_attendance"] = (df["base_attendance"] * df["ranking_mult"] * df["time_mult"]).clip(upper=df["capacity_max"])
    return df

# Compute UZI using geopy
def compute_undserved_zone_index(df_events: pd.DataFrame, df_stops: pd.DataFrame) -> pd.DataFrame:
    df = df_events.copy()
    stop_coords = df_stops[["stopId","latitude","longitude"]].values if not df_stops.empty else np.array([])
    stops_count, avg_buses = [], []
    for _, row in df.iterrows():
        lat0, lon0 = float(row["lat"]), float(row["lon"])
        cnt = 0
        for _, lat1, lon1 in stop_coords:
            if geodesic((lat0, lon0), (lat1, lon1)).meters <= STOP_RADIUS_METERS:
                cnt += 1
        stops_count.append(cnt)
        avg_buses.append(cnt * 4.0)
    df["stops_nearby"] = np.array(stops_count)
    df["avg_buses_per_hour"] = np.array(avg_buses)
    df["UZI_raw"] = df["estimated_attendance"] / (df["stops_nearby"] * df["avg_buses_per_hour"] + 1e-9)
    df["priority_boost"] = 1 + ((4 - df["ranking_level"]) * 0.25)
    df["UZI"] = df["UZI_raw"] * df["priority_boost"]
    min_uzi, max_uzi = df["UZI"].min(), df["UZI"].max()
    df["UZI_norm"] = (df["UZI"] - min_uzi) / (max_uzi - min_uzi if max_uzi - min_uzi > 0 else 1)
    return df

def process_transport_optimisation(json_file: str):
    # Filter events from mid-July to end of July
    start_date = datetime(2025, 7, 15, tzinfo=timezone.utc)
    end_date = datetime(2025, 7, 31, tzinfo=timezone.utc)
    
    events_df = load_events_from_json(json_file, start_date=start_date, end_date=end_date)
    events_df = events_df.drop_duplicates(subset=["event_id"]).reset_index(drop=True)
    events_df = estimate_attendance(events_df)
    
    center_lat = float(events_df["lat"].median() if not events_df.empty else 55.9533)
    center_lon = float(events_df["lon"].median() if not events_df.empty else -3.1883)

    ta_client = TransportAPIClient()  # offline dummy
    stops_df = ta_client.fetch_stops(center_lat, center_lon)

    uzi_df = compute_undserved_zone_index(events_df, stops_df)
    rec_df = generate_recommendations(uzi_df)

    # Peak flows
    df = events_df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["ingress_start"] = df["ts"] - pd.to_timedelta(INGRESS_BUFFER_MINUTES, "m")
    df["ingress_end"] = df["ts"]
    df["egress_start"] = df["ts"] + pd.to_timedelta(df["duration"], "m")
    df["egress_end"] = df["egress_start"] + pd.to_timedelta(EGRESS_BUFFER_MINUTES, "m")

    bins = pd.date_range(
        start=df["ingress_start"].min().floor("min"),
        end=df["egress_end"].max().ceil("min"),
        freq="15min"
    )
    inflow, outflow = pd.Series(0, index=bins), pd.Series(0, index=bins)
    for _, row in df.iterrows():
        mask_in = (inflow.index >= row["ingress_start"]) & (inflow.index < row["ingress_end"])
        mask_out = (outflow.index >= row["egress_start"]) & (outflow.index < row["egress_end"])
        inflow.loc[mask_in] += row["estimated_attendance"]
        outflow.loc[mask_out] += row["estimated_attendance"]

    peak_df = pd.DataFrame({"timeslot": bins, "inflow": inflow.values, "outflow": outflow.values})

    return {"venues": uzi_df, "peak_flows": peak_df, "recommendations": rec_df}


# Recommendations
def generate_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    recs = []
    for _, row in df.iterrows():
        rec_label, rec_msg = "Low", "No immediate transport action required."
        if row["UZI_norm"] >= 0.85 and int(row["ranking_level"]) == 1:
            extra = ceil(row["estimated_attendance"] / AVG_BUS_SEAT_CAPACITY)
            rec_label = "Critical"
            rec_msg = f"CRITICAL: {row['place_name']} – recommend {extra} shuttle(s) around {row['ts']}."
        elif row["UZI_norm"] >= 0.6:
            extra = ceil(row["estimated_attendance"] / (2*AVG_BUS_SEAT_CAPACITY))
            rec_label = "High"
            rec_msg = f"HIGH: {row['place_name']} – consider {extra} bus(es) or extended service."
        elif row["UZI_norm"] >= 0.4:
            rec_label = "Medium"
            rec_msg = f"MONITOR: {row['place_name']} – moderate transport load expected."
        recs.append({"place_id": row["place_id"], "rec_label": rec_label, "rec_message": rec_msg})
    return df.merge(pd.DataFrame(recs), on="place_id", how="left")