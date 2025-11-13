"""
Transport optimisation for Festival Ecosystem.
Includes ingestion, processing, recommendation engine and API endpoints.
"""

import os
import logging
import time
import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from math import ceil
from geopy.distance import geodesic
from typing import List, Dict, Any

# configurations & constants
DATA_THISTLE_API_URL = os.getenv('DATATHISTLE_API_URL', 'https://api.datathistle.com/v1/events')
TRANSPORTAPI_APP_ID = '4b0a2590'
TRANSPORTAPI_APP_KEY = 'b30b0022f9719366d530efe99a7bf936'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
STOP_RADIUS_METERS = int(os.getenv('STOP_RADIUS_METERS', '400'))
DEFAULT_FILL = float(os.getenv('DEFAULT_FILL', '0.6'))
INGRESS_BUFFER_MINUTES = int(os.getenv('INGRESS_BUFFER_MINUTES', '60'))
EGRESS_BUFFER_MINUTES = int(os.getenv('EGRESS_BUFFER_MINUTES', '60'))
PRIME_TIME_START_HOUR = int(os.getenv('PRIME_TIME_START_HOUR', '18'))
PRIME_TIME_END_HOUR = int(os.getenv('PRIME_TIME_END_HOUR', '23'))
AVG_BUS_SEAT_CAPACITY = int(os.getenv('AVG_BUS_SEAT_CAPACITY', '40'))


# logging setup
# purpose: display debug messages on the console to identify where they come from(origin of files)
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


# Data Thistle client
class DataThistleClient:
    """Wrapper for Data Thistle API with safe fallback for missing fields"""
    def __init__(self, base_url: str, token: str = None):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}

    def ping(self) -> bool:
        """Check if API is reachable"""
        try:
            resp = self.session.get(f"{self.base_url}/ping", headers=self.headers, timeout=10)
            resp.raise_for_status()
            return True
        except Exception as e:
            logger.error("Ping failed: %s", e)
            return False

    def fetch_events(self, params: dict) -> dict:
        """
        Fetch events and places from Data Thistle API.
        Returns empty lists if the request fails.
        """
        try:
            resp = self.session.get(f"{self.base_url}/events", headers=self.headers, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            return {
                "events": data.get("events", []),
                "places": data.get("places", [])
            }
        except Exception as e:
            logger.error("Data Thistle events fetch failed: %s", e)
            return {"events": [], "places": []}


# TransportAPI client
class TransportAPIClient:
    BASE_URL = "https://transportapi.com/v3/uk/bus"

    def __init__(self, app_id: str, app_key: str):
        self.app_id = app_id
        self.app_key = app_key

    def fetch_stops(self, lat: float = 55.9533, lon: float = -3.1883, radius: int = 2000) -> pd.DataFrame:
        """Fetch bus stops within a radius of given coordinates"""
        try:
            params = {
                "app_id": self.app_id,
                "app_key": self.app_key,
                "type": "bus_stop",
                "lat": lat,
                "lon": lon,
                "r": radius
            }
            resp = requests.get(f"{self.BASE_URL}/places.json", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json().get("member", [])
            df = pd.DataFrame([{
                "stopId": d.get("atcocode"),
                "name": d.get("name"),
                "latitude": float(d.get("latitude", 0)),
                "longitude": float(d.get("longitude", 0))
            } for d in data])
            return df
        except Exception as e:
            logger.error("Failed to fetch stops: %s", e)
            return pd.DataFrame(columns=["stopId", "name", "latitude", "longitude"])

    def fetch_service_frequency(self, stop_ids: List[str], time_from: datetime, time_to: datetime) -> pd.DataFrame:
        """Estimate bus frequency per stop"""
        records = []
        for stop_id in stop_ids:
            try:
                params = {"app_id": self.app_id, "app_key": self.app_key, "group": "no", "nextbuses": "yes"}
                resp = requests.get(f"{self.BASE_URL}/stop/{stop_id}/live.json", params=params, timeout=15)
                resp.raise_for_status()
                departures = resp.json().get("departures", {})
                for service, dep_list in departures.items():
                    for dep in dep_list:
                        ts_str = dep.get("expected_departure_time") or dep.get("aimed_departure_time")
                        if ts_str:
                            ts = datetime.strptime(ts_str, "%H:%M")
                            records.append({"stopId": stop_id, "hour": ts.hour})
            except Exception as e:
                logger.warning("Stop %s fetch failed: %s", stop_id, e)
        df_freq = pd.DataFrame(records)
        if df_freq.empty:
            return pd.DataFrame({"stopId": stop_ids, "buses_per_hour": [4]*len(stop_ids)})
        return df_freq.groupby("stopId").size().reset_index(name="buses_per_hour")


# Processing
def is_prime_time(dt: datetime) -> bool:
    return PRIME_TIME_START_HOUR <= dt.hour < PRIME_TIME_END_HOUR

# Processing
def load_and_prepare_events(client: "DataThistleClient", date_from: datetime, date_to: datetime) -> pd.DataFrame:
    """
    Load events from Data Thistle, clean missing fields, and prepare for transport optimisation.
    Ensures all essential columns exist to avoid KeyErrors.
    """
    params = {'dateFrom': date_from.strftime('%Y-%m-%d'), 'dateTo': date_to.strftime('%Y-%m-%d'), 'impact': 'true'}
    feed = client.fetch_events(params)
    events = feed.get('events', [])
    places = {p.get('place_id'): p for p in feed.get('places', [])}

    records = []
    for e in events:
        ranking_level = e.get('ranking_level', 3)
        ranking_in_level = e.get('ranking_in_level', 1)
        for sched in e.get('schedules', []):
            place = sched.get('place') or places.get(sched.get('place_id'))
            if not place:
                logger.warning("No place found for schedule in event_id=%s", e.get('event_id'))
                continue

            # Capacity
            try:
                capacity_max = int(place.get('properties', {}).get('place.capacity.max', 500))
            except (ValueError, TypeError):
                capacity_max = 500

            # Coordinates
            try:
                lat = float(place.get('loc', {}).get('latitude', 0))
                lon = float(place.get('loc', {}).get('longitude', 0))
            except (ValueError, TypeError):
                lat, lon = 0.0, 0.0

            # Performances
            performances = sched.get('performances', [])
            if not performances:
                ts_str = sched.get('start_ts') or e.get('modified_ts') or datetime.utcnow().isoformat()
                performances = [{'ts': ts_str, 'duration': 120, 'sold_out': False}]

            for perf in performances:
                try:
                    ts = datetime.fromisoformat(perf.get('ts', datetime.utcnow().isoformat()).replace('Z', '+00:00'))
                except Exception:
                    ts = datetime.utcnow()

                duration = perf.get('duration', 120) or 120
                sold_out = perf.get('sold_out', False)

                records.append({
                    'event_id': e.get('event_id', 'unknown'),
                    'event_name': e.get('name', 'unknown'),
                    'place_id': place.get('place_id', 'unknown'),
                    'place_name': place.get('name', 'unknown'),
                    'lat': lat,
                    'lon': lon,
                    'capacity_max': capacity_max,
                    'ranking_level': ranking_level,
                    'ranking_in_level': ranking_in_level,
                    'ts': ts,
                    'duration': duration,
                    'sold_out': sold_out
                })

    df = pd.DataFrame(records)

    # Ensure all essential columns exist
    essential_cols = {
        'ranking_level': 3,
        'ranking_in_level': 1,
        'capacity_max': 500,
        'sold_out': False,
        'duration': 120,
        'ts': datetime.utcnow()
    }
    for col, default in essential_cols.items():
        if col not in df.columns:
            logger.warning("Column '%s' missing, creating with default value", col)
            df[col] = default
        else:
            df[col] = df[col].fillna(default)

    return df


def estimate_attendance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estimate attendance using capacity, ranking, time multipliers, and sold_out status
    """
    df = df.copy()

    # Ensure essential columns exist and are numeric
    essential_cols = {
        'capacity_max': 500,
        'sold_out': False,
        'ranking_level': 3,
        'ranking_in_level': 1,
        'ts': datetime.utcnow()
    }
    for col, default in essential_cols.items():
        if col not in df.columns:
            logger.warning("Column '%s' missing, creating default value", col)
            df[col] = default
        df[col] = df[col].fillna(default)
    
    # Convert numeric columns explicitly
    df['capacity_max'] = pd.to_numeric(df['capacity_max'], errors='coerce').fillna(500)
    df['ranking_level'] = pd.to_numeric(df['ranking_level'], errors='coerce').fillna(3)
    df['ranking_in_level'] = pd.to_numeric(df['ranking_in_level'], errors='coerce').fillna(1)
    
    # Ensure ts is datetime
    df['ts'] = pd.to_datetime(df['ts'], errors='coerce').fillna(datetime.utcnow())

    # Base attendance
    df['base_attendance'] = np.where(df['sold_out'], df['capacity_max'], df['capacity_max'] * DEFAULT_FILL)

    # Multipliers
    df['ranking_mult'] = 1.0 + ((4 - df['ranking_level']) * 0.15) + ((4 - df['ranking_in_level']) * 0.05)
    df['time_mult'] = df['ts'].apply(lambda x: 1.1 if is_prime_time(x) else 1.0)

    # Ensure all are numeric before multiplication
    df['ranking_mult'] = pd.to_numeric(df['ranking_mult'], errors='coerce').fillna(1.0)
    df['time_mult'] = pd.to_numeric(df['time_mult'], errors='coerce').fillna(1.0)
    df['base_attendance'] = pd.to_numeric(df['base_attendance'], errors='coerce').fillna(0)

    # Final estimated attendance
    df['estimated_attendance'] = (df['base_attendance'] * df['ranking_mult'] * df['time_mult']).clip(upper=df['capacity_max'])

    return df


def compute_undserved_zone_index(df_events: pd.DataFrame, df_stops: pd.DataFrame, service_freq_df: pd.DataFrame) -> pd.DataFrame:
    df = df_events.copy()
    stop_coords = df_stops[["stopId", "latitude", "longitude"]].values
    stops_count = []
    avg_buses = []
    for _, row in df.iterrows():
        lat0, lon0 = row["lat"], row["lon"]
        cnt = sum(geodesic((lat0, lon0), (lat1, lon1)).meters <= STOP_RADIUS_METERS for _, lat1, lon1 in stop_coords)
        stops_count.append(cnt)
        avg_buses.append(cnt * (service_freq_df["buses_per_hour"].mean() if not service_freq_df.empty else 4))
    df["stops_nearby"] = stops_count
    df["avg_buses_per_hour"] = avg_buses
    df["UZI_raw"] = df["estimated_attendance"] / (df["stops_nearby"] * df["avg_buses_per_hour"] + 1e-9)
    df["priority_boost"] = 1 + ((4 - df["ranking_level"]) * 0.25)
    df["UZI"] = df["UZI_raw"] * df["priority_boost"]
    df["UZI_norm"] = (df["UZI"] - df["UZI"].min()) / (df["UZI"].max() - df["UZI"].min() + 1e-9)
    return df

def aggregate_peak_flows(df_events: pd.DataFrame, bin_size_minutes: int = 15) -> pd.DataFrame:
    df = df_events.copy()
    df["ingress_start"] = df["ts"] - pd.to_timedelta(INGRESS_BUFFER_MINUTES, unit="m")
    df["ingress_end"] = df["ts"]
    df["egress_start"] = df["ts"] + pd.to_timedelta(df["duration"], unit="m")
    df["egress_end"] = df["egress_start"] + pd.to_timedelta(EGRESS_BUFFER_MINUTES, unit="m")
    start_time = df["ingress_start"].min().floor("15T")
    end_time = df["egress_end"].max().ceil("15T")
    bins = pd.date_range(start=start_time, end=end_time, freq=f"{bin_size_minutes}T") 
    inflow = pd.Series(0, index=bins)
    outflow = pd.Series(0, index=bins)
    for _, row in df.iterrows():
        inflow[(inflow.index >= row["ingress_start"]) & (inflow.index < row["ingress_end"])] += row["estimated_attendance"]
        outflow[(outflow.index >= row["egress_start"]) & (outflow.index < row["egress_end"])] += row["estimated_attendance"]
    return pd.DataFrame({"timeslot": bins, "inflow": inflow.values, "outflow": outflow.values})

def generate_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    recs = []
    for _, row in df.iterrows():
        rec_label, rec_msg = "Low", "No immediate transport action required."
        if row["UZI_norm"] >= 0.85 and row["ranking_level"] == 1:
            rec_label = "Critical"
            extra = ceil(row["estimated_attendance"] / AVG_BUS_SEAT_CAPACITY)
            rec_msg = f"CRITICAL: {row['place_name']} (event {row['event_name']}) – predicted overload. Recommend {extra} additional shuttle(s) around {row['ts']}."
        elif row["UZI_norm"] >= 0.6:
            rec_label = "High"
            extra = ceil(row["estimated_attendance"] / (2 * AVG_BUS_SEAT_CAPACITY))
            rec_msg = f"HIGH: {row['place_name']} – consider adding {extra} bus(es) or extending service hours."
        elif row["UZI_norm"] >= 0.4:
            rec_label = "Medium"
            rec_msg = f"MONITOR: {row['place_name']} – moderate transport load expected."
        recs.append({"place_id": row["place_id"], "rec_label": rec_label, "rec_message": rec_msg})
    rec_df = pd.DataFrame(recs)
    return df.merge(rec_df, on="place_id", how="left")

# Execution / Integration
def process_transport_optimisation(date_from: datetime, date_to: datetime) -> Dict[str, pd.DataFrame]:
    dt_client = DataThistleClient(DATA_THISTLE_API_URL)
    ta_client = TransportAPIClient(TRANSPORTAPI_APP_ID, TRANSPORTAPI_APP_KEY)

    events_df = load_and_prepare_events(dt_client, date_from, date_to)
    events_df = estimate_attendance(events_df)

    stops_df = ta_client.fetch_stops()
    service_freq_df = ta_client.fetch_service_frequency(stops_df["stopId"].tolist(), date_from, date_to)

    uzi_df = compute_undserved_zone_index(events_df, stops_df, service_freq_df)
    peak_df = aggregate_peak_flows(events_df)
    rec_df = generate_recommendations(uzi_df)

    return {"venues": uzi_df, "peak_flows": peak_df, "recommendations": rec_df}