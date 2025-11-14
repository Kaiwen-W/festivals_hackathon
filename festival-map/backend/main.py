#!/usr/bin/env python3
"""
Transit Demand Prediction - Direct Capacity Usage
Uses 70% of venue capacity as expected passengers
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional
import pandas as pd
import json
import requests
from geopy.distance import geodesic
import uvicorn

app = FastAPI(title="Transit Demand Prediction API - Direct Capacity")

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===========================================================================
# DATA MODELS
# ===========================================================================


class NearbyStopWithDemand(BaseModel):
    stop_name: str
    stop_locality: str
    latitude: float
    longitude: float
    distance_meters: int
    expected_passengers: int
    percentage_of_total: float
    bus_services: str


class EventWithDemand(BaseModel):
    event_id: int
    event_name: str
    event_category: str
    venue_name: str
    venue_town: str
    venue_address: str
    venue_capacity: Optional[int]
    expected_attendance: int
    venue_lat: float
    venue_lon: float
    performance_datetime: str
    min_price: Optional[float]
    nearby_stops: List[NearbyStopWithDemand]
    total_stops_within_300m: int
    total_expected_passengers: int


class EventListItem(BaseModel):
    event_id: int
    event_name: str
    event_category: str
    venue_name: str
    venue_town: str
    venue_capacity: Optional[int]
    performance_datetime: str


# ===========================================================================
# GLOBAL DATA
# ===========================================================================

events_df = None
stations_df = None
places_dict = {}


@app.on_event("startup")
async def load_data():
    """Load events and bus stops data when API starts"""
    global events_df, stations_df, places_dict

    print("=" * 70)
    print("LOADING DATA - DIRECT CAPACITY MODEL")
    print("=" * 70)

    # Load events data
    with open("../public/thistle_data.json", "r") as f:
        data = json.load(f)

    # Create places lookup WITH CAPACITY
    for place in data["places"]:
        place_id = place["place_id"]
        lat, lon = None, None
        capacity = None

        if "loc" in place:
            try:
                lat = float(place["loc"]["latitude"])
                lon = float(place["loc"]["longitude"])
            except:
                pass

        # Extract capacity from properties
        if "properties" in place:
            props = place["properties"]
            capacity = (
                props.get("place.capacity.max")
                or props.get("capacity.max")
                or props.get("capacity")
            )

            if capacity:
                try:
                    capacity = int(capacity)
                except:
                    capacity = None

        places_dict[place_id] = {
            "name": place.get("name", ""),
            "town": place.get("town", ""),
            "address": place.get("address", ""),
            "latitude": lat,
            "longitude": lon,
            "capacity": capacity,
        }

    print(f"Loaded {len(places_dict)} venues")
    venues_with_capacity = sum(
        1 for p in places_dict.values() if p["capacity"] is not None
    )
    print(f"  Venues with capacity data: {venues_with_capacity}")

    # Flatten events
    events_list = []

    for event in data["events"]:
        original_event_id = event.get("event_id")

        if original_event_id is None:
            continue

        for schedule in event["schedules"]:
            place = places_dict.get(schedule["place_id"], {})

            for performance in schedule["performances"]:
                min_price = next(
                    (
                        t.get("min_price")
                        for t in performance.get("tickets", [])
                        if t.get("type") == "Standard"
                    ),
                    None,
                )

                events_list.append(
                    {
                        "event_id": original_event_id,
                        "event_name": event["name"],
                        "event_category": event.get("category", ""),
                        "place_id": schedule["place_id"],
                        "venue_name": place.get("name", ""),
                        "venue_town": place.get("town", ""),
                        "venue_address": place.get("address", ""),
                        "venue_capacity": place.get("capacity"),
                        "venue_lat": place.get("latitude"),
                        "venue_lon": place.get("longitude"),
                        "performance_ts": performance["ts"],
                        "duration": performance.get("duration", 120),
                        "min_price": min_price,
                    }
                )

    events_df = pd.DataFrame(events_list)

    # Parse timestamps
    events_df["datetime"] = pd.to_datetime(
        events_df["performance_ts"], format="mixed", errors="coerce", utc=True
    ).dt.tz_localize(None)

    events_df = events_df[events_df["datetime"].notna()]

    # Load bus stops
    print("\nFetching bus stops from TfE API...")
    try:
        response = requests.get("https://tfe-opendata.com/api/v1/stops", timeout=10)
        stops_data = response.json()

        stations_df = pd.DataFrame(stops_data["stops"])
        stations_df["destinations"] = stations_df["destinations"].apply(
            lambda x: ", ".join(x) if isinstance(x, list) else ""
        )
        stations_df["services"] = stations_df["services"].apply(
            lambda x: ", ".join(x) if isinstance(x, list) else ""
        )

        print(f"Loaded {len(stations_df)} bus stops")
    except Exception as e:
        print(f"Warning: Could not load bus stops: {e}")
        stations_df = pd.DataFrame()

    print(f"\nLoaded {len(events_df)} performances")
    print(
        f"Date range: {events_df['datetime'].min().date()} to {events_df['datetime'].max().date()}"
    )

    unique_event_ids = events_df["event_id"].unique()
    print(f"Unique events: {len(unique_event_ids)}")

    # Show capacity statistics
    events_with_capacity = events_df[events_df["venue_capacity"].notna()]
    if len(events_with_capacity) > 0:
        print("\nCapacity statistics:")
        print(f"  Events with capacity: {len(events_with_capacity)}/{len(events_df)}")
        print(f"  Min capacity: {events_with_capacity['venue_capacity'].min()}")
        print(f"  Max capacity: {events_with_capacity['venue_capacity'].max()}")
        print(
            f"  Average capacity: {events_with_capacity['venue_capacity'].mean():.0f}"
        )

    print("\n" + "=" * 70)


# ===========================================================================
# HELPER FUNCTIONS
# ===========================================================================


def safe_float(value) -> Optional[float]:
    """Convert value to float, handling NaN"""
    if pd.isna(value):
        return None
    return float(value)


def safe_int(value) -> Optional[int]:
    """Convert value to int, handling NaN"""
    if pd.isna(value):
        return None
    return int(value)


def safe_str(value) -> str:
    """Convert value to string, handling NaN"""
    if pd.isna(value):
        return ""
    return str(value)


def distribute_passengers_to_stops(
    venue_lat: float,
    venue_lon: float,
    venue_capacity: Optional[int],
    max_distance_km: float = 0.3,
):
    """
    Distribute 70% of venue capacity across nearby bus stops
    Based on distance (closer stops get more passengers)
    """
    if pd.isna(venue_lat) or pd.isna(venue_lon) or stations_df.empty:
        return [], 0

    # Calculate expected attendance (70% of capacity)
    if venue_capacity is None or venue_capacity <= 0:
        # Default for venues without capacity data
        expected_attendance = 100
    else:
        expected_attendance = int(venue_capacity * 0.7)

    # Find nearby stops
    nearby = []
    venue_coords = (venue_lat, venue_lon)

    for idx, station in stations_df.iterrows():
        station_coords = (station["latitude"], station["longitude"])
        distance_km = geodesic(venue_coords, station_coords).km

        if distance_km <= max_distance_km:
            # Calculate distance weight (closer = higher weight)
            distance_weight = 1 / (1 + distance_km * 3)

            nearby.append(
                {
                    "stop_name": station["name"],
                    "locality": station["locality"],
                    "latitude": station["latitude"],
                    "longitude": station["longitude"],
                    "distance_meters": int(distance_km * 1000),
                    "distance_km": distance_km,
                    "distance_weight": distance_weight,
                    "services": station.get("services", ""),
                }
            )

    if not nearby:
        return [], expected_attendance

    # Calculate total weight
    total_weight = sum(stop["distance_weight"] for stop in nearby)

    # Distribute passengers proportionally to each stop
    for stop in nearby:
        # Percentage of total weight
        weight_percentage = stop["distance_weight"] / total_weight

        # Assign passengers based on weight
        stop["expected_passengers"] = int(expected_attendance * weight_percentage)
        stop["percentage_of_total"] = round(weight_percentage * 100, 1)

    # Sort by expected passengers (highest first)
    nearby_sorted = sorted(nearby, key=lambda x: x["expected_passengers"], reverse=True)

    return nearby_sorted, expected_attendance


# ===========================================================================
# API ENDPOINTS
# ===========================================================================


@app.get("/")
async def root():
    """API info"""
    return {
        "name": "Transit Demand Prediction API - Direct Capacity",
        "version": "7.0",
        "model": "70% of venue capacity distributed across nearby stops",
        "formula": "attendance = capacity — 0.7, then distributed by distance weight",
    }


@app.get("/event/{event_id}", response_model=EventWithDemand)
async def get_event_with_demand(
    event_id: int,
    max_distance: float = Query(0.3, description="Max distance to bus stops in km"),
):
    """Get event with capacity-based passenger distribution"""

    event = events_df[events_df["event_id"] == event_id]

    if len(event) == 0:
        available_ids = list(sorted(events_df["event_id"].unique())[:20])
        raise HTTPException(
            status_code=404,
            detail=f"Event ID {event_id} not found. Sample IDs: {available_ids}",
        )

    event = event.iloc[0]

    # Convert NaN values
    min_price_value = safe_float(event["min_price"])
    venue_address = safe_str(event["venue_address"])
    venue_capacity = safe_int(event["venue_capacity"])
    venue_lat = safe_float(event["venue_lat"])
    venue_lon = safe_float(event["venue_lon"])

    if venue_lat is None or venue_lon is None:
        raise HTTPException(
            status_code=400, detail=f"Event {event_id} venue has no coordinates"
        )

    # Distribute passengers to nearby stops
    nearby_stops, expected_attendance = distribute_passengers_to_stops(
        venue_lat, venue_lon, venue_capacity, max_distance
    )

    stops_list = [
        NearbyStopWithDemand(
            stop_name=stop["stop_name"],
            stop_locality=stop["locality"],
            latitude=stop["latitude"],
            longitude=stop["longitude"],
            distance_meters=stop["distance_meters"],
            expected_passengers=stop["expected_passengers"],
            percentage_of_total=stop["percentage_of_total"],
            bus_services=stop["services"],
        )
        for stop in nearby_stops
    ]

    total_passengers = sum(stop["expected_passengers"] for stop in nearby_stops)

    return EventWithDemand(
        event_id=int(event["event_id"]),
        event_name=event["event_name"],
        event_category=event["event_category"],
        venue_name=safe_str(event["venue_name"]),
        venue_town=safe_str(event["venue_town"]),
        venue_address=venue_address,
        venue_capacity=venue_capacity,
        expected_attendance=expected_attendance,
        venue_lat=venue_lat,
        venue_lon=venue_lon,
        performance_datetime=event["datetime"].strftime("%Y-%m-%d %H:%M"),
        min_price=min_price_value,
        nearby_stops=stops_list,
        total_stops_within_300m=len(stops_list),
        total_expected_passengers=total_passengers,
    )


@app.get("/events/search", response_model=List[EventListItem])
async def search_events(
    query: str = Query(None),
    venue: str = Query(None),
    town: str = Query(None),
    category: str = Query(None),
    date: str = Query(None),
    min_capacity: int = Query(None),
    limit: int = Query(50),
):
    """Search events with capacity filter"""
    filtered = events_df.copy()

    if query:
        filtered = filtered[
            filtered["event_name"].str.contains(query, case=False, na=False)
            | filtered["venue_name"].str.contains(query, case=False, na=False)
        ]

    if venue:
        filtered = filtered[
            filtered["venue_name"].str.contains(venue, case=False, na=False)
        ]

    if town:
        filtered = filtered[
            filtered["venue_town"].str.contains(town, case=False, na=False)
        ]

    if category:
        filtered = filtered[
            filtered["event_category"].str.contains(category, case=False, na=False)
        ]

    if date:
        try:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
            filtered = filtered[filtered["datetime"].dt.date == target_date]
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format")

    if min_capacity:
        filtered = filtered[filtered["venue_capacity"] >= min_capacity]

    filtered = filtered.head(limit)

    if len(filtered) == 0:
        return []

    results = []
    for _, event in filtered.iterrows():
        results.append(
            EventListItem(
                event_id=int(event["event_id"]),
                event_name=event["event_name"],
                event_category=event["event_category"],
                venue_name=safe_str(event["venue_name"]),
                venue_town=safe_str(event["venue_town"]),
                venue_capacity=safe_int(event["venue_capacity"]),
                performance_datetime=event["datetime"].strftime("%Y-%m-%d %H:%M"),
            )
        )

    return results


@app.get("/events/list", response_model=List[EventListItem])
async def list_events(skip: int = Query(0), limit: int = Query(50)):
    """List all events"""
    events = events_df.iloc[skip : skip + limit]

    results = []
    for _, event in events.iterrows():
        results.append(
            EventListItem(
                event_id=int(event["event_id"]),
                event_name=event["event_name"],
                event_category=event["event_category"],
                venue_name=safe_str(event["venue_name"]),
                venue_town=safe_str(event["venue_town"]),
                venue_capacity=safe_int(event["venue_capacity"]),
                performance_datetime=event["datetime"].strftime("%Y-%m-%d %H:%M"),
            )
        )

    return results


@app.get("/stats")
async def get_stats():
    """Get dataset statistics"""
    capacity_stats = {}
    if events_df["venue_capacity"].notna().any():
        capacity_stats = {
            "min": int(events_df["venue_capacity"].min()),
            "max": int(events_df["venue_capacity"].max()),
            "average": int(events_df["venue_capacity"].mean()),
            "events_with_capacity": int(events_df["venue_capacity"].notna().sum()),
        }

    return {
        "total_performances": len(events_df),
        "unique_events": int(events_df["event_id"].nunique()),
        "total_venues": int(events_df["venue_name"].nunique()),
        "total_bus_stops": len(stations_df) if not stations_df.empty else 0,
        "capacity_statistics": capacity_stats,
        "model": "70% capacity distributed by distance weight",
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
