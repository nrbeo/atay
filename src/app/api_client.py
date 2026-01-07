"""
API Client for ATAY UFO Analytics Backend
Streamlit equivalent of the React frontend API module
"""
import os
import requests
from typing import Optional, Dict, Any, List
import streamlit as st

# Use environment variable or default to local backend
# In Docker: API_URL=http://backend:8000
# Locally: defaults to http://localhost:8000
API_BASE_URL = os.environ.get("API_URL", "http://localhost:8000")


def _get(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Make a GET request to the API"""
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}", params=params, timeout=30)
        response.raise_for_status()
        return {"data": response.json(), "error": None}
    except requests.exceptions.RequestException as e:
        return {"data": None, "error": str(e)}


# ============ META ENDPOINTS ============
class Meta:
    @staticmethod
    @st.cache_data(ttl=300)
    def health() -> Dict:
        return _get("/meta/health")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def tables() -> Dict:
        return _get("/meta/tables")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def row_counts() -> Dict:
        return _get("/meta/row-counts")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def info() -> Dict:
        return _get("/meta/info")


# ============ DIMENSIONS ENDPOINTS ============
class Dimensions:
    @staticmethod
    @st.cache_data(ttl=600)
    def shapes() -> Dict:
        return _get("/dimensions/shapes")
    
    @staticmethod
    @st.cache_data(ttl=600)
    def shape_by_key(key: str) -> Dict:
        return _get(f"/dimensions/shapes/{key}")
    
    @staticmethod
    @st.cache_data(ttl=600)
    def frshtt() -> Dict:
        return _get("/dimensions/frshtt")
    
    @staticmethod
    @st.cache_data(ttl=600)
    def frshtt_by_key(key: str) -> Dict:
        return _get(f"/dimensions/frshtt/{key}")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def locations(country: Optional[str] = None, city: Optional[str] = None, 
                  limit: int = 100, offset: int = 0) -> Dict:
        params = {"limit": limit, "offset": offset}
        if country:
            params["country"] = country
        if city:
            params["city"] = city
        return _get("/dimensions/locations", params)
    
    @staticmethod
    @st.cache_data(ttl=600)
    def location_by_key(key: str) -> Dict:
        return _get(f"/dimensions/locations/{key}")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def weather_stations(limit: int = 100, offset: int = 0) -> Dict:
        return _get("/dimensions/weather-stations", {"limit": limit, "offset": offset})
    
    @staticmethod
    @st.cache_data(ttl=600)
    def weather_station_by_key(key: str) -> Dict:
        return _get(f"/dimensions/weather-stations/{key}")
    
    @staticmethod
    @st.cache_data(ttl=600)
    def date_range() -> Dict:
        return _get("/dimensions/date-range")


# ============ UFO OBSERVATIONS ENDPOINTS ============
class UFO:
    @staticmethod
    @st.cache_data(ttl=120)
    def observations(
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        city: Optional[str] = None,
        country: Optional[str] = None,
        state: Optional[str] = None,
        shape_key: Optional[int] = None,
        min_duration: Optional[int] = None,
        max_duration: Optional[int] = None,
        only_commented: Optional[bool] = None,
        limit: int = 20,
        offset: int = 0
    ) -> Dict:
        params = {"limit": limit, "offset": offset}
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to
        if city:
            params["city"] = city
        if country:
            params["country"] = country
        if state:
            params["state"] = state
        if shape_key:
            params["shape_key"] = shape_key
        if min_duration:
            params["min_duration"] = min_duration
        if max_duration:
            params["max_duration"] = max_duration
        if only_commented is not None:
            params["only_commented"] = only_commented
        return _get("/ufo/observations", params)
    
    @staticmethod
    @st.cache_data(ttl=300)
    def observation_by_id(fact_id: str) -> Dict:
        return _get(f"/ufo/observations/{fact_id}")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def observation_detail(fact_id: str) -> Dict:
        return _get(f"/ufo/observations/{fact_id}/detail")
    
    @staticmethod
    @st.cache_data(ttl=120)
    def count(**params) -> Dict:
        return _get("/ufo/count", params)


# ============ MAP ENDPOINTS ============
class Map:
    @staticmethod
    @st.cache_data(ttl=120)
    def points(
        country: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        max_points: Optional[int] = None
    ) -> Dict:
        params = {}
        if country:
            params["country"] = country
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to
        if max_points:
            params["max_points"] = max_points
        return _get("/ufo/map/points", params)
    
    @staticmethod
    @st.cache_data(ttl=120)
    def heatmap(
        country: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        max_points: Optional[int] = None
    ) -> Dict:
        params = {}
        if country:
            params["country"] = country
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to
        if max_points:
            params["max_points"] = max_points
        return _get("/ufo/map/heatmap", params)
    
    @staticmethod
    @st.cache_data(ttl=600)
    def bounds(country: Optional[str] = None) -> Dict:
        params = {}
        if country:
            params["country"] = country
        return _get("/ufo/map/bounds", params)


# ============ STATS ENDPOINTS ============
class Stats:
    @staticmethod
    @st.cache_data(ttl=300)
    def overview() -> Dict:
        return _get("/stats/overview")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def top_locations(country: Optional[str] = None, limit: int = 10) -> Dict:
        params = {"limit": limit}
        if country:
            params["country"] = country
        return _get("/stats/top-locations", params)
    
    @staticmethod
    @st.cache_data(ttl=300)
    def top_countries(limit: int = 10) -> Dict:
        return _get("/stats/top-countries", {"limit": limit})
    
    @staticmethod
    @st.cache_data(ttl=300)
    def by_season() -> Dict:
        return _get("/stats/by-season")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def by_weather() -> Dict:
        return _get("/stats/by-weather")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def by_shape() -> Dict:
        return _get("/stats/by-shape")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def by_year() -> Dict:
        return _get("/stats/by-year")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def time_series_monthly(country: Optional[str] = None, shape_key: Optional[int] = None) -> Dict:
        params = {}
        if country:
            params["country"] = country
        if shape_key:
            params["shape_key"] = shape_key
        return _get("/stats/time-series/monthly", params)
    
    @staticmethod
    @st.cache_data(ttl=300)
    def duration_distribution() -> Dict:
        return _get("/stats/duration-distribution")
    
    # Climate × UFO correlation endpoints
    @staticmethod
    @st.cache_data(ttl=300)
    def shape_by_weather() -> Dict:
        return _get("/stats/shape-by-weather")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def shape_by_season() -> Dict:
        return _get("/stats/shape-by-season")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def duration_by_weather() -> Dict:
        return _get("/stats/duration-by-weather")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def by_temperature() -> Dict:
        return _get("/stats/by-temperature")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def by_visibility() -> Dict:
        return _get("/stats/by-visibility")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def top_shapes_by_weather() -> Dict:
        return _get("/stats/top-shapes-by-weather")
    
    @staticmethod
    @st.cache_data(ttl=300)
    def season_weather_matrix() -> Dict:
        return _get("/stats/season-weather-matrix")


# API client instances
meta = Meta()
dimensions = Dimensions()
ufo = UFO()
map_api = Map()
stats = Stats()
