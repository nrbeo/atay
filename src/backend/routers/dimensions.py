"""
Dimensions router for dimension table endpoints.

Provides endpoints for accessing dimension data:
- Shapes
- FRSHTT (weather conditions)
- Locations
- Weather stations
- Date ranges
"""

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from ..database import fetch_all, fetch_one
from ..schemas import DateRange, FRSHTT, Location, Shape, WeatherStation
from ..utils import sql_queries as sql

router = APIRouter(prefix="/dimensions", tags=["dimensions"])


# =============================================================================
# SHAPES
# =============================================================================

@router.get(
    "/shapes",
    response_model=List[Shape],
    summary="List all UFO shapes",
    description="Retrieve all distinct UFO shapes recorded in observations.",
    response_description="List of shape records",
)
def get_shapes() -> List[Shape]:
    """
    Get all UFO shapes from the dimension table.
    
    Returns:
        List of Shape objects containing shape_key, shape name, and category.
    """
    return fetch_all(sql.DIM_SHAPE_LIST)


@router.get(
    "/shapes/{shape_key}",
    response_model=Shape,
    summary="Get shape by ID",
    description="Retrieve a specific UFO shape by its key.",
    response_description="Shape record",
)
def get_shape_by_id(shape_key: int) -> Shape:
    """
    Get a specific shape by its key.
    
    Args:
        shape_key: Unique identifier for the shape.
    
    Returns:
        Shape object if found.
    
    Raises:
        HTTPException: 404 if shape not found.
    """
    row = fetch_one(sql.DIM_SHAPE_BY_ID, (shape_key,))
    if not row:
        raise HTTPException(status_code=404, detail="Shape not found")
    return row


# =============================================================================
# FRSHTT (WEATHER CONDITIONS)
# =============================================================================

@router.get(
    "/frshtt",
    response_model=List[FRSHTT],
    summary="List weather conditions",
    description="Retrieve all FRSHTT weather condition patterns. FRSHTT stands for Fog, Rain, Snow, Hail, Thunder, Tornado.",
    response_description="List of weather condition patterns",
)
def get_frshtt() -> List[FRSHTT]:
    """
    Get all FRSHTT weather condition patterns.
    
    FRSHTT encodes weather conditions as boolean flags:
    - Fog, Rain, Snow, Hail, Thunder, Tornado
    - Extended with blue_sky indicator
    
    Returns:
        List of FRSHTT objects with weather condition flags.
    """
    return fetch_all(sql.DIM_FRSHTT_LIST)


@router.get(
    "/frshtt/{frshtt_key}",
    response_model=FRSHTT,
    summary="Get weather condition by ID",
    description="Retrieve a specific weather condition pattern by its key.",
    response_description="Weather condition record",
)
def get_frshtt_by_id(frshtt_key: int) -> FRSHTT:
    """
    Get a specific FRSHTT pattern by its key.
    
    Args:
        frshtt_key: Unique identifier for the weather pattern.
    
    Returns:
        FRSHTT object if found.
    
    Raises:
        HTTPException: 404 if pattern not found.
    """
    row = fetch_one(sql.DIM_FRSHTT_BY_ID, (frshtt_key,))
    if not row:
        raise HTTPException(status_code=404, detail="FRSHTT pattern not found")
    return row


# =============================================================================
# LOCATIONS
# =============================================================================

@router.get(
    "/locations",
    response_model=List[Location],
    summary="List locations",
    description="Retrieve locations with optional filtering by country and city. Supports pagination.",
    response_description="List of location records",
)
def get_locations(
    country: Optional[str] = Query(
        None,
        description="Filter by country code (case-insensitive)",
        example="us",
    ),
    city: Optional[str] = Query(
        None,
        description="Filter by city name (partial match, case-insensitive)",
        example="seattle",
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of results",
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Number of results to skip",
    ),
) -> List[Location]:
    """
    Get locations with optional filters and pagination.
    
    Args:
        country: Optional country code filter (e.g., 'us', 'gb').
        city: Optional city name filter (partial match).
        limit: Maximum results to return (1-1000).
        offset: Number of results to skip for pagination.
    
    Returns:
        List of Location objects matching the criteria.
    """
    query = sql.DIM_LOCATION_BASE
    params = []
    
    if country:
        query += " AND LOWER(country) = LOWER(%s)"
        params.append(country)
    
    if city:
        query += " AND LOWER(city) LIKE LOWER(%s)"
        params.append(f"%{city}%")
    
    query += " ORDER BY location_key LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    return fetch_all(query, tuple(params))


@router.get(
    "/locations/{location_key}",
    response_model=Location,
    summary="Get location by ID",
    description="Retrieve a specific location by its key.",
    response_description="Location record",
)
def get_location_by_id(location_key: int) -> Location:
    """
    Get a specific location by its key.
    
    Args:
        location_key: Unique identifier for the location.
    
    Returns:
        Location object if found.
    
    Raises:
        HTTPException: 404 if location not found.
    """
    row = fetch_one(sql.DIM_LOCATION_BY_ID, (location_key,))
    if not row:
        raise HTTPException(status_code=404, detail="Location not found")
    return row


# =============================================================================
# WEATHER STATIONS
# =============================================================================

@router.get(
    "/weather-stations",
    response_model=List[WeatherStation],
    summary="List weather stations",
    description="Retrieve weather stations with pagination support.",
    response_description="List of weather station records",
)
def get_weather_stations(
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of results",
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Number of results to skip",
    ),
) -> List[WeatherStation]:
    """
    Get weather stations with pagination.
    
    Args:
        limit: Maximum results to return (1-1000).
        offset: Number of results to skip for pagination.
    
    Returns:
        List of WeatherStation objects.
    """
    return fetch_all(sql.DIM_WEATHER_STATION_LIST, (limit, offset))


@router.get(
    "/weather-stations/{station_key}",
    response_model=WeatherStation,
    summary="Get weather station by ID",
    description="Retrieve a specific weather station by its key.",
    response_description="Weather station record",
)
def get_weather_station_by_id(station_key: int) -> WeatherStation:
    """
    Get a specific weather station by its key.
    
    Args:
        station_key: Unique identifier for the station.
    
    Returns:
        WeatherStation object if found.
    
    Raises:
        HTTPException: 404 if station not found.
    """
    row = fetch_one(sql.DIM_WEATHER_STATION_BY_ID, (station_key,))
    if not row:
        raise HTTPException(status_code=404, detail="Weather station not found")
    return row


# =============================================================================
# DATE INFORMATION
# =============================================================================

@router.get(
    "/date-range",
    response_model=DateRange,
    summary="Get date range",
    description="Retrieve the date range covered by the observation data.",
    response_description="Start and end dates",
)
def get_date_range() -> DateRange:
    """
    Get the date range covered by observations in the dataset.
    
    Returns:
        DateRange object with start_date and end_date.
    """
    row = fetch_one(sql.DIM_DATE_RANGE)
    return row if row else {"start_date": None, "end_date": None}
