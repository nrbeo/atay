"""
Statistics router for analytics and aggregation endpoints.

Provides endpoints for:
- Overview statistics
- Rankings (top locations, countries)
- Distribution data (by season, weather, shape, duration)
- Time series data
"""

from typing import List, Optional

from fastapi import APIRouter, Query

from ..schemas import (
    DurationBucket,
    OverviewStats,
    SeasonStat,
    ShapeStat,
    TimeSeriesPoint,
    TopCountry,
    TopLocation,
    WeatherStat,
    YearStat,
)
from ..services import stats_service

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get(
    "/overview",
    response_model=OverviewStats,
    summary="Get overview statistics",
    description="""
    Retrieve high-level summary statistics for the entire UFO sighting dataset.
    
    Includes counts for observations, locations, shapes, weather stations,
    and weather patterns, plus the date range covered by the data.
    """,
    response_description="Overview statistics summary",
)
def get_overview() -> OverviewStats:
    """
    Get high-level overview statistics.
    
    Returns:
        OverviewStats object with:
        - total_observations: Total UFO sightings count
        - total_locations: Unique locations count
        - total_shapes: Distinct shapes count
        - total_stations: Weather stations count
        - total_frshtt_patterns: Weather patterns count
        - period_start: Earliest observation date
        - period_end: Latest observation date
    """
    return stats_service.get_overview()


@router.get(
    "/top-locations",
    response_model=List[TopLocation],
    summary="Get top locations by sightings",
    description="Retrieve locations ranked by the number of UFO sightings.",
    response_description="List of top locations",
)
def get_top_locations(
    country: Optional[str] = Query(
        None,
        description="Filter by country code to get top locations within a country",
        example="us",
    ),
    limit: int = Query(
        10,
        ge=1,
        le=100,
        description="Number of top locations to return",
    ),
) -> List[TopLocation]:
    """
    Get locations ranked by sighting count.
    
    Args:
        country: Optional country filter (case-insensitive).
        limit: Maximum locations to return (default 10).
    
    Returns:
        List of TopLocation objects with city, country, and sighting count.
    """
    return stats_service.get_top_locations(country=country, limit=limit)


@router.get(
    "/top-countries",
    response_model=List[TopCountry],
    summary="Get top countries by sightings",
    description="Retrieve countries ranked by the number of UFO sightings.",
    response_description="List of top countries",
)
def get_top_countries(
    limit: int = Query(
        10,
        ge=1,
        le=50,
        description="Number of top countries to return",
    ),
) -> List[TopCountry]:
    """
    Get countries ranked by sighting count.
    
    Args:
        limit: Maximum countries to return (default 10).
    
    Returns:
        List of TopCountry objects with country code and sighting count.
    """
    return stats_service.get_top_countries(limit=limit)


@router.get(
    "/by-season",
    response_model=List[SeasonStat],
    summary="Get sightings by season",
    description="Retrieve UFO sighting counts grouped by season (winter, spring, summer, fall).",
    response_description="List of season statistics",
)
def get_by_season() -> List[SeasonStat]:
    """
    Get sighting distribution by season.
    
    Returns:
        List of SeasonStat objects with season name and sighting count,
        sorted by sighting count descending.
    """
    return stats_service.get_by_season()


@router.get(
    "/by-weather",
    response_model=List[WeatherStat],
    summary="Get sightings by weather condition",
    description="Retrieve UFO sighting counts grouped by weather condition (FRSHTT pattern).",
    response_description="List of weather statistics",
)
def get_by_weather() -> List[WeatherStat]:
    """
    Get sighting distribution by weather condition.
    
    Returns:
        List of WeatherStat objects with weather label and sighting count,
        sorted by sighting count descending.
    """
    return stats_service.get_by_weather()


@router.get(
    "/by-shape",
    response_model=List[ShapeStat],
    summary="Get sightings by UFO shape",
    description="Retrieve UFO sighting counts grouped by reported shape.",
    response_description="List of shape statistics",
)
def get_by_shape() -> List[ShapeStat]:
    """
    Get sighting distribution by UFO shape.
    
    Returns:
        List of ShapeStat objects with shape, category, and sighting count,
        sorted by sighting count descending.
    """
    return stats_service.get_by_shape()


@router.get(
    "/by-year",
    response_model=List[YearStat],
    summary="Get sightings by year",
    description="Retrieve UFO sighting counts grouped by year.",
    response_description="List of yearly statistics",
)
def get_by_year() -> List[YearStat]:
    """
    Get sighting counts by year.
    
    Returns:
        List of YearStat objects with year and sighting count,
        sorted by year ascending.
    """
    return stats_service.get_by_year()


@router.get(
    "/time-series/monthly",
    response_model=List[TimeSeriesPoint],
    summary="Get monthly time series",
    description="""
    Retrieve UFO sighting counts as a monthly time series.
    
    Useful for trend analysis and time-based visualizations.
    Can be filtered by country and/or UFO shape.
    """,
    response_description="Monthly time series data",
)
def get_monthly_time_series(
    country: Optional[str] = Query(
        None,
        description="Filter by country code (case-insensitive)",
        example="us",
    ),
    shape_key: Optional[int] = Query(
        None,
        description="Filter by shape key (get available shapes from /dimensions/shapes)",
    ),
) -> List[TimeSeriesPoint]:
    """
    Get monthly time series of observation counts.
    
    Args:
        country: Optional country filter.
        shape_key: Optional shape filter.
    
    Returns:
        List of TimeSeriesPoint objects with year, month, and observation count,
        sorted chronologically.
    """
    return stats_service.get_monthly_time_series(
        country=country,
        shape_key=shape_key,
    )


@router.get(
    "/duration-distribution",
    response_model=List[DurationBucket],
    summary="Get duration distribution",
    description="""
    Retrieve UFO sighting counts grouped by duration buckets.
    
    Duration buckets:
    - < 1 min: Very short sightings
    - 1-5 min: Short sightings
    - 5-15 min: Medium sightings
    - 15-60 min: Long sightings
    - > 1 hour: Extended sightings
    """,
    response_description="Duration distribution data",
)
def get_duration_distribution() -> List[DurationBucket]:
    """
    Get sighting distribution by duration.
    
    Returns:
        List of DurationBucket objects with duration range label and count.
    """
    return stats_service.get_duration_distribution()
