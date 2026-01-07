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
    DurationWeatherStat,
    OverviewStats,
    SeasonStat,
    SeasonWeatherStat,
    ShapeSeasonStat,
    ShapeStat,
    ShapeWeatherStat,
    TemperatureStat,
    TimeSeriesPoint,
    TopCountry,
    TopLocation,
    TopShapeWeatherStat,
    VisibilityStat,
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


# =============================================================================
# CLIMATE × UFO CORRELATION ENDPOINTS
# =============================================================================

@router.get(
    "/shape-by-weather",
    response_model=List[ShapeWeatherStat],
    summary="Get shapes by weather condition",
    description="""
    Retrieve UFO shape distribution grouped by weather condition (FRSHTT pattern).
    
    Useful for analyzing which shapes are most commonly reported under specific
    weather conditions (fog, rain, clear sky, etc.).
    """,
    response_description="Shape distribution by weather condition",
    tags=["climate-correlation"],
)
def get_shape_by_weather() -> List[ShapeWeatherStat]:
    """
    Get shape distribution by weather condition.
    
    Returns:
        List of ShapeWeatherStat objects with shape, weather flags, and sighting count.
    """
    return stats_service.get_shape_by_weather()


@router.get(
    "/shape-by-season",
    response_model=List[ShapeSeasonStat],
    summary="Get shapes by season",
    description="""
    Retrieve UFO shape distribution grouped by season.
    
    Useful for analyzing whether certain shapes are more commonly reported
    in specific seasons (summer vs winter, etc.).
    """,
    response_description="Shape distribution by season",
    tags=["climate-correlation"],
)
def get_shape_by_season() -> List[ShapeSeasonStat]:
    """
    Get shape distribution by season.
    
    Returns:
        List of ShapeSeasonStat objects with shape, season, and sighting count.
    """
    return stats_service.get_shape_by_season()


@router.get(
    "/duration-by-weather",
    response_model=List[DurationWeatherStat],
    summary="Get average duration by weather",
    description="""
    Retrieve average sighting duration grouped by weather condition.
    
    Useful for analyzing whether sightings last longer under certain
    weather conditions (e.g., clear sky vs fog).
    """,
    response_description="Duration statistics by weather condition",
    tags=["climate-correlation"],
)
def get_duration_by_weather() -> List[DurationWeatherStat]:
    """
    Get average duration by weather condition.
    
    Returns:
        List of DurationWeatherStat objects with weather, avg duration, and count.
    """
    return stats_service.get_duration_by_weather()


@router.get(
    "/by-temperature",
    response_model=List[TemperatureStat],
    summary="Get sightings by temperature range",
    description="""
    Retrieve UFO sighting counts grouped by temperature ranges.
    
    Temperature buckets (Fahrenheit converted to Celsius labels):
    - < 0°C (Freezing): Below 32°F
    - 0-10°C (Cold): 32-50°F
    - 10-20°C (Mild): 50-68°F  
    - 20-30°C (Warm): 68-86°F
    - > 30°C (Hot): Above 86°F
    """,
    response_description="Sighting counts by temperature range",
    tags=["climate-correlation"],
)
def get_by_temperature() -> List[TemperatureStat]:
    """
    Get sighting distribution by temperature.
    
    Returns:
        List of TemperatureStat objects with temp range and sighting count.
    """
    return stats_service.get_by_temperature()


@router.get(
    "/by-visibility",
    response_model=List[VisibilityStat],
    summary="Get sightings by visibility range",
    description="""
    Retrieve UFO sighting counts grouped by visibility ranges.
    
    Visibility buckets:
    - < 1 mi (Poor): Very low visibility
    - 1-5 mi (Low): Low visibility
    - 5-10 mi (Medium): Medium visibility
    - > 10 mi (Good): Good visibility
    """,
    response_description="Sighting counts by visibility range",
    tags=["climate-correlation"],
)
def get_by_visibility() -> List[VisibilityStat]:
    """
    Get sighting distribution by visibility.
    
    Returns:
        List of VisibilityStat objects with visibility range and sighting count.
    """
    return stats_service.get_by_visibility()


@router.get(
    "/top-shapes-by-weather",
    response_model=List[TopShapeWeatherStat],
    summary="Get top 5 shapes per weather condition",
    description="""
    Retrieve the top 5 most reported UFO shapes for each weather condition.
    
    Useful for identifying which shapes dominate under specific weather patterns.
    """,
    response_description="Top shapes for each weather condition",
    tags=["climate-correlation"],
)
def get_top_shapes_by_weather() -> List[TopShapeWeatherStat]:
    """
    Get top 5 shapes for each weather condition.
    
    Returns:
        List of TopShapeWeatherStat objects grouped by weather condition.
    """
    return stats_service.get_top_shapes_by_weather()


@router.get(
    "/season-weather-matrix",
    response_model=List[SeasonWeatherStat],
    summary="Get season × weather matrix",
    description="""
    Retrieve UFO sighting counts for each combination of season and weather condition.
    
    Useful for building a heatmap showing how sightings distribute across
    the season/weather matrix.
    """,
    response_description="Season and weather combination statistics",
    tags=["climate-correlation"],
)
def get_season_weather_matrix() -> List[SeasonWeatherStat]:
    """
    Get sightings by season and weather combination.
    
    Returns:
        List of SeasonWeatherStat objects with season, weather, and sighting count.
    """
    return stats_service.get_season_weather_matrix()
