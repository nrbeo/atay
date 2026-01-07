"""
Schemas package for ATAY Backend API.

Re-exports all Pydantic models for convenient importing.
"""

from .dimensions import (
    DateDim,
    DateRange,
    FRSHTT,
    Location,
    Shape,
    WeatherStation,
)
from .stats import (
    DurationBucket,
    DurationWeatherStat,
    OverviewStats,
    SeasonStat,
    SeasonWeatherStat,
    ShapeSeasonStat,
    ShapeStat,
    ShapeWeatherStat,
    TableRowCount,
    TemperatureStat,
    TimeSeriesPoint,
    TopCountry,
    TopLocation,
    TopShapeWeatherStat,
    VisibilityStat,
    WeatherStat,
    YearStat,
)
from .ufo import (
    HeatmapPoint,
    MapPoint,
    UfoObservation,
    UfoObservationDetail,
    UfoObservationFilters,
)

__all__ = [
    # Dimensions
    "DateDim",
    "DateRange",
    "FRSHTT",
    "Location",
    "Shape",
    "WeatherStation",
    # Stats
    "DurationBucket",
    "DurationWeatherStat",
    "OverviewStats",
    "SeasonStat",
    "SeasonWeatherStat",
    "ShapeSeasonStat",
    "ShapeStat",
    "ShapeWeatherStat",
    "TableRowCount",
    "TemperatureStat",
    "TimeSeriesPoint",
    "TopCountry",
    "TopLocation",
    "TopShapeWeatherStat",
    "VisibilityStat",
    "WeatherStat",
    "YearStat",
    # UFO
    "HeatmapPoint",
    "MapPoint",
    "UfoObservation",
    "UfoObservationDetail",
    "UfoObservationFilters",
]
