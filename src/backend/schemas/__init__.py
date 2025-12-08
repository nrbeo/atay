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
    OverviewStats,
    SeasonStat,
    ShapeStat,
    TableRowCount,
    TimeSeriesPoint,
    TopCountry,
    TopLocation,
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
    "OverviewStats",
    "SeasonStat",
    "ShapeStat",
    "TableRowCount",
    "TimeSeriesPoint",
    "TopCountry",
    "TopLocation",
    "WeatherStat",
    "YearStat",
    # UFO
    "HeatmapPoint",
    "MapPoint",
    "UfoObservation",
    "UfoObservationDetail",
    "UfoObservationFilters",
]
