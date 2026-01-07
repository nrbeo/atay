"""
Map router for geographic visualization endpoints.

Provides endpoints for map-based visualizations:
- Clustered map points
- Heatmap data
"""

from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Query

from ..schemas import HeatmapPoint, MapPoint
from ..services import ufo_service

router = APIRouter(prefix="/ufo/map", tags=["map"])


@router.get(
    "/points",
    response_model=List[MapPoint],
    summary="Get map points",
    description="""
    Retrieve aggregated geographic points for map visualization.
    
    Points are grouped by location (city, country, latitude, longitude)
    with counts representing the number of sightings at each location.
    
    Results are sorted by count descending to prioritize high-activity areas.
    """,
    response_description="List of map points with coordinates and counts",
)
def get_map_points(
    date_from: Optional[date] = Query(
        None,
        description="Filter observations from this date (inclusive)",
        example="2000-01-01",
    ),
    date_to: Optional[date] = Query(
        None,
        description="Filter observations until this date (inclusive)",
        example="2010-12-31",
    ),
    country: Optional[str] = Query(
        None,
        description="Filter by country code (case-insensitive)",
        example="us",
    ),
    max_points: int = Query(
        5000,
        ge=10,
        le=20000,
        description="Maximum number of points to return",
    ),
) -> List[MapPoint]:
    """
    Get aggregated map points for visualization.
    
    Points are aggregated by location to reduce data volume while
    maintaining visualization accuracy. Each point includes:
    - Coordinates (latitude, longitude)
    - Sighting count
    - City and country names
    
    Args:
        date_from: Optional start date filter.
        date_to: Optional end date filter.
        country: Optional country code filter.
        max_points: Maximum points to return (default 5000).
    
    Returns:
        List of MapPoint objects sorted by count descending.
    """
    return ufo_service.get_map_points(
        date_from=date_from,
        date_to=date_to,
        country=country,
        max_points=max_points,
    )


@router.get(
    "/heatmap",
    response_model=List[HeatmapPoint],
    summary="Get heatmap data",
    description="""
    Retrieve geographic points with intensity values for heatmap visualization.
    
    Points include latitude, longitude, and intensity (sighting count) for
    rendering heat layers on maps.
    """,
    response_description="List of heatmap points with coordinates and intensity",
)
def get_heatmap_points(
    date_from: Optional[date] = Query(
        None,
        description="Filter observations from this date (inclusive)",
    ),
    date_to: Optional[date] = Query(
        None,
        description="Filter observations until this date (inclusive)",
    ),
    country: Optional[str] = Query(
        None,
        description="Filter by country code (case-insensitive)",
    ),
    max_points: int = Query(
        10000,
        ge=100,
        le=50000,
        description="Maximum number of points to return",
    ),
) -> List[HeatmapPoint]:
    """
    Get heatmap points for geographic visualization.
    
    Returns location points with intensity values based on sighting
    density. Higher intensity indicates more sightings at that location.
    
    Args:
        date_from: Optional start date filter.
        date_to: Optional end date filter.
        country: Optional country code filter.
        max_points: Maximum points to return (default 10000).
    
    Returns:
        List of HeatmapPoint objects with coordinates and intensity values.
    """
    return ufo_service.get_heatmap_points(
        date_from=date_from,
        date_to=date_to,
        country=country,
        max_points=max_points,
    )


@router.get(
    "/bounds",
    summary="Get geographic bounds",
    description="Get the bounding box of all observations for map initialization.",
    response_description="Geographic bounds with min/max coordinates",
)
def get_map_bounds(
    country: Optional[str] = Query(
        None,
        description="Filter by country code to get bounds for specific country",
    ),
) -> dict:
    """
    Get geographic bounds for map initialization.
    
    Returns the bounding box (min/max latitude and longitude) for
    all observations, useful for setting initial map viewport.
    
    Args:
        country: Optional country filter to get bounds for specific region.
    
    Returns:
        Dictionary with bounds coordinates and center point.
    """
    # Get points and calculate bounds
    points = ufo_service.get_map_points(
        country=country,
        max_points=50000,  # Get all points for accurate bounds
    )
    
    if not points:
        return {
            "min_lat": -90,
            "max_lat": 90,
            "min_lng": -180,
            "max_lng": 180,
            "center_lat": 0,
            "center_lng": 0,
        }
    
    lats = [p["latitude"] for p in points]
    lngs = [p["longitude"] for p in points]
    
    return {
        "min_lat": min(lats),
        "max_lat": max(lats),
        "min_lng": min(lngs),
        "max_lng": max(lngs),
        "center_lat": sum(lats) / len(lats),
        "center_lng": sum(lngs) / len(lngs),
    }
