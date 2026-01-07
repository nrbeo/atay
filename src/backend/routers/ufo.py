"""
UFO observations router.

Provides endpoints for accessing UFO sighting data:
- List observations with filters
- Get observation details
- Get observation comments
"""

from datetime import date
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from ..schemas import UfoObservation, UfoObservationDetail
from ..services import ufo_service

router = APIRouter(prefix="/ufo", tags=["ufo"])


@router.get(
    "/observations",
    response_model=List[UfoObservation],
    summary="List UFO observations",
    description="""
    Retrieve UFO sighting observations with comprehensive filtering options.
    
    Observations are enriched with dimension data including location details,
    shape information, weather station data, and weather conditions.
    
    Results are paginated and sorted by date descending.
    """,
    response_description="List of UFO observation records",
)
def list_observations(
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
    state: Optional[str] = Query(
        None,
        description="Filter by state/province code (case-insensitive)",
        example="ca",
    ),
    city: Optional[str] = Query(
        None,
        description="Filter by city name (partial match, case-insensitive)",
        example="seattle",
    ),
    shape_key: Optional[int] = Query(
        None,
        description="Filter by shape key (get available shapes from /dimensions/shapes)",
    ),
    min_duration: Optional[float] = Query(
        None,
        ge=0,
        description="Minimum sighting duration in seconds",
    ),
    max_duration: Optional[float] = Query(
        None,
        ge=0,
        description="Maximum sighting duration in seconds",
    ),
    only_commented: bool = Query(
        False,
        description="Only return observations that have associated comments",
    ),
) -> List[UfoObservation]:
    """
    List UFO observations with optional filters.
    
    Supports extensive filtering by:
    - Date range
    - Geographic location (country, state, city)
    - UFO shape
    - Sighting duration
    - Comment availability
    
    Returns:
        List of UfoObservation objects with enriched dimension data.
    """
    return ufo_service.get_observations(
        limit=limit,
        offset=offset,
        date_from=date_from,
        date_to=date_to,
        country=country,
        state=state,
        city=city,
        shape_key=shape_key,
        min_duration=min_duration,
        max_duration=max_duration,
        only_commented=only_commented,
    )


@router.get(
    "/observations/{fact_id}",
    response_model=UfoObservation,
    summary="Get observation by ID",
    description="Retrieve a specific UFO observation by its fact ID.",
    response_description="UFO observation record",
)
def get_observation(fact_id: int) -> UfoObservation:
    """
    Get a specific UFO observation by fact ID.
    
    Args:
        fact_id: Unique identifier for the observation.
    
    Returns:
        UfoObservation object with enriched dimension data.
    
    Raises:
        HTTPException: 404 if observation not found.
    """
    row = ufo_service.get_observation_by_id(fact_id)
    if not row:
        raise HTTPException(status_code=404, detail="Observation not found")
    return row


@router.get(
    "/observations/{fact_id}/detail",
    response_model=UfoObservationDetail,
    summary="Get observation with comment",
    description="Retrieve a UFO observation including its full comment text if available.",
    response_description="UFO observation with comment",
)
def get_observation_detail(fact_id: int) -> UfoObservationDetail:
    """
    Get a UFO observation with its associated comment.
    
    Args:
        fact_id: Unique identifier for the observation.
    
    Returns:
        UfoObservationDetail object with observation data and comment text.
    
    Raises:
        HTTPException: 404 if observation not found.
    """
    row = ufo_service.get_observation_with_comment(fact_id)
    if not row:
        raise HTTPException(status_code=404, detail="Observation not found")
    return row


@router.get(
    "/count",
    summary="Count observations",
    description="Get the total count of observations matching the specified filters.",
    response_description="Count of matching observations",
)
def count_observations(
    date_from: Optional[date] = Query(None, description="Filter from date"),
    date_to: Optional[date] = Query(None, description="Filter until date"),
    country: Optional[str] = Query(None, description="Filter by country"),
    shape_key: Optional[int] = Query(None, description="Filter by shape"),
    only_commented: bool = Query(False, description="Only commented observations"),
) -> dict:
    """
    Count observations matching the specified filters.
    
    Uses the same filter parameters as list_observations but returns
    only the count for efficiency.
    
    Returns:
        Dictionary with count of matching observations.
    """
    # Reuse get_observations with a large limit to count
    # For production, implement a dedicated COUNT query for better performance
    observations = ufo_service.get_observations(
        limit=1000000,  # Large limit for counting
        offset=0,
        date_from=date_from,
        date_to=date_to,
        country=country,
        shape_key=shape_key,
        only_commented=only_commented,
    )
    return {"count": len(observations)}
