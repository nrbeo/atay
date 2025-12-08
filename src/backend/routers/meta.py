"""
Meta router for system and health check endpoints.

Provides endpoints for:
- Health checks
- Database table listing
- Row count statistics
"""

from typing import Dict, List

from fastapi import APIRouter

from ..config import get_database_settings
from ..database import fetch_all
from ..services import stats_service
from ..utils import sql_queries as sql

router = APIRouter(prefix="/meta", tags=["meta"])


@router.get(
    "/health",
    summary="Health check",
    description="Check if the API service is running and database is accessible.",
    response_description="Service status and database name",
)
def health_check() -> Dict[str, str]:
    """
    Perform a basic health check.
    
    Returns:
        Dictionary with status and service information:
        - status: "ok" if healthy
        - service: Service identifier
        - db: Connected database name
    """
    db_settings = get_database_settings()
    return {
        "status": "ok",
        "service": "atay-backend",
        "db": db_settings.name,
    }


@router.get(
    "/tables",
    summary="List database tables",
    description="Retrieve all tables in the public schema of the connected database.",
    response_description="List of table names",
)
def list_tables() -> Dict[str, List[str]]:
    """
    List all tables in the public schema.
    
    Returns:
        Dictionary with tables key containing list of table names.
    """
    rows = fetch_all(sql.META_LIST_TABLES)
    return {"tables": [row["table_name"] for row in rows]}


@router.get(
    "/row-counts",
    summary="Get table row counts",
    description="Retrieve row counts for all star schema tables.",
    response_description="Dictionary mapping table names to row counts",
)
def row_counts() -> Dict[str, int]:
    """
    Get row counts for all main tables in the star schema.
    
    Tables included:
    - dim_date
    - dim_location
    - dim_shape
    - dim_weather_station
    - dim_frshtt
    - fact_ufo_observation
    - ufo_comments_raw
    
    Returns:
        Dictionary mapping table names to their row counts.
    """
    return stats_service.get_table_row_counts()


@router.get(
    "/info",
    summary="API information",
    description="Get API version and configuration information.",
    response_description="API metadata",
)
def api_info() -> Dict[str, str]:
    """
    Get API information and version.
    
    Returns:
        Dictionary with API metadata:
        - name: API name
        - version: Current version
        - description: Brief description
    """
    return {
        "name": "ATAY UFO & Weather API",
        "version": "1.0.0",
        "description": "Backend API for UFO sightings and weather data analysis",
    }
