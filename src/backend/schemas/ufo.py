"""
Pydantic schemas for UFO observations and map data.

Defines response models for UFO sighting endpoints and
geographic visualization data.
"""

import datetime
from typing import Optional

from pydantic import BaseModel, Field


class UfoObservation(BaseModel):
    """
    Complete UFO observation record with enriched dimension data.
    
    Combines fact table data with joined dimensions for a
    comprehensive view of each sighting.
    """
    
    fact_id: int = Field(..., description="Unique observation identifier")
    date: datetime.date = Field(..., description="Date of the sighting")
    city: Optional[str] = Field(None, description="City where sighting occurred")
    state: Optional[str] = Field(None, description="State/province of sighting")
    country: Optional[str] = Field(None, description="Country code")
    shape: Optional[str] = Field(None, description="Reported UFO shape")
    station_name: Optional[str] = Field(None, description="Nearest weather station name")
    duration_seconds: Optional[float] = Field(None, ge=0, description="Duration of sighting in seconds")
    temp_mean: Optional[float] = Field(None, description="Mean temperature at time of sighting")
    visibility_mean: Optional[float] = Field(None, description="Mean visibility at time of sighting")
    has_comment: bool = Field(..., description="Whether observation has associated comment")
    frshtt_label: Optional[str] = Field(None, description="Weather condition label")
    latitude: Optional[float] = Field(None, ge=-90, le=90, description="Latitude of sighting")
    longitude: Optional[float] = Field(None, ge=-180, le=180, description="Longitude of sighting")
    
    class Config:
        json_schema_extra = {
            "example": {
                "fact_id": 1,
                "date": "2005-07-26",
                "city": "san diego",
                "state": "ca",
                "country": "us",
                "shape": "light",
                "station_name": "SAN DIEGO LINDBERGH",
                "duration_seconds": 2700.0,
                "temp_mean": 72.5,
                "visibility_mean": 10.0,
                "has_comment": True,
                "frshtt_label": "clear / blue sky",
                "latitude": 32.7152778,
                "longitude": -117.1563889
            }
        }


class UfoObservationDetail(UfoObservation):
    """
    Extended UFO observation with comment text.
    
    Includes the full comment text when available.
    """
    
    comment: Optional[str] = Field(None, description="Witness description/comment")
    
    class Config:
        json_schema_extra = {
            "example": {
                "fact_id": 1,
                "date": "2005-07-26",
                "city": "san diego",
                "state": "ca",
                "country": "us",
                "shape": "light",
                "station_name": "SAN DIEGO LINDBERGH",
                "duration_seconds": 2700.0,
                "temp_mean": 72.5,
                "visibility_mean": 10.0,
                "has_comment": True,
                "frshtt_label": "clear / blue sky",
                "latitude": 32.7152778,
                "longitude": -117.1563889,
                "comment": "Bright light hovering over the bay..."
            }
        }


class MapPoint(BaseModel):
    """
    Geographic point for map visualization.
    
    Aggregates sighting counts by location for efficient
    map marker rendering.
    """
    
    latitude: float = Field(..., ge=-90, le=90, description="Latitude coordinate")
    longitude: float = Field(..., ge=-180, le=180, description="Longitude coordinate")
    count: int = Field(..., ge=1, description="Number of sightings at this location")
    city: Optional[str] = Field(None, description="City name")
    country: Optional[str] = Field(None, description="Country code")
    
    class Config:
        json_schema_extra = {
            "example": {
                "latitude": 32.7152778,
                "longitude": -117.1563889,
                "count": 45,
                "city": "san diego",
                "country": "us"
            }
        }


class HeatmapPoint(BaseModel):
    """
    Point for heatmap visualization.
    
    Provides location and intensity for heatmap layer rendering.
    """
    
    latitude: float = Field(..., ge=-90, le=90, description="Latitude coordinate")
    longitude: float = Field(..., ge=-180, le=180, description="Longitude coordinate")
    intensity: int = Field(..., ge=1, description="Heat intensity (sighting count)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "latitude": 32.7152778,
                "longitude": -117.1563889,
                "intensity": 45
            }
        }


class UfoObservationFilters(BaseModel):
    """
    Query filters for UFO observation searches.
    
    Used for documenting available filter parameters.
    """
    
    date_from: Optional[datetime.date] = Field(None, description="Filter sightings from this date")
    date_to: Optional[datetime.date] = Field(None, description="Filter sightings until this date")
    country: Optional[str] = Field(None, description="Filter by country code")
    state: Optional[str] = Field(None, description="Filter by state/province")
    city: Optional[str] = Field(None, description="Filter by city (partial match)")
    shape_key: Optional[int] = Field(None, description="Filter by shape key")
    min_duration: Optional[float] = Field(None, ge=0, description="Minimum duration in seconds")
    max_duration: Optional[float] = Field(None, ge=0, description="Maximum duration in seconds")
    only_commented: bool = Field(False, description="Only return observations with comments")
