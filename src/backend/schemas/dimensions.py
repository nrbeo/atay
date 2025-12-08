"""
Pydantic schemas for dimension tables.

Defines response models for the dimension endpoints including
shapes, locations, weather stations, FRSHTT conditions, and dates.
"""

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field


class Shape(BaseModel):
    """UFO shape dimension model."""
    
    shape_key: int = Field(..., description="Unique identifier for the shape")
    shape: str = Field(..., description="Shape name (e.g., 'circle', 'triangle')")
    shape_category: str = Field(..., description="Broader shape category")
    
    class Config:
        json_schema_extra = {
            "example": {
                "shape_key": 1,
                "shape": "triangle",
                "shape_category": "geometric"
            }
        }


class FRSHTT(BaseModel):
    """
    Weather conditions dimension model.
    
    FRSHTT stands for: Fog, Rain, Snow, Hail, Thunder, Tornado.
    Extended with blue_sky indicator.
    """
    
    frshtt_key: int = Field(..., description="Unique identifier for weather pattern")
    fog: bool = Field(..., description="Fog present")
    rain: bool = Field(..., description="Rain present")
    snow: bool = Field(..., description="Snow present")
    hail: bool = Field(..., description="Hail present")
    thunder: bool = Field(..., description="Thunder present")
    tornado: bool = Field(..., description="Tornado present")
    blue_sky: bool = Field(..., description="Clear blue sky")
    label: Optional[str] = Field(None, description="Human-readable weather label")
    
    class Config:
        json_schema_extra = {
            "example": {
                "frshtt_key": 1,
                "fog": False,
                "rain": False,
                "snow": False,
                "hail": False,
                "thunder": False,
                "tornado": False,
                "blue_sky": True,
                "label": "clear / blue sky"
            }
        }


class Location(BaseModel):
    """Geographic location dimension model."""
    
    location_key: int = Field(..., description="Unique identifier for the location")
    city: Optional[str] = Field(None, description="City name")
    state: Optional[str] = Field(None, description="State/province code")
    country: Optional[str] = Field(None, description="Country code (ISO)")
    latitude: Optional[float] = Field(None, ge=-90, le=90, description="Latitude coordinate")
    longitude: Optional[float] = Field(None, ge=-180, le=180, description="Longitude coordinate")
    
    class Config:
        json_schema_extra = {
            "example": {
                "location_key": 7,
                "city": "san diego",
                "state": "ca",
                "country": "us",
                "latitude": 32.7152778,
                "longitude": -117.1563889
            }
        }


class WeatherStation(BaseModel):
    """Weather station dimension model."""
    
    station_key: int = Field(..., description="Unique identifier for the station")
    station_id: str = Field(..., description="External station identifier")
    station_name: Optional[str] = Field(None, description="Station name")
    station_latitude: Optional[float] = Field(None, ge=-90, le=90, description="Station latitude")
    station_longitude: Optional[float] = Field(None, ge=-180, le=180, description="Station longitude")
    station_elevation: Optional[float] = Field(None, description="Station elevation in meters")
    
    class Config:
        json_schema_extra = {
            "example": {
                "station_key": 7,
                "station_id": "3766399999",
                "station_name": "BIGGIN HILL, UK",
                "station_latitude": 51.330833,
                "station_longitude": 0.0325,
                "station_elevation": 182.27
            }
        }


class DateDim(BaseModel):
    """Date dimension model for time-based analysis."""
    
    date_key: int = Field(..., description="Unique identifier for the date")
    full_date: date = Field(..., description="Full calendar date")
    year: int = Field(..., description="Year component")
    month: int = Field(..., ge=1, le=12, description="Month component (1-12)")
    day: int = Field(..., ge=1, le=31, description="Day of month (1-31)")
    day_of_week: int = Field(..., ge=1, le=7, description="Day of week (1=Monday, 7=Sunday)")
    quarter: int = Field(..., ge=1, le=4, description="Quarter (1-4)")
    is_weekend: bool = Field(..., description="Whether it's a weekend day")
    season: str = Field(..., description="Season name (winter, spring, summer, fall)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "date_key": 1,
                "full_date": "2002-08-27",
                "year": 2002,
                "month": 8,
                "day": 27,
                "day_of_week": 2,
                "quarter": 3,
                "is_weekend": False,
                "season": "summer"
            }
        }


class DateRange(BaseModel):
    """Response model for date range queries."""
    
    start_date: Optional[date] = Field(None, description="Earliest date in dataset")
    end_date: Optional[date] = Field(None, description="Latest date in dataset")
    
    class Config:
        json_schema_extra = {
            "example": {
                "start_date": "1966-04-14",
                "end_date": "2012-05-13"
            }
        }
