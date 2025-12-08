"""
Pydantic schemas for statistics and analytics endpoints.

Defines response models for aggregated statistics, time series data,
and analytical summaries.
"""

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field


class OverviewStats(BaseModel):
    """
    High-level overview statistics for the entire dataset.
    
    Provides counts and date ranges for quick dashboard display.
    """
    
    total_observations: int = Field(..., ge=0, description="Total UFO sightings in database")
    total_locations: int = Field(..., ge=0, description="Number of unique locations")
    total_shapes: int = Field(..., ge=0, description="Number of distinct shapes")
    total_stations: int = Field(..., ge=0, description="Number of weather stations")
    total_frshtt_patterns: int = Field(..., ge=0, description="Number of weather condition patterns")
    period_start: Optional[date] = Field(None, description="Earliest observation date")
    period_end: Optional[date] = Field(None, description="Latest observation date")
    
    class Config:
        json_schema_extra = {
            "example": {
                "total_observations": 87459,
                "total_locations": 26289,
                "total_shapes": 29,
                "total_stations": 12159,
                "total_frshtt_patterns": 2,
                "period_start": "1966-04-14",
                "period_end": "2012-05-13"
            }
        }


class TopLocation(BaseModel):
    """Location ranking by number of sightings."""
    
    city: Optional[str] = Field(None, description="City name")
    country: Optional[str] = Field(None, description="Country code")
    sightings: int = Field(..., ge=0, description="Number of sightings")
    
    class Config:
        json_schema_extra = {
            "example": {
                "city": "seattle",
                "country": "us",
                "sightings": 1250
            }
        }


class TopCountry(BaseModel):
    """Country ranking by number of sightings."""
    
    country: Optional[str] = Field(None, description="Country code")
    sightings: int = Field(..., ge=0, description="Number of sightings")
    
    class Config:
        json_schema_extra = {
            "example": {
                "country": "us",
                "sightings": 75000
            }
        }


class SeasonStat(BaseModel):
    """Sighting counts grouped by season."""
    
    season: str = Field(..., description="Season name (winter, spring, summer, fall)")
    sightings: int = Field(..., ge=0, description="Number of sightings")
    
    class Config:
        json_schema_extra = {
            "example": {
                "season": "summer",
                "sightings": 28000
            }
        }


class WeatherStat(BaseModel):
    """Sighting counts grouped by weather condition."""
    
    label: Optional[str] = Field(None, description="Weather condition label")
    sightings: int = Field(..., ge=0, description="Number of sightings")
    
    class Config:
        json_schema_extra = {
            "example": {
                "label": "clear / blue sky",
                "sightings": 45000
            }
        }


class ShapeStat(BaseModel):
    """Sighting counts grouped by UFO shape."""
    
    shape: str = Field(..., description="Shape name")
    shape_category: str = Field(..., description="Shape category")
    sightings: int = Field(..., ge=0, description="Number of sightings")
    
    class Config:
        json_schema_extra = {
            "example": {
                "shape": "light",
                "shape_category": "luminous",
                "sightings": 15000
            }
        }


class YearStat(BaseModel):
    """Sighting counts grouped by year."""
    
    year: int = Field(..., description="Year")
    sightings: int = Field(..., ge=0, description="Number of sightings")
    
    class Config:
        json_schema_extra = {
            "example": {
                "year": 2010,
                "sightings": 5000
            }
        }


class TimeSeriesPoint(BaseModel):
    """Monthly time series data point."""
    
    year: int = Field(..., description="Year")
    month: int = Field(..., ge=1, le=12, description="Month (1-12)")
    observations: int = Field(..., ge=0, description="Number of observations")
    
    class Config:
        json_schema_extra = {
            "example": {
                "year": 2010,
                "month": 7,
                "observations": 450
            }
        }


class DurationBucket(BaseModel):
    """Sighting counts grouped by duration range."""
    
    duration_bucket: str = Field(..., description="Duration range label")
    count: int = Field(..., ge=0, description="Number of sightings")
    
    class Config:
        json_schema_extra = {
            "example": {
                "duration_bucket": "1-5 min",
                "count": 25000
            }
        }


class TableRowCount(BaseModel):
    """Row count for a database table."""
    
    table_name: str = Field(..., description="Table name")
    row_count: int = Field(..., ge=0, description="Number of rows")
    
    class Config:
        json_schema_extra = {
            "example": {
                "table_name": "fact_ufo_observation",
                "row_count": 87459
            }
        }
