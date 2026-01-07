"""
Statistics service module.

Encapsulates all data access logic for statistical aggregations,
analytics, and time series data.
"""

from typing import Any, Dict, List, Optional

from ..database import fetch_all, fetch_one
from ..utils import sql_queries as sql


class StatsService:
    """
    Service class for statistical data access.
    
    Provides methods for retrieving aggregated statistics,
    rankings, time series, and analytical summaries.
    """
    
    @staticmethod
    def get_overview() -> Dict[str, Any]:
        """
        Get high-level overview statistics for the entire dataset.
        
        Returns:
            Dictionary with counts and date ranges:
            - total_observations: Total UFO sightings
            - total_locations: Unique locations count
            - total_shapes: Distinct shapes count
            - total_stations: Weather stations count
            - total_frshtt_patterns: Weather patterns count
            - period_start: Earliest observation date
            - period_end: Latest observation date
        """
        result = fetch_one(sql.STATS_OVERVIEW)
        return result if result else {}
    
    @staticmethod
    def get_top_locations(
        country: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get locations ranked by number of sightings.
        
        Args:
            country: Filter by country code (case-insensitive).
            limit: Maximum number of locations to return.
        
        Returns:
            List of location dictionaries with city, country, and sighting count.
        """
        query = sql.STATS_TOP_LOCATIONS_BASE
        params: List[Any] = []
        
        if country:
            query += " AND LOWER(l.country) = LOWER(%s)"
            params.append(country)
        
        query += sql.STATS_TOP_LOCATIONS_GROUP_BY
        params.append(limit)
        
        return fetch_all(query, tuple(params))
    
    @staticmethod
    def get_top_countries(limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get countries ranked by number of sightings.
        
        Args:
            limit: Maximum number of countries to return.
        
        Returns:
            List of country dictionaries with country code and sighting count.
        """
        return fetch_all(sql.STATS_TOP_COUNTRIES, (limit,))
    
    @staticmethod
    def get_by_season() -> List[Dict[str, Any]]:
        """
        Get sighting counts grouped by season.
        
        Returns:
            List of season statistics with season name and sighting count.
        """
        return fetch_all(sql.STATS_BY_SEASON)
    
    @staticmethod
    def get_by_weather() -> List[Dict[str, Any]]:
        """
        Get sighting counts grouped by weather condition.
        
        Returns:
            List of weather statistics with condition label and sighting count.
        """
        return fetch_all(sql.STATS_BY_WEATHER)
    
    @staticmethod
    def get_by_shape() -> List[Dict[str, Any]]:
        """
        Get sighting counts grouped by UFO shape.
        
        Returns:
            List of shape statistics with shape, category, and sighting count.
        """
        return fetch_all(sql.STATS_BY_SHAPE)
    
    @staticmethod
    def get_by_year() -> List[Dict[str, Any]]:
        """
        Get sighting counts grouped by year.
        
        Returns:
            List of yearly statistics with year and sighting count.
        """
        return fetch_all(sql.STATS_BY_YEAR)
    
    @staticmethod
    def get_monthly_time_series(
        country: Optional[str] = None,
        shape_key: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get monthly time series data for observations.
        
        Args:
            country: Filter by country code (case-insensitive).
            shape_key: Filter by specific shape key.
        
        Returns:
            List of time series points with year, month, and observation count.
        """
        query = sql.STATS_TIME_SERIES_MONTHLY_BASE
        params: List[Any] = []
        
        if country:
            query += " AND LOWER(l.country) = LOWER(%s)"
            params.append(country)
        
        if shape_key:
            query += " AND s.shape_key = %s"
            params.append(shape_key)
        
        query += sql.STATS_TIME_SERIES_MONTHLY_GROUP_BY
        
        return fetch_all(query, tuple(params))
    
    @staticmethod
    def get_duration_distribution() -> List[Dict[str, Any]]:
        """
        Get sighting counts grouped by duration buckets.
        
        Duration buckets:
        - < 1 min
        - 1-5 min
        - 5-15 min
        - 15-60 min
        - > 1 hour
        
        Returns:
            List of duration bucket statistics with bucket label and count.
        """
        return fetch_all(sql.STATS_DURATION_DISTRIBUTION)
    
    @staticmethod
    def get_table_row_counts() -> Dict[str, int]:
        """
        Get row counts for all star schema tables.
        
        Returns:
            Dictionary mapping table names to their row counts.
        """
        counts = {}
        for table in sql.STAR_SCHEMA_TABLES:
            # Using safe table name (from constant list, not user input)
            query = sql.META_TABLE_ROW_COUNT.format(table=table)
            row = fetch_one(query)
            counts[table] = row["count"] if row else 0
        return counts

    # =========================================================================
    # CLIMATE × UFO CORRELATION METHODS
    # =========================================================================
    
    @staticmethod
    def get_shape_by_weather() -> List[Dict[str, Any]]:
        """
        Get shape distribution grouped by weather condition.
        
        Returns:
            List of dictionaries with shape, weather condition, and sighting count.
        """
        return fetch_all(sql.STATS_SHAPE_BY_WEATHER)
    
    @staticmethod
    def get_shape_by_season() -> List[Dict[str, Any]]:
        """
        Get shape distribution grouped by season.
        
        Returns:
            List of dictionaries with shape, season, and sighting count.
        """
        return fetch_all(sql.STATS_SHAPE_BY_SEASON)
    
    @staticmethod
    def get_duration_by_weather() -> List[Dict[str, Any]]:
        """
        Get average sighting duration grouped by weather condition.
        
        Returns:
            List of dictionaries with weather condition and average duration.
        """
        return fetch_all(sql.STATS_DURATION_BY_WEATHER)
    
    @staticmethod
    def get_by_temperature() -> List[Dict[str, Any]]:
        """
        Get sighting counts grouped by temperature ranges.
        
        Returns:
            List of dictionaries with temperature bucket and sighting count.
        """
        return fetch_all(sql.STATS_BY_TEMPERATURE)
    
    @staticmethod
    def get_by_visibility() -> List[Dict[str, Any]]:
        """
        Get sighting counts grouped by visibility ranges.
        
        Returns:
            List of dictionaries with visibility bucket and sighting count.
        """
        return fetch_all(sql.STATS_BY_VISIBILITY)
    
    @staticmethod
    def get_top_shapes_by_weather() -> List[Dict[str, Any]]:
        """
        Get top 5 shapes for each weather condition.
        
        Returns:
            List of dictionaries with weather condition, shape, and sighting count.
        """
        return fetch_all(sql.STATS_TOP_SHAPES_BY_WEATHER)
    
    @staticmethod
    def get_season_weather_matrix() -> List[Dict[str, Any]]:
        """
        Get sightings grouped by season and weather combination.
        
        Returns:
            List of dictionaries with season, weather condition, and sighting count.
        """
        return fetch_all(sql.STATS_SEASON_WEATHER_MATRIX)


# Singleton instance for convenience
stats_service = StatsService()
