"""
UFO observation service module.

Encapsulates all data access logic for UFO observations,
including filtering, pagination, and geographic queries.
"""

from datetime import date
from typing import Any, Dict, List, Optional

from ..database import fetch_all, fetch_one
from ..utils import sql_queries as sql


class UfoService:
    """
    Service class for UFO observation data access.
    
    Provides methods for querying UFO sightings with various
    filters and for geographic visualization data.
    """
    
    @staticmethod
    def get_observations(
        limit: int = 100,
        offset: int = 0,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        country: Optional[str] = None,
        state: Optional[str] = None,
        city: Optional[str] = None,
        shape_key: Optional[int] = None,
        min_duration: Optional[float] = None,
        max_duration: Optional[float] = None,
        only_commented: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve paginated UFO observations with optional filters.
        
        Args:
            limit: Maximum number of results (default 100).
            offset: Number of results to skip for pagination.
            date_from: Filter observations from this date onwards.
            date_to: Filter observations up to this date.
            country: Filter by country code (case-insensitive).
            state: Filter by state/province (case-insensitive).
            city: Filter by city name (partial match, case-insensitive).
            shape_key: Filter by specific shape key.
            min_duration: Minimum sighting duration in seconds.
            max_duration: Maximum sighting duration in seconds.
            only_commented: If True, only return observations with comments.
        
        Returns:
            List of observation dictionaries with enriched dimension data.
        """
        query = sql.UFO_OBSERVATION_BASE
        params: List[Any] = []
        
        # Apply filters
        if date_from:
            query += " AND d.full_date >= %s"
            params.append(date_from)
        
        if date_to:
            query += " AND d.full_date <= %s"
            params.append(date_to)
        
        if country:
            query += " AND LOWER(l.country) = LOWER(%s)"
            params.append(country)
        
        if state:
            query += " AND LOWER(l.state) = LOWER(%s)"
            params.append(state)
        
        if city:
            query += " AND LOWER(l.city) LIKE LOWER(%s)"
            params.append(f"%{city}%")
        
        if shape_key:
            query += " AND s.shape_key = %s"
            params.append(shape_key)
        
        if min_duration is not None:
            query += " AND f.duration_seconds >= %s"
            params.append(min_duration)
        
        if max_duration is not None:
            query += " AND f.duration_seconds <= %s"
            params.append(max_duration)
        
        if only_commented:
            query += " AND f.has_comment = TRUE"
        
        # Add ordering and pagination
        query += " ORDER BY d.full_date DESC, f.fact_id LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        return fetch_all(query, tuple(params))
    
    @staticmethod
    def get_observation_by_id(fact_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieve a single observation by its fact ID.
        
        Args:
            fact_id: The unique identifier of the observation.
        
        Returns:
            Observation dictionary or None if not found.
        """
        return fetch_one(sql.UFO_OBSERVATION_BY_ID, (fact_id,))
    
    @staticmethod
    def get_observation_with_comment(fact_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieve an observation with its associated comment.
        
        Args:
            fact_id: The unique identifier of the observation.
        
        Returns:
            Observation dictionary with comment field, or None if not found.
        """
        observation = fetch_one(sql.UFO_OBSERVATION_BY_ID, (fact_id,))
        
        if not observation:
            return None
        
        # Fetch comment if available
        comment_row = fetch_one(sql.UFO_COMMENT_BY_FACT_ID, (fact_id,))
        observation["comment"] = comment_row["comment"] if comment_row else None
        
        return observation
    
    @staticmethod
    def get_map_points(
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        country: Optional[str] = None,
        max_points: int = 5000,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve aggregated geographic points for map visualization.
        
        Points are aggregated by location (city, country, lat, lon) with
        counts for efficient map marker rendering.
        
        Args:
            date_from: Filter observations from this date onwards.
            date_to: Filter observations up to this date.
            country: Filter by country code (case-insensitive).
            max_points: Maximum number of points to return.
        
        Returns:
            List of map point dictionaries with coordinates and counts.
        """
        query = sql.MAP_POINTS_BASE
        params: List[Any] = []
        
        if date_from:
            query += " AND d.full_date >= %s"
            params.append(date_from)
        
        if date_to:
            query += " AND d.full_date <= %s"
            params.append(date_to)
        
        if country:
            query += " AND LOWER(l.country) = LOWER(%s)"
            params.append(country)
        
        query += sql.MAP_POINTS_GROUP_BY
        params.append(max_points)
        
        return fetch_all(query, tuple(params))
    
    @staticmethod
    def get_heatmap_points(
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        country: Optional[str] = None,
        max_points: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve points for heatmap visualization.
        
        Returns location points with intensity values based on
        sighting counts for heatmap layer rendering.
        
        Args:
            date_from: Filter observations from this date onwards.
            date_to: Filter observations up to this date.
            country: Filter by country code (case-insensitive).
            max_points: Maximum number of points to return.
        
        Returns:
            List of heatmap point dictionaries with coordinates and intensity.
        """
        query = sql.MAP_HEATMAP_BASE
        params: List[Any] = []
        
        if date_from:
            query += " AND d.full_date >= %s"
            params.append(date_from)
        
        if date_to:
            query += " AND d.full_date <= %s"
            params.append(date_to)
        
        if country:
            query += " AND LOWER(l.country) = LOWER(%s)"
            params.append(country)
        
        query += sql.MAP_HEATMAP_GROUP_BY
        params.append(max_points)
        
        return fetch_all(query, tuple(params))


# Singleton instance for convenience
ufo_service = UfoService()
