"""
Services package for ATAY Backend API.

Re-exports service classes and singleton instances.
"""

from .stats_service import StatsService, stats_service
from .ufo_service import UfoService, ufo_service

__all__ = [
    "StatsService",
    "stats_service",
    "UfoService",
    "ufo_service",
]
