"""
Routers package for ATAY Backend API.

Re-exports all routers for registration in main app.
"""

from .dimensions import router as dimensions_router
from .map import router as map_router
from .meta import router as meta_router
from .stats import router as stats_router
from .ufo import router as ufo_router

__all__ = [
    "dimensions_router",
    "map_router",
    "meta_router",
    "stats_router",
    "ufo_router",
]
