"""
ATAY Backend API Package.

A modular FastAPI backend for UFO sighting and weather data analysis.
"""

from .main import app, create_app

__all__ = ["app", "create_app"]
__version__ = "1.0.0"
