"""
ATAY UFO & Weather API - Main Application Module.

A modular FastAPI backend for UFO sighting and weather data analysis.
Developed for INSA Lyon data engineering project.

This module initializes the FastAPI application and registers all routers.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import get_app_settings, get_database_settings
from .routers import (
    dimensions_router,
    map_router,
    meta_router,
    stats_router,
    ufo_router,
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan context manager.
    
    Handles startup and shutdown events for the FastAPI application.
    Can be extended to manage database connection pools, caches, etc.
    """
    # Startup: Log configuration
    db_settings = get_database_settings()
    print(f"🚀 Starting ATAY Backend API")
    print(f"📊 Database: {db_settings.connection_string}")
    
    yield
    
    # Shutdown: Cleanup resources
    print("👋 Shutting down ATAY Backend API")


def create_app() -> FastAPI:
    """
    Application factory for creating the FastAPI instance.
    
    Creates and configures the FastAPI application with:
    - API metadata and documentation
    - CORS middleware for cross-origin requests
    - All API routers
    
    Returns:
        Configured FastAPI application instance.
    """
    app_settings = get_app_settings()
    
    app = FastAPI(
        title=app_settings.title,
        version=app_settings.version,
        description=app_settings.description,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        openapi_tags=[
            {
                "name": "meta",
                "description": "System health checks and metadata endpoints",
            },
            {
                "name": "dimensions",
                "description": "Dimension table data (shapes, locations, weather stations, etc.)",
            },
            {
                "name": "ufo",
                "description": "UFO observation data and search endpoints",
            },
            {
                "name": "map",
                "description": "Geographic visualization data for maps",
            },
            {
                "name": "stats",
                "description": "Aggregated statistics and analytics",
            },
        ],
    )
    
    # Configure CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Register routers
    app.include_router(meta_router)
    app.include_router(dimensions_router)
    app.include_router(ufo_router)
    app.include_router(map_router)
    app.include_router(stats_router)
    
    return app


# Create the application instance
app = create_app()


# Root endpoint redirect to documentation
@app.get("/", include_in_schema=False)
async def root():
    """Redirect root to API documentation."""
    return {
        "message": "Welcome to ATAY UFO & Weather API",
        "docs": "/docs",
        "redoc": "/redoc",
    }


# Health check at root level (alias for /meta/health)
@app.get("/health", tags=["meta"], include_in_schema=False)
async def health():
    """Root-level health check alias."""
    db_settings = get_database_settings()
    return {
        "status": "ok",
        "service": "atay-backend",
        "db": db_settings.name,
    }
