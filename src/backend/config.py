"""
Configuration module for ATAY Backend API.

Loads database and application settings from environment variables
with sensible defaults for development.
"""

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class DatabaseSettings:
    """Database connection settings."""
    
    name: str
    user: str
    password: str
    host: str
    port: int
    
    @property
    def connection_string(self) -> str:
        """Generate a connection string for logging (password masked)."""
        return f"postgresql://{self.user}:***@{self.host}:{self.port}/{self.name}"


@dataclass(frozen=True)
class AppSettings:
    """Application-level settings."""
    
    title: str = "ATAY UFO & Weather API"
    version: str = "1.0.0"
    description: str = "Backend API for UFO sightings and weather data analysis (INSA Lyon)"
    debug: bool = False
    
    # Pagination defaults
    default_limit: int = 100
    max_limit: int = 1000
    
    # Map settings
    default_max_map_points: int = 5000
    max_map_points: int = 20000


@lru_cache()
def get_database_settings() -> DatabaseSettings:
    """
    Load database settings from environment variables.
    
    Environment Variables:
        POSTGRES_WAREHOUSE_DB: Data warehouse database name (default: atay_dw)
        POSTGRES_USER: Database user (default: airflow)
        POSTGRES_PASSWORD: Database password (default: airflow)
        POSTGRES_HOST: Database host (default: postgres)
        POSTGRES_PORT: Database port (default: 5432)
    
    Returns:
        DatabaseSettings: Frozen dataclass with database configuration.
    """
    return DatabaseSettings(
        name=os.getenv("POSTGRES_WAREHOUSE_DB", "atay_dw"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
    )


@lru_cache()
def get_app_settings() -> AppSettings:
    """
    Load application settings.
    
    Environment Variables:
        ATAY_DEBUG: Enable debug mode (default: false)
    
    Returns:
        AppSettings: Frozen dataclass with application configuration.
    """
    debug = os.getenv("ATAY_DEBUG", "false").lower() in ("true", "1", "yes")
    return AppSettings(debug=debug)


# Convenience aliases for direct import
db_settings = get_database_settings()
app_settings = get_app_settings()
