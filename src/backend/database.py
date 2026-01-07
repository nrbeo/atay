"""
Database module for ATAY Backend API.

Provides PostgreSQL connection management and query execution helpers
using psycopg2 with RealDictCursor for dictionary-like row access.
"""

from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from fastapi import HTTPException

from .config import get_database_settings


def get_connection() -> psycopg2.extensions.connection:
    """
    Create a new PostgreSQL database connection.
    
    For production use with high concurrency, consider implementing
    connection pooling using psycopg2.pool.ThreadedConnectionPool.
    
    Returns:
        psycopg2.extensions.connection: A new database connection.
    
    Raises:
        HTTPException: If connection fails (500 status code).
    """
    settings = get_database_settings()
    try:
        return psycopg2.connect(
            dbname=settings.name,
            user=settings.user,
            password=settings.password,
            host=settings.host,
            port=settings.port,
        )
    except psycopg2.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database connection failed: {e}"
        )


def fetch_all(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """
    Execute a SELECT query and return all rows as a list of dictionaries.
    
    Args:
        sql: The SQL query to execute. Use %s placeholders for parameters.
        params: Tuple of parameters to safely substitute into the query.
    
    Returns:
        List[Dict[str, Any]]: List of rows, each as a dictionary.
    
    Raises:
        HTTPException: If query execution fails (500 status code).
    
    Example:
        >>> rows = fetch_all(
        ...     "SELECT * FROM dim_shape WHERE shape_key = %s",
        ...     (1,)
        ... )
    """
    conn = None
    try:
        conn = get_connection()
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                return [dict(row) for row in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query error: {e}"
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def fetch_one(sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    """
    Execute a SELECT query and return the first row as a dictionary.
    
    Args:
        sql: The SQL query to execute. Use %s placeholders for parameters.
        params: Tuple of parameters to safely substitute into the query.
    
    Returns:
        Optional[Dict[str, Any]]: First row as a dictionary, or None if no results.
    
    Raises:
        HTTPException: If query execution fails (500 status code).
    
    Example:
        >>> row = fetch_one(
        ...     "SELECT * FROM dim_shape WHERE shape_key = %s",
        ...     (1,)
        ... )
    """
    rows = fetch_all(sql, params)
    return rows[0] if rows else None


def execute(sql: str, params: tuple = ()) -> int:
    """
    Execute a non-SELECT query (INSERT, UPDATE, DELETE) and return affected row count.
    
    Args:
        sql: The SQL statement to execute. Use %s placeholders for parameters.
        params: Tuple of parameters to safely substitute into the query.
    
    Returns:
        int: Number of rows affected by the operation.
    
    Raises:
        HTTPException: If execution fails (500 status code).
    
    Example:
        >>> count = execute(
        ...     "UPDATE dim_shape SET shape_category = %s WHERE shape_key = %s",
        ...     ("updated", 1)
        ... )
    """
    conn = None
    try:
        conn = get_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.rowcount
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database execution error: {e}"
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
