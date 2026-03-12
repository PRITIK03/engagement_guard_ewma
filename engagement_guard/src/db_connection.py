# ============================================================
#  src/db_connection.py
#  Handles MySQL connection using a connection pool.
#  Every other file imports get_connection() from here.
# ============================================================

import mysql.connector
from mysql.connector import pooling
import logging
import sys
import os

# So we can import config from the config/ folder
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.config import (
    DB_HOST, DB_PORT, DB_NAME,
    DB_USER, DB_PASSWORD
)

logger = logging.getLogger(__name__)

# ── Connection pool (created once when this module is imported) ──
_pool = None

def _get_pool():
    """Create the pool on first call, reuse after that."""
    global _pool
    if _pool is None:
        try:
            _pool = pooling.MySQLConnectionPool(
                pool_name       = "ewma_pool",
                pool_size       = 5,
                host            = DB_HOST,
                port            = DB_PORT,
                database        = DB_NAME,
                user            = DB_USER,
                password        = DB_PASSWORD,
                autocommit      = True,
                connect_timeout = 10
            )
            logger.info("MySQL connection pool created successfully.")
        except mysql.connector.Error as e:
            logger.error("Failed to create MySQL connection pool: %s", e)
            raise
    return _pool


def get_connection():
    """
    Get a connection from the pool.

    Usage:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT ...")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()   # returns it to the pool, does not actually close
    """
    return _get_pool().get_connection()


def test_connection():
    """Quick check — call this from terminal to verify DB is reachable."""
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()
        cursor.close()
        conn.close()
        print(f"Connected to MySQL successfully. Version: {version[0]}")
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False


if __name__ == "__main__":
    # Run this file directly to test your connection:
    # python src/db_connection.py
    logging.basicConfig(level=logging.INFO)
    test_connection()