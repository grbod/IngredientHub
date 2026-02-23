"""
Database service for PostgreSQL connection management.

Loads credentials from .env file and provides connection pooling
for the PostgreSQL database.
"""

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


# Load .env from backend directory
_backend_dir = Path(__file__).parent.parent.parent
_env_path = _backend_dir / ".env"
load_dotenv(_env_path)


def get_database_url() -> Optional[str]:
    """Get the PostgreSQL database URL from environment variables."""
    return os.getenv("DATABASE_URL")


class DatabasePool:
    """
    Simple connection pool for PostgreSQL.

    Uses a single connection that is reused across requests.
    For production, consider using a proper connection pool like psycopg2.pool.
    """

    def __init__(self):
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._db_url: Optional[str] = None

    def initialize(self) -> None:
        """Initialize the database connection."""
        self._db_url = get_database_url()
        if not self._db_url:
            raise ValueError(
                "DATABASE_URL not found in environment. "
                "Please set it in backend/.env"
            )
        self._connect()

    def _connect(self) -> None:
        """Establish database connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass

        self._conn = psycopg2.connect(self._db_url)
        self._conn.autocommit = False

    def _ensure_connection(self) -> None:
        """Ensure the connection is alive, reconnect if needed."""
        if self._conn is None:
            self._connect()
            return

        try:
            # Test connection with a simple query
            with self._conn.cursor() as cursor:
                cursor.execute("SELECT 1")
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            # Connection lost, reconnect
            self._connect()

    @contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """
        Get a database connection from the pool.

        Yields:
            A psycopg2 connection object.

        Example:
            with db_pool.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM vendors")
                    rows = cursor.fetchall()
        """
        self._ensure_connection()
        try:
            yield self._conn
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    @contextmanager
    def get_cursor(
        self,
        cursor_factory=psycopg2.extras.RealDictCursor
    ) -> Generator[psycopg2.extensions.cursor, None, None]:
        """
        Get a cursor with automatic connection management.

        Args:
            cursor_factory: The cursor factory to use. Defaults to RealDictCursor
                           for dictionary-style row access.

        Yields:
            A psycopg2 cursor object.

        Example:
            with db_pool.get_cursor() as cursor:
                cursor.execute("SELECT * FROM vendors")
                rows = cursor.fetchall()
                # rows is a list of dicts: [{'vendor_id': 1, 'name': 'IO'}, ...]
        """
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


# Global database pool instance
db_pool = DatabasePool()


def get_db():
    """
    Dependency for FastAPI routes to get database cursor.

    Usage in routes:
        @router.get("/items")
        def get_items(db = Depends(get_db)):
            with db as cursor:
                cursor.execute("SELECT * FROM items")
                return cursor.fetchall()
    """
    return db_pool.get_cursor
