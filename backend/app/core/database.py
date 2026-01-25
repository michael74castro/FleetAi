"""
FleetAI - Database Configuration
SQLAlchemy async database setup supporting SQLite (dev) and MSSQL (production)
"""

from typing import AsyncGenerator
from contextlib import asynccontextmanager
import logging

from sqlalchemy import create_engine, text, event
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import Session, sessionmaker, DeclarativeBase
from sqlalchemy.pool import QueuePool, StaticPool

from .config import settings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """SQLAlchemy declarative base class"""
    pass


# Configure engine based on database type
if settings.DATABASE_TYPE == "sqlite":
    # SQLite configuration (for local development)
    sync_engine = create_engine(
        settings.database_url,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=settings.DEBUG,
    )

    async_engine = create_async_engine(
        settings.async_database_url,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=settings.DEBUG,
    )
else:
    # MSSQL configuration (for production)
    sync_engine = create_engine(
        settings.database_url,
        poolclass=QueuePool,
        pool_size=settings.MSSQL_POOL_SIZE,
        max_overflow=settings.MSSQL_MAX_OVERFLOW,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=settings.DEBUG,
    )

    async_engine = create_async_engine(
        settings.async_database_url,
        pool_size=settings.MSSQL_POOL_SIZE,
        max_overflow=settings.MSSQL_MAX_OVERFLOW,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=settings.DEBUG,
    )

# Session factories
SyncSessionLocal = sessionmaker(
    bind=sync_engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)

AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
    class_=AsyncSession,
)


# Event listener to set session context for RLS (MSSQL only)
if settings.DATABASE_TYPE == "mssql":
    @event.listens_for(sync_engine, "before_cursor_execute")
    def set_session_context(conn, cursor, statement, parameters, context, executemany):
        """Set session context for row-level security"""
        # Get user_id from connection info if available
        user_id = getattr(conn, '_fleetai_user_id', None)
        if user_id:
            cursor.execute(f"EXEC sp_set_session_context @key=N'user_id', @value={user_id}")


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency for getting async database sessions.

    Usage:
        @app.get("/items")
        async def get_items(db: AsyncSession = Depends(get_async_session)):
            result = await db.execute(select(Item))
            return result.scalars().all()
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


def get_sync_session() -> Session:
    """
    Get a synchronous database session.

    Usage:
        with get_sync_session() as db:
            db.execute(...)
    """
    return SyncSessionLocal()


@asynccontextmanager
async def get_session_with_user(user_id: int) -> AsyncGenerator[AsyncSession, None]:
    """
    Get an async session with user context set for RLS.

    Usage:
        async with get_session_with_user(user_id=1) as db:
            result = await db.execute(select(Item))  # RLS applied
    """
    async with AsyncSessionLocal() as session:
        try:
            # Set session context for RLS (MSSQL only)
            if settings.DATABASE_TYPE == "mssql":
                await session.execute(
                    text("EXEC sp_set_session_context @key=N'user_id', @value=:user_id"),
                    {"user_id": user_id}
                )
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def execute_raw_query(query: str, params: dict = None) -> list:
    """
    Execute a raw SQL query and return results.

    Args:
        query: SQL query string
        params: Optional parameters dict

    Returns:
        List of result rows as dicts
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(query), params or {})
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]


async def check_database_connection() -> bool:
    """Check if database connection is working"""
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
            return True
    except Exception as e:
        logger.error(f"Database connection check failed: {e}")
        return False


async def init_database():
    """Initialize database tables and schemas"""
    # In production, use Alembic migrations instead
    async with async_engine.begin() as conn:
        # Create tables if they don't exist
        await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables initialized")


async def close_database():
    """Close database connections"""
    await async_engine.dispose()
    sync_engine.dispose()
    logger.info("Database connections closed")


class DatabaseHealthCheck:
    """Database health check utility"""

    @staticmethod
    async def is_healthy() -> dict:
        """Check database health and return status"""
        try:
            async with AsyncSessionLocal() as session:
                # Basic connectivity
                result = await session.execute(text("SELECT 1 AS check_value"))
                basic_check = result.scalar()

                if settings.DATABASE_TYPE == "sqlite":
                    # SQLite health check
                    table_check = await session.execute(text(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ))
                    tables = [row[0] for row in table_check.fetchall()]
                    return {
                        "healthy": True,
                        "database_type": "sqlite",
                        "connectivity": basic_check == 1,
                        "tables": tables,
                    }
                else:
                    # MSSQL health check
                    schema_check = await session.execute(text("""
                        SELECT COUNT(*) FROM sys.schemas
                        WHERE name IN ('landing', 'staging', 'reporting', 'app')
                    """))
                    schema_count = schema_check.scalar()

                    table_check = await session.execute(text("""
                        SELECT
                            (SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('landing')) as landing_tables,
                            (SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('staging')) as staging_tables,
                            (SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('reporting')) as reporting_tables,
                            (SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('app')) as app_tables
                    """))
                    tables = table_check.fetchone()

                    return {
                        "healthy": True,
                        "database_type": "mssql",
                        "connectivity": basic_check == 1,
                        "schemas_found": schema_count,
                        "tables": {
                            "landing": tables[0] if tables else 0,
                            "staging": tables[1] if tables else 0,
                            "reporting": tables[2] if tables else 0,
                            "app": tables[3] if tables else 0,
                        }
                    }
        except Exception as e:
            return {
                "healthy": False,
                "error": str(e)
            }
