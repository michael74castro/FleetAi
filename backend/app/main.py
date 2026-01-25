"""
FleetAI - Main FastAPI Application
AI-Enabled Fleet Reporting Platform
"""

from contextlib import asynccontextmanager
import logging
from typing import AsyncGenerator

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
import uvicorn

from app.core.config import settings
from app.core.database import init_database, close_database, DatabaseHealthCheck

# Import API routers
from app.api.v1 import auth, dashboards, reports, datasets, ai_agent, admin
from app.api.deps import get_db

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format=settings.LOG_FORMAT
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Application lifespan manager"""
    # Startup
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    logger.info(f"Environment: {settings.ENVIRONMENT}")

    # Initialize database
    try:
        await init_database()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        # Continue anyway - might be using external migrations

    yield

    # Shutdown
    logger.info("Shutting down application")
    await close_database()
    logger.info("Application shutdown complete")


# Create FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    description="AI-Enabled Fleet Reporting Platform for Vehicle Leasing Business",
    version=settings.APP_VERSION,
    docs_url="/api/docs" if settings.DEBUG else None,
    redoc_url="/api/redoc" if settings.DEBUG else None,
    openapi_url="/api/openapi.json" if settings.DEBUG else None,
    lifespan=lifespan,
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)


# Exception handlers
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors"""
    errors = []
    for error in exc.errors():
        errors.append({
            "field": ".".join(str(loc) for loc in error["loc"]),
            "message": error["msg"],
            "type": error["type"]
        })

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "detail": "Validation error",
            "errors": errors
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions"""
    logger.exception(f"Unhandled exception: {exc}")

    # Don't expose internal errors in production
    if settings.ENVIRONMENT == "production":
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"}
        )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": str(exc),
            "type": type(exc).__name__
        }
    )


# Include API routers
app.include_router(
    auth.router,
    prefix=f"{settings.API_V1_PREFIX}/auth",
    tags=["Authentication"]
)

app.include_router(
    dashboards.router,
    prefix=f"{settings.API_V1_PREFIX}/dashboards",
    tags=["Dashboards"]
)

app.include_router(
    reports.router,
    prefix=f"{settings.API_V1_PREFIX}/reports",
    tags=["Reports"]
)

app.include_router(
    datasets.router,
    prefix=f"{settings.API_V1_PREFIX}/datasets",
    tags=["Datasets"]
)

app.include_router(
    ai_agent.router,
    prefix=f"{settings.API_V1_PREFIX}/ai",
    tags=["AI Assistant"]
)

app.include_router(
    admin.router,
    prefix=f"{settings.API_V1_PREFIX}/admin",
    tags=["Administration"]
)


# Health check endpoints
@app.get("/health", tags=["Health"])
async def health_check():
    """Basic health check endpoint"""
    return {
        "status": "healthy",
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "environment": settings.ENVIRONMENT
    }


@app.get("/health/detailed", tags=["Health"])
async def detailed_health_check():
    """Detailed health check with database status"""
    db_health = await DatabaseHealthCheck.is_healthy()

    return {
        "status": "healthy" if db_health.get("healthy") else "degraded",
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "environment": settings.ENVIRONMENT,
        "components": {
            "database": db_health
        }
    }


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint"""
    return {
        "message": f"Welcome to {settings.APP_NAME}",
        "version": settings.APP_VERSION,
        "docs": "/api/docs" if settings.DEBUG else "Disabled in production"
    }


# API Info endpoint
@app.get(f"{settings.API_V1_PREFIX}/info", tags=["Info"])
async def api_info():
    """API information endpoint"""
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "api_version": "v1",
        "environment": settings.ENVIRONMENT,
        "features": {
            "dashboards": "No-code dashboard builder with drag-and-drop widgets",
            "reports": "No-code report builder with Excel/PDF export",
            "ai_assistant": "GPT-4 powered conversational analytics",
            "rbac": "Role-based access control with row-level security"
        },
        "roles": [
            {"name": "driver", "description": "View own vehicle data"},
            {"name": "fleet_admin", "description": "Manage company vehicles"},
            {"name": "client_contact", "description": "View company-wide data"},
            {"name": "account_manager", "description": "Manage assigned clients"},
            {"name": "super_user", "description": "Full read access"},
            {"name": "admin", "description": "Full system access"}
        ]
    }


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )
