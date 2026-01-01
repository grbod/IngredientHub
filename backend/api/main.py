"""
FastAPI application for IngredientHub backend API.

Provides REST endpoints for:
- Triggering and monitoring scraper runs
- Viewing scrape run history and statistics
- Managing and viewing alerts

Run with:
    cd backend
    source venv/bin/activate
    uvicorn api.main:app --reload --port 8000
"""

from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .routes import alerts, products, runs, scrapers
from .services.database import db_pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.

    Initializes database connection on startup and closes it on shutdown.
    """
    # Startup
    try:
        db_pool.initialize()
        print("Database connection initialized")
    except Exception as e:
        print(f"Warning: Could not initialize database: {e}")
        print("Some endpoints may not work without database connection")

    yield

    # Shutdown
    db_pool.close()
    print("Database connection closed")


app = FastAPI(
    title="IngredientHub API",
    description="Backend API for managing ingredient scrapers and viewing results",
    version="1.0.0",
    lifespan=lifespan,
)

# Configure CORS for frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev server
        "http://localhost:5174",  # Vite fallback port
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5174",
        "http://localhost:3000",  # Alternative dev port
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(scrapers.router)
app.include_router(runs.router)
app.include_router(alerts.router)
app.include_router(products.router)


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    timestamp: datetime
    database: str


@app.get("/api/health", response_model=HealthResponse, tags=["health"])
def health_check():
    """
    Health check endpoint.

    Returns:
        Health status including database connectivity
    """
    db_status = "unknown"

    try:
        with db_pool.get_cursor() as cursor:
            cursor.execute("SELECT 1")
            db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"

    return HealthResponse(
        status="ok",
        timestamp=datetime.utcnow(),
        database=db_status,
    )


@app.get("/", tags=["root"])
def root():
    """
    Root endpoint with API information.

    Returns:
        API welcome message and documentation link
    """
    return {
        "message": "IngredientHub API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/health",
    }
