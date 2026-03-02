from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.db import close_pool, init_pool
from app.stats_service import StatsService

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await init_pool()
    app.state.stats_service = StatsService(pool)
    yield
    await close_pool()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/availability")
async def availability_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "availability.html")


@app.get("/channels")
async def channels_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "channels.html")


@app.get("/tokens")
async def tokens_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "tokens.html")


@app.get("/models")
async def models_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "models.html")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/dashboard")
async def get_dashboard(
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict:
    try:
        time_range = app.state.stats_service.resolve_time_range(start_date, end_date)
        return await app.state.stats_service.get_dashboard(time_range)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/stats/cost")
async def get_cost(
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict:
    try:
        time_range = app.state.stats_service.resolve_time_range(start_date, end_date)
        return await app.state.stats_service.get_cost_overview(time_range)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/stats/model")
async def get_model(
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    try:
        time_range = app.state.stats_service.resolve_time_range(start_date, end_date)
        return await app.state.stats_service.get_model_usage(time_range)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/stats/call-trend")
async def get_call_trend(
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    try:
        time_range = app.state.stats_service.resolve_time_range(start_date, end_date)
        return await app.state.stats_service.get_call_trend(time_range)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/stats/availability")
async def get_availability(
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    try:
        time_range = app.state.stats_service.resolve_time_range(start_date, end_date)
        return await app.state.stats_service.get_model_availability(time_range)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/stats/channel")
async def get_channel(
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    try:
        time_range = app.state.stats_service.resolve_time_range(start_date, end_date)
        return await app.state.stats_service.get_channel_usage(time_range)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/stats/token")
async def get_token(
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict:
    try:
        time_range = app.state.stats_service.resolve_time_range(start_date, end_date)
        return await app.state.stats_service.get_token_usage(time_range)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/stats/realtime-availability")
async def get_realtime_availability(window: str = "7d") -> dict:
    try:
        return await app.state.stats_service.get_realtime_availability(window)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
