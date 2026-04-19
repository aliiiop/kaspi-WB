from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.schemas import SearchResponse
from app.services.marketplaces import group_items_by_source, unified_search

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(
    title="Kaspi + WB Search",
    version="0.1.0",
)

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    q: str = Query(default="", max_length=256),
    limit: int = Query(default=24, ge=1, le=120),
) -> HTMLResponse:
    query = " ".join(q.split())
    items = []
    statuses = []

    if query:
        items, statuses = await unified_search(query, per_source_limit=limit)

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "query": query,
            "limit": limit,
            "items": items,
            "statuses": statuses,
            "grouped_items": group_items_by_source(items),
        },
    )


@app.get("/api/search", response_model=SearchResponse)
async def api_search(
    q: str = Query(..., min_length=1, max_length=256),
    limit: int = Query(default=24, ge=1, le=120),
) -> SearchResponse:
    query = " ".join(q.split())
    items, statuses = await unified_search(query, per_source_limit=limit)
    return SearchResponse(query=query, items=items, statuses=statuses)
