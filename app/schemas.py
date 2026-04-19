from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


Marketplace = Literal["wildberries", "kaspi"]


class SearchItem(BaseModel):
    source: Marketplace
    title: str
    brand: str | None = None
    price: float | None = None
    currency: str
    rating: float | None = None
    review_count: int | None = None
    seller: str | None = None
    delivery: str | None = None
    product_url: str
    image_url: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceStatus(BaseModel):
    source: Marketplace
    ok: bool
    message: str | None = None


class SearchResponse(BaseModel):
    query: str
    items: list[SearchItem]
    statuses: list[SourceStatus]

