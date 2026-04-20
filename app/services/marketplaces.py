from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from urllib.parse import quote, urlencode

from app.schemas import SearchItem, SourceStatus

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)

WB_SEARCH_URL = "https://search.wb.ru/exactmatch/ru/common/v4/search"
KASPI_SEARCH_PAGE_URL = "https://kaspi.kz/shop/search/"
KASPI_RESULTS_URL = "https://kaspi.kz/yml/product-view/pl/results"

WB_DEST = os.getenv("WB_DEST", "-1257786")
KASPI_CITY_CODE = os.getenv("KASPI_CITY_CODE", "750000000")
CACHE_TTL_SECONDS = 60

SEARCH_CACHE: dict[tuple[str, str], tuple[float, list[dict]]] = {}


class SearchError(RuntimeError):
    pass


async def unified_search(
    query: str,
    per_source_limit: int = 20,
) -> tuple[list[SearchItem], list[SourceStatus]]:
    tasks = [
        search_wildberries(query, per_source_limit),
        search_kaspi(query, per_source_limit),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    items: list[SearchItem] = []
    statuses: list[SourceStatus] = []

    for source_name, result in zip(("wildberries", "kaspi"), results, strict=True):
        if isinstance(result, Exception):
            statuses.append(
                SourceStatus(
                    source=source_name,
                    ok=False,
                    message=str(result) or result.__class__.__name__,
                )
            )
            continue

        source_items = result
        items.extend(source_items)
        statuses.append(
            SourceStatus(
                source=source_name,
                ok=True,
                message=f"Найдено: {len(source_items)}",
            )
        )

    return items, statuses


async def search_wildberries(query: str, limit: int) -> list[SearchItem]:
    cached = _get_cached_products("wildberries", query)
    if cached is None:
        params = {
            "appType": "1",
            "curr": "rub",
            "dest": WB_DEST,
            "query": query,
            "resultset": "catalog",
            "sort": "popular",
            "spp": "30",
            "suppressSpellcheck": "false",
        }
        url = f"{WB_SEARCH_URL}?{urlencode(params)}"
        raw = await _run_curl(
            url,
            headers={"Accept": "application/json, text/plain, */*"},
            source_name="Wildberries",
        )
        payload = _decode_json(raw, "Wildberries")
        cached = payload.get("products", [])
        _store_cached_products("wildberries", query, cached)

    return [_wb_item(product) for product in cached[:limit]]


async def search_kaspi(query: str, limit: int) -> list[SearchItem]:
    cached = _get_cached_products("kaspi", query)
    if cached is None:
        # Kaspi opens the JSON endpoint only after the regular search page is visited first.
        referer = f"https://kaspi.kz/shop/search/?text={quote(query)}"
        init_url = f"{KASPI_SEARCH_PAGE_URL}?{urlencode({'text': query})}"
        results_url = f"{KASPI_RESULTS_URL}?{urlencode({'text': query})}"

        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as cookie_file:
            cookie_path = Path(cookie_file.name)

        try:
            await _run_curl(
                init_url,
                cookie_write_path=cookie_path,
                source_name="Kaspi init",
                discard_output=True,
            )
            raw = await _run_curl(
                results_url,
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "X-Requested-With": "XMLHttpRequest",
                },
                referer=referer,
                cookie_read_path=cookie_path,
                source_name="Kaspi",
            )
        finally:
            cookie_path.unlink(missing_ok=True)

        payload = _decode_json(raw, "Kaspi")
        cached = payload.get("data", [])
        _store_cached_products("kaspi", query, cached)

    return [_kaspi_item(product) for product in cached[:limit]]


def group_items_by_source(items: list[SearchItem]) -> dict[str, list[SearchItem]]:
    grouped = {"wildberries": [], "kaspi": []}
    for item in items:
        grouped[item.source].append(item)
    return grouped


def _wb_item(product: dict) -> SearchItem:
    price = None
    sizes = product.get("sizes") or []
    if sizes:
        price_data = sizes[0].get("price") or {}
        raw_price = price_data.get("product") or price_data.get("basic")
        if raw_price is not None:
            price = round(raw_price / 100, 2)

    delivery = None
    if product.get("time1") is not None and product.get("time2") is not None:
        delivery = f"{product['time1']}-{product['time2']} дн."

    product_id = product.get("id")
    return SearchItem(
        source="wildberries",
        title=product.get("name") or "Без названия",
        brand=product.get("brand"),
        price=price,
        currency="RUB",
        rating=_to_float(product.get("rating") or product.get("reviewRating")),
        review_count=_to_int(product.get("feedbacks")),
        seller=product.get("supplier"),
        delivery=delivery,
        product_url=f"https://www.wildberries.ru/catalog/{product_id}/detail.aspx",
        image_url=None,
        metadata={"id": product_id},
    )


def _kaspi_item(product: dict) -> SearchItem:
    image_url = None
    preview_images = product.get("previewImages") or []
    if preview_images:
        first_image = preview_images[0]
        image_url = (
            first_image.get("medium")
            or first_image.get("large")
            or first_image.get("small")
        )

    delivery = None
    if product.get("deliveryDuration"):
        delivery = str(product["deliveryDuration"]).replace("_", " ").title()

    shop_link = product.get("shopLink", "")
    if shop_link.startswith("/p/"):
        shop_link = f"/shop{shop_link}"

    return SearchItem(
        source="kaspi",
        title=product.get("title") or "Без названия",
        brand=product.get("brand"),
        price=_to_float(product.get("unitSalePrice") or product.get("unitPrice")),
        currency=product.get("currency") or "KZT",
        rating=_to_float(product.get("rating")),
        review_count=_to_int(product.get("reviewsQuantity")),
        seller=product.get("bestMerchant"),
        delivery=delivery,
        product_url=f"https://kaspi.kz{shop_link}",
        image_url=image_url,
        metadata={
            "id": product.get("id"),
            "city_code": KASPI_CITY_CODE,
        },
    )


async def _run_curl(
    url: str,
    *,
    source_name: str,
    headers: dict[str, str] | None = None,
    referer: str | None = None,
    cookie_write_path: Path | None = None,
    cookie_read_path: Path | None = None,
    discard_output: bool = False,
) -> bytes:
    curl_bin = shutil.which("curl") or shutil.which("curl.exe")
    if not curl_bin:
        raise SearchError("curl не найден в PATH.")

    command = [
        curl_bin,
        "-L",
        "--silent",
        "--show-error",
        "--compressed",
        "--max-time",
        "20",
        "-A",
        USER_AGENT,
    ]

    for key, value in (headers or {}).items():
        command.extend(["-H", f"{key}: {value}"])

    if referer:
        command.extend(["-e", referer])

    if cookie_write_path:
        command.extend(["-c", str(cookie_write_path)])

    if cookie_read_path:
        command.extend(["-b", str(cookie_read_path)])

    command.append(url)

    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.DEVNULL if discard_output else asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        details = stderr.decode("utf-8", errors="ignore").strip()
        raise SearchError(
            f"{source_name}: запрос не выполнился ({details or process.returncode})"
        )

    return b"" if discard_output else stdout


def _decode_json(raw: bytes, source_name: str) -> dict:
    text = raw.decode("utf-8", errors="ignore").strip()
    if not text:
        raise SearchError(f"{source_name}: пустой ответ.")

    lowered = text.lower()
    if text.startswith("<!DOCTYPE html") or text.startswith("<html"):
        if "429 too many requests" in lowered:
            raise SearchError(f"{source_name}: слишком много запросов, ответ 429.")
        if "403 forbidden" in lowered:
            raise SearchError(f"{source_name}: доступ заблокирован, ответ 403.")
        raise SearchError(f"{source_name}: вместо JSON пришел HTML.")

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SearchError(f"{source_name}: не удалось прочитать JSON.") from exc


def _get_cached_products(source: str, query: str) -> list[dict] | None:
    cache_key = (source, query.lower())
    cached = SEARCH_CACHE.get(cache_key)
    if cached is None:
        return None

    expires_at, products = cached
    if time.monotonic() >= expires_at:
        SEARCH_CACHE.pop(cache_key, None)
        return None

    return products


def _store_cached_products(source: str, query: str, products: list[dict]) -> None:
    cache_key = (source, query.lower())
    SEARCH_CACHE[cache_key] = (time.monotonic() + CACHE_TTL_SECONDS, products)


def _to_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _to_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    return int(value)
