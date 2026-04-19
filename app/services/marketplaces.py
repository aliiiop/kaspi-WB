from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import quote, urlencode

import httpx

from app.schemas import SearchItem, SourceStatus

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}

DEFAULT_WB_DEST = os.getenv("WB_DEST", "-1257786")
DEFAULT_KASPI_CITY = os.getenv("KASPI_CITY_CODE", "750000000")
CACHE_TTL_SECONDS = 120
_PAYLOAD_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}
_LAST_REQUEST_AT: dict[str, float] = {}
WB_PAGE_SIZE = 100
KASPI_PAGE_SIZE = 12
MAX_PROVIDER_PAGES = 10
REQUEST_RETRIES = 3
WB_MIN_INTERVAL_SECONDS = 1.25


class ProviderError(RuntimeError):
    pass


@dataclass(slots=True)
class ProviderResult:
    items: list[SearchItem]
    status: SourceStatus


class WildberriesProvider:
    source = "wildberries"
    search_urls = (
        "https://search.wb.ru/exactmatch/ru/common/v9/search",
        "https://search.wb.ru/exactmatch/ru/common/v4/search",
    )

    async def search(self, query: str, limit: int) -> ProviderResult:
        products, total = await self._fetch_products(query, limit)
        items = [self._normalize_product(product) for product in products[:limit]]

        return ProviderResult(
            items=items,
            status=SourceStatus(
                source=self.source,
                ok=True,
                message=f"Loaded cards: {len(items)} of {total}",
            ),
        )

    async def _fetch_products(self, query: str, limit: int) -> tuple[list[dict], int]:
        products: list[dict] = []
        seen_ids: set[int] = set()
        total = 0
        pages_to_fetch = max(1, min(MAX_PROVIDER_PAGES, (limit + WB_PAGE_SIZE - 1) // WB_PAGE_SIZE))

        for page in range(1, pages_to_fetch + 1):
            payload = await self._fetch_payload(query, page)
            total = max(total, int(payload.get("total") or 0))
            batch = payload.get("products", [])
            if not batch:
                break

            for product in batch:
                product_id = product.get("id")
                if product_id in seen_ids:
                    continue
                seen_ids.add(product_id)
                products.append(product)
                if len(products) >= limit:
                    return products, total or len(products)

            if len(batch) < WB_PAGE_SIZE:
                break

        return products, total or len(products)

    async def _fetch_payload(self, query: str, page: int) -> dict:
        cached = _get_cached_payload(f"{self.source}:{page}", query)
        if cached is not None:
            return cached

        params = {
            "appType": "1",
            "curr": "rub",
            "dest": DEFAULT_WB_DEST,
            "page": str(page),
            "query": query,
            "resultset": "catalog",
            "sort": "popular",
            "spp": "30",
            "suppressSpellcheck": "false",
        }

        last_error: ProviderError | None = None
        for search_url in self.search_urls:
            url = f"{search_url}?{urlencode(params)}"
            try:
                stdout = await _curl_json_request(
                    url,
                    source_name="Wildberries",
                    extra_args=["-H", "Accept: application/json, text/plain, */*"],
                    request_key=self.source,
                    min_interval_seconds=WB_MIN_INTERVAL_SECONDS,
                )
                payload = _decode_json_response(stdout, "Wildberries")
                if not payload.get("products"):
                    raise ProviderError(
                        "Wildberries returned a search payload without product cards."
                    )
                _store_cached_payload(f"{self.source}:{page}", query, payload)
                return payload
            except ProviderError as exc:
                last_error = exc

        raise last_error or ProviderError("Wildberries request failed.")

    def _normalize_product(self, product: dict) -> SearchItem:
        price_block = self._pick_price_block(product)
        price = None
        if price_block is not None:
            raw_price = price_block.get("product") or price_block.get("basic")
            if raw_price is not None:
                price = round(raw_price / 100, 2)

        time1 = product.get("time1")
        time2 = product.get("time2")
        delivery = None
        if time1 is not None and time2 is not None:
            delivery = f"{time1}-{time2} d."

        product_id = product.get("id")
        return SearchItem(
            source=self.source,
            title=product.get("name") or "Untitled",
            brand=product.get("brand"),
            price=price,
            currency="RUB",
            rating=self._to_float(product.get("rating") or product.get("reviewRating")),
            review_count=self._to_int(product.get("feedbacks")),
            seller=product.get("supplier"),
            delivery=delivery,
            product_url=f"https://www.wildberries.ru/catalog/{product_id}/detail.aspx",
            image_url=None,
            metadata={
                "id": product_id,
                "subject": product.get("entity"),
            },
        )

    @staticmethod
    def _pick_price_block(product: dict) -> dict | None:
        sizes = product.get("sizes") or []
        if not sizes:
            return None
        return sizes[0].get("price")

    @staticmethod
    def _to_float(value: object) -> float | None:
        if value in (None, ""):
            return None
        return float(value)

    @staticmethod
    def _to_int(value: object) -> int | None:
        if value in (None, ""):
            return None
        return int(value)


class KaspiProvider:
    source = "kaspi"
    init_url = "https://kaspi.kz/shop/search/"
    results_url = "https://kaspi.kz/yml/product-view/pl/results"

    async def search(self, query: str, limit: int) -> ProviderResult:
        products = await self._fetch_products(query, limit)
        items = [self._normalize_product(product) for product in products[:limit]]

        return ProviderResult(
            items=items,
            status=SourceStatus(
                source=self.source,
                ok=True,
                message=f"Loaded cards: {len(items)}",
            ),
        )

    async def _fetch_products(self, query: str, limit: int) -> list[dict]:
        products: list[dict] = []
        seen_ids: set[str] = set()
        pages_to_fetch = max(1, min(MAX_PROVIDER_PAGES, (limit + KASPI_PAGE_SIZE - 1) // KASPI_PAGE_SIZE))

        for page in range(pages_to_fetch):
            payload = await self._fetch_payload(query, page)
            batch = payload.get("data", [])
            if not batch:
                break

            for product in batch:
                product_id = str(product.get("id") or "")
                if product_id in seen_ids:
                    continue
                seen_ids.add(product_id)
                products.append(product)
                if len(products) >= limit:
                    return products

            if len(batch) < KASPI_PAGE_SIZE:
                break

        return products

    async def _fetch_payload(self, query: str, page: int = 0) -> dict:
        cached = _get_cached_payload(f"{self.source}:{page}", query)
        if cached is not None:
            return cached

        referer = f"https://kaspi.kz/shop/search/?text={quote(query)}"
        init_url = f"{self.init_url}?{urlencode({'text': query})}"
        results_params = {"text": query}
        if page > 0:
            results_params["page"] = page
        results_url = f"{self.results_url}?{urlencode(results_params)}"
        last_error: ProviderError | None = None

        for attempt in range(1, REQUEST_RETRIES + 1):
            try:
                async with httpx.AsyncClient(
                    headers=DEFAULT_HEADERS,
                    follow_redirects=True,
                    timeout=20.0,
                ) as client:
                    await client.get(init_url)
                    response = await client.get(
                        results_url,
                        headers={
                            "Accept": "application/json, text/plain, */*",
                            "X-Requested-With": "XMLHttpRequest",
                            "Referer": referer,
                        },
                    )
                    payload = _decode_json_response(response.content, "Kaspi")
                    _store_cached_payload(f"{self.source}:{page}", query, payload)
                    return payload
            except (httpx.HTTPError, ProviderError) as exc:
                last_error = ProviderError(str(exc))
                if attempt < REQUEST_RETRIES:
                    await asyncio.sleep(0.8 * attempt)

        raise last_error or ProviderError("Kaspi request failed.")

    def _normalize_product(self, product: dict) -> SearchItem:
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
        product_url = f"https://kaspi.kz{shop_link}"

        return SearchItem(
            source=self.source,
            title=product.get("title") or "Untitled",
            brand=product.get("brand"),
            price=self._to_float(product.get("unitSalePrice") or product.get("unitPrice")),
            currency=product.get("currency") or "KZT",
            rating=self._to_float(product.get("rating")),
            review_count=self._to_int(product.get("reviewsQuantity")),
            seller=product.get("bestMerchant"),
            delivery=delivery,
            product_url=product_url,
            image_url=image_url,
            metadata={
                "id": product.get("id"),
                "city_code": DEFAULT_KASPI_CITY,
            },
        )

    @staticmethod
    def _to_float(value: object) -> float | None:
        if value in (None, ""):
            return None
        return float(value)

    @staticmethod
    def _to_int(value: object) -> int | None:
        if value in (None, ""):
            return None
        return int(value)


async def unified_search(
    query: str,
    per_source_limit: int = 12,
) -> tuple[list[SearchItem], list[SourceStatus]]:
    providers = [WildberriesProvider(), KaspiProvider()]
    tasks = [provider.search(query, per_source_limit) for provider in providers]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    items: list[SearchItem] = []
    statuses: list[SourceStatus] = []

    for provider, result in zip(providers, results, strict=True):
        if isinstance(result, Exception):
            statuses.append(
                SourceStatus(
                    source=provider.source,
                    ok=False,
                    message=str(result) or result.__class__.__name__,
                )
            )
            continue

        items.extend(result.items)
        statuses.append(result.status)

    return items, statuses


def group_items_by_source(items: Iterable[SearchItem]) -> dict[str, list[SearchItem]]:
    grouped = {"wildberries": [], "kaspi": []}
    for item in items:
        grouped.setdefault(item.source, []).append(item)
    return grouped


def _decode_json_response(stdout: bytes, source_name: str) -> dict:
    text = stdout.decode("utf-8", errors="ignore").strip()
    if not text:
        raise ProviderError(f"{source_name} returned an empty response.")

    lowered = text.lower()
    if text.startswith("<!DOCTYPE html") or text.startswith("<html"):
        if "429 too many requests" in lowered:
            raise ProviderError(f"{source_name} rate-limited the request (429).")
        if "403 forbidden" in lowered:
            raise ProviderError(f"{source_name} blocked the request (403).")
        raise ProviderError(f"{source_name} returned HTML instead of JSON.")

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ProviderError(f"{source_name} returned invalid JSON.") from exc


def _get_cached_payload(source: str, query: str) -> dict | None:
    key = (source, query.lower())
    entry = _PAYLOAD_CACHE.get(key)
    if entry is None:
        return None

    expires_at, payload = entry
    if time.monotonic() >= expires_at:
        _PAYLOAD_CACHE.pop(key, None)
        return None

    return payload


def _store_cached_payload(source: str, query: str, payload: dict) -> None:
    key = (source, query.lower())
    _PAYLOAD_CACHE[key] = (time.monotonic() + CACHE_TTL_SECONDS, payload)


async def _curl_json_request(
    url: str,
    *,
    source_name: str,
    extra_args: list[str] | None = None,
    expect_json: bool = True,
    discard_output: bool = False,
    request_key: str | None = None,
    min_interval_seconds: float = 0.0,
) -> bytes:
    extra_args = extra_args or []
    last_error: ProviderError | None = None
    curl_bin = _get_curl_binary()

    for attempt in range(1, REQUEST_RETRIES + 1):
        if request_key and min_interval_seconds > 0:
            await _respect_min_interval(request_key, min_interval_seconds)

        try:
            stdout, stderr, returncode = await asyncio.to_thread(
                _run_curl_request,
                curl_bin,
                url,
                extra_args,
                discard_output,
            )
            if returncode != 0:
                details = stderr.decode("utf-8", errors="ignore").strip()
                last_error = ProviderError(
                    f"{source_name} curl request failed: {details or returncode}"
                )
            elif expect_json:
                try:
                    _decode_json_response(stdout, source_name)
                    return stdout
                except ProviderError as exc:
                    last_error = exc
            else:
                return stdout or b""
        except OSError as exc:
            last_error = ProviderError(f"{source_name} curl request failed: {exc}")

        if attempt < REQUEST_RETRIES:
            await asyncio.sleep(0.8 * attempt)

    raise last_error or ProviderError(f"{source_name} request failed.")


async def _respect_min_interval(request_key: str, min_interval_seconds: float) -> None:
    now = time.monotonic()
    last_request_at = _LAST_REQUEST_AT.get(request_key)
    if last_request_at is not None:
        wait_seconds = min_interval_seconds - (now - last_request_at)
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
    _LAST_REQUEST_AT[request_key] = time.monotonic()


def _parse_http_args(extra_args: list[str]) -> tuple[dict[str, str], str | None, dict[str, str]]:
    headers: dict[str, str] = {}
    cookies: dict[str, str] = {}
    referer: str | None = None
    i = 0
    while i < len(extra_args):
        arg = extra_args[i]
        if arg == "-H" and i + 1 < len(extra_args):
            raw_header = extra_args[i + 1]
            if ":" in raw_header:
                name, value = raw_header.split(":", 1)
                headers[name.strip()] = value.strip()
            i += 2
            continue
        if arg == "-e" and i + 1 < len(extra_args):
            referer = extra_args[i + 1]
            headers["Referer"] = referer
            i += 2
            continue
        i += 1
    return headers, referer, cookies


def _get_curl_binary() -> str:
    curl_bin = shutil.which("curl") or shutil.which("curl.exe")
    if not curl_bin:
        raise ProviderError("curl was not found in PATH.")
    return curl_bin


def _run_curl_request(
    curl_bin: str,
    url: str,
    extra_args: list[str],
    discard_output: bool,
) -> tuple[bytes, bytes, int]:
    command = [
        curl_bin,
        "-L",
        "--silent",
        "--show-error",
        "--compressed",
        "--max-time",
        "20",
        "-A",
        DEFAULT_HEADERS["User-Agent"],
        *extra_args,
        url,
    ]
    completed = subprocess.run(
        command,
        capture_output=not discard_output,
        check=False,
    )
    stdout = b"" if discard_output else completed.stdout
    stderr = completed.stderr or b""
    return stdout, stderr, completed.returncode
