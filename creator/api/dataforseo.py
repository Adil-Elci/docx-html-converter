"""Typed client for the three DataForSEO endpoints we use.

Endpoints:
- POST /v3/serp/google/organic/live/advanced       — top-10 SERP + PAA + related searches
- POST /v3/keywords_data/google_ads/search_volume/live — search volume / CPC / competition
- POST /v3/dataforseo_labs/google/related_keywords/live — semantically related keywords

All calls are pinned to Germany (location_code=2276) and German (language_code="de") by default.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("creator.dataforseo")

DEFAULT_BASE_URL = "https://api.dataforseo.com"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_LOCATION_CODE = 2276  # Germany
DEFAULT_LANGUAGE_CODE = "de"
DEFAULT_RETRIES = 2
RETRY_BACKOFF_SECONDS = 1.5

# DataForSEO success status code; anything else indicates failure.
SUCCESS_STATUS = 20000
INSUFFICIENT_FUNDS_STATUS = 40202
RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}


class DataForSEOError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        response_body: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class DataForSEOInsufficientFunds(DataForSEOError):
    pass


@dataclass(frozen=True)
class DataForSEOConfig:
    login: str
    password: str
    base_url: str = DEFAULT_BASE_URL
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    retries: int = DEFAULT_RETRIES


def load_config_from_env() -> DataForSEOConfig:
    login = os.getenv("DATAFORSEO_LOGIN", "").strip()
    password = os.getenv("DATAFORSEO_PASSWORD", "").strip()
    if not login or not password:
        raise DataForSEOError("DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD env vars must be set.")
    base_url = os.getenv("DATAFORSEO_BASE_URL", DEFAULT_BASE_URL).strip() or DEFAULT_BASE_URL
    try:
        timeout_seconds = int(os.getenv("DATAFORSEO_TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS)))
    except ValueError:
        timeout_seconds = DEFAULT_TIMEOUT_SECONDS
    try:
        retries = int(os.getenv("DATAFORSEO_RETRIES", str(DEFAULT_RETRIES)))
    except ValueError:
        retries = DEFAULT_RETRIES
    return DataForSEOConfig(
        login=login,
        password=password,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        retries=retries,
    )


@dataclass
class OrganicResult:
    rank: int
    url: str
    title: str
    description: str
    domain: str


@dataclass
class SerpResult:
    keyword: str
    organic: List[OrganicResult] = field(default_factory=list)
    people_also_ask: List[str] = field(default_factory=list)
    related_searches: List[str] = field(default_factory=list)
    cost: float = 0.0


@dataclass
class KeywordMetric:
    keyword: str
    search_volume: Optional[int] = None
    competition: Optional[float] = None
    cpc: Optional[float] = None
    cost: float = 0.0


@dataclass
class RelatedKeyword:
    keyword: str
    search_volume: Optional[int] = None
    cpc: Optional[float] = None
    competition: Optional[float] = None


@dataclass
class RelatedKeywordsResult:
    seed_keyword: str
    items: List[RelatedKeyword] = field(default_factory=list)
    cost: float = 0.0


class DataForSEOClient:
    def __init__(self, config: Optional[DataForSEOConfig] = None) -> None:
        self.config = config or load_config_from_env()

    # ---- transport ----------------------------------------------------------

    def _post(self, path: str, body: Any) -> Dict[str, Any]:
        url = f"{self.config.base_url.rstrip('/')}{path}"
        last_exc: Optional[Exception] = None
        for attempt in range(self.config.retries + 1):
            try:
                response = requests.post(
                    url,
                    json=body,
                    auth=(self.config.login, self.config.password),
                    timeout=self.config.timeout_seconds,
                    headers={"Content-Type": "application/json"},
                )
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < self.config.retries:
                    time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
                    continue
                raise DataForSEOError(f"DataForSEO request failed: {exc}") from exc

            if response.status_code in RETRYABLE_HTTP_STATUSES and attempt < self.config.retries:
                time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
                continue
            if response.status_code == 401:
                raise DataForSEOError("DataForSEO authentication failed (401). Check DATAFORSEO_LOGIN / DATAFORSEO_PASSWORD.")
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                raise DataForSEOError(f"DataForSEO HTTP error {response.status_code}: {exc}") from exc
            try:
                return response.json()
            except ValueError as exc:
                raise DataForSEOError(f"DataForSEO returned non-JSON body: {exc}") from exc
        raise DataForSEOError(f"DataForSEO request exhausted retries: {last_exc}")

    @staticmethod
    def _check_response(payload: Dict[str, Any]) -> Dict[str, Any]:
        top_status = int(payload.get("status_code") or 0)
        if top_status == INSUFFICIENT_FUNDS_STATUS:
            raise DataForSEOInsufficientFunds(
                f"DataForSEO insufficient funds: {payload.get('status_message')}",
                status_code=top_status,
                response_body=payload,
            )
        if top_status != SUCCESS_STATUS:
            raise DataForSEOError(
                f"DataForSEO returned status {top_status}: {payload.get('status_message')}",
                status_code=top_status,
                response_body=payload,
            )
        tasks = payload.get("tasks") or []
        if not tasks:
            raise DataForSEOError("DataForSEO returned no tasks.", response_body=payload)
        task = tasks[0]
        task_status = int(task.get("status_code") or 0)
        if task_status == INSUFFICIENT_FUNDS_STATUS:
            raise DataForSEOInsufficientFunds(
                f"DataForSEO task insufficient funds: {task.get('status_message')}",
                status_code=task_status,
                response_body=payload,
            )
        if task_status != SUCCESS_STATUS:
            raise DataForSEOError(
                f"DataForSEO task failed with status {task_status}: {task.get('status_message')}",
                status_code=task_status,
                response_body=payload,
            )
        return task

    # ---- endpoint: SERP organic --------------------------------------------

    def serp_organic(
        self,
        keyword: str,
        *,
        location_code: int = DEFAULT_LOCATION_CODE,
        language_code: str = DEFAULT_LANGUAGE_CODE,
        depth: int = 10,
    ) -> SerpResult:
        body = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": "desktop",
            "depth": depth,
        }]
        payload = self._post("/v3/serp/google/organic/live/advanced", body)
        task = self._check_response(payload)
        results = task.get("result") or []
        if not results:
            return SerpResult(keyword=keyword, cost=float(payload.get("cost") or 0.0))
        items = results[0].get("items") or []

        organic: List[OrganicResult] = []
        paa: List[str] = []
        related: List[str] = []

        for item in items:
            item_type = item.get("type")
            if item_type == "organic":
                organic.append(OrganicResult(
                    rank=int(item.get("rank_absolute") or item.get("rank_group") or len(organic) + 1),
                    url=str(item.get("url") or "").strip(),
                    title=str(item.get("title") or "").strip(),
                    description=str(item.get("description") or "").strip(),
                    domain=str(item.get("domain") or "").strip(),
                ))
            elif item_type == "people_also_ask":
                for inner in item.get("items") or []:
                    title = str(inner.get("title") or "").strip()
                    if title:
                        paa.append(title)
            elif item_type == "related_searches":
                for related_item in item.get("items") or []:
                    text = str(related_item).strip() if not isinstance(related_item, dict) else str(related_item.get("title") or "").strip()
                    if text:
                        related.append(text)

        organic = sorted(organic, key=lambda r: r.rank)[:depth]
        return SerpResult(
            keyword=keyword,
            organic=organic,
            people_also_ask=paa,
            related_searches=related,
            cost=float(payload.get("cost") or 0.0),
        )

    # ---- endpoint: keyword volume ------------------------------------------

    def keyword_volume(
        self,
        keywords: List[str],
        *,
        location_code: int = DEFAULT_LOCATION_CODE,
        language_code: str = DEFAULT_LANGUAGE_CODE,
    ) -> List[KeywordMetric]:
        cleaned = [k.strip() for k in keywords if k and k.strip()]
        if not cleaned:
            return []
        body = [{
            "keywords": cleaned,
            "location_code": location_code,
            "language_code": language_code,
        }]
        payload = self._post("/v3/keywords_data/google_ads/search_volume/live", body)
        task = self._check_response(payload)
        items = task.get("result") or []
        cost = float(payload.get("cost") or 0.0)
        metrics: List[KeywordMetric] = []
        for item in items:
            metrics.append(KeywordMetric(
                keyword=str(item.get("keyword") or "").strip(),
                search_volume=item.get("search_volume"),
                competition=item.get("competition"),
                cpc=item.get("cpc"),
                cost=cost,
            ))
        return metrics

    # ---- endpoint: related keywords ----------------------------------------

    def related_keywords(
        self,
        keyword: str,
        *,
        location_code: int = DEFAULT_LOCATION_CODE,
        language_code: str = DEFAULT_LANGUAGE_CODE,
        limit: int = 50,
    ) -> RelatedKeywordsResult:
        body = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "limit": limit,
        }]
        payload = self._post("/v3/dataforseo_labs/google/related_keywords/live", body)
        task = self._check_response(payload)
        results = task.get("result") or []
        if not results:
            return RelatedKeywordsResult(seed_keyword=keyword, cost=float(payload.get("cost") or 0.0))
        items = results[0].get("items") or []
        related: List[RelatedKeyword] = []
        for item in items:
            keyword_data = item.get("keyword_data") if isinstance(item.get("keyword_data"), dict) else {}
            keyword_info = keyword_data.get("keyword_info") if isinstance(keyword_data.get("keyword_info"), dict) else {}
            kw_text = str(keyword_data.get("keyword") or item.get("keyword") or "").strip()
            if not kw_text:
                continue
            related.append(RelatedKeyword(
                keyword=kw_text,
                search_volume=keyword_info.get("search_volume"),
                cpc=keyword_info.get("cpc"),
                competition=keyword_info.get("competition"),
            ))
        return RelatedKeywordsResult(
            seed_keyword=keyword,
            items=related,
            cost=float(payload.get("cost") or 0.0),
        )
