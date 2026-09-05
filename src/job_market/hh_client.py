from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import Any

import httpx

from .config import settings

logger = logging.getLogger(__name__)


class HeadHunterClient:
    """Small client for the documented public HeadHunter vacancies API."""

    base_url = "https://api.hh.ru"

    def __init__(
        self,
        *,
        user_agent: str = settings.user_agent,
        request_delay: float = settings.request_delay_seconds,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.request_delay = request_delay
        self._last_request_at = 0.0
        self.client = httpx.Client(
            base_url=self.base_url,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            timeout=20,
            transport=transport,
        )

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> HeadHunterClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def search(self, text: str, area: int, pages: int = 1) -> Iterator[dict[str, Any]]:
        for page in range(pages):
            payload = self._get(
                "/vacancies",
                params={"text": text, "area": area, "page": page, "per_page": 100},
            )
            yield from payload.get("items", [])
            if page >= int(payload.get("pages", 1)) - 1:
                break

    def vacancy(self, vacancy_id: str) -> dict[str, Any]:
        return self._get(f"/vacancies/{vacancy_id}")

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        for attempt in range(3):
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.request_delay:
                time.sleep(self.request_delay - elapsed)
            response = self.client.get(path, params=params)
            self._last_request_at = time.monotonic()
            if response.status_code == 429 or response.status_code >= 500:
                if attempt == 2:
                    response.raise_for_status()
                delay = float(response.headers.get("Retry-After", 2**attempt))
                logger.warning("HH API retry in %.1fs (status=%s)", delay, response.status_code)
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError("unreachable")
