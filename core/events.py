from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

logger = logging.getLogger("DropsMiner")


class EventBus:
    """Centralized async event bus and ring-buffer log manager for the Web UI."""

    def __init__(self, max_logs: int = 300):
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._logs: deque[dict[str, Any]] = deque(maxlen=max_logs)
        self._webhook_callback: Callable[[dict[str, Any]], Coroutine[Any, Any, None]] | None = None

    def set_webhook_callback(
        self, cb: Callable[[dict[str, Any]], Coroutine[Any, Any, None]]
    ) -> None:
        self._webhook_callback = cb

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=200)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(q)

    def get_logs(self) -> list[dict[str, Any]]:
        return list(self._logs)

    def log(
        self,
        message: str,
        *,
        platform: str = "system",
        level: str = "info",
        category: str = "general",
    ) -> None:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "platform": platform,
            "level": level,
            "category": category,
            "message": message,
        }
        self._logs.append(entry)
        prefix = f"[{platform.upper()}]"
        if level == "error":
            logger.error(f"{prefix} {message}")
        elif level == "warn":
            logger.warning(f"{prefix} {message}")
        else:
            logger.info(f"{prefix} {message}")

        self.emit("log", entry)

    def emit(self, event_type: str, data: Any) -> None:
        payload = {
            "type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }
        for q in list(self._subscribers):
            try:
                if q.full():
                    try:
                        q.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                q.put_nowait(payload)
            except Exception:
                self._subscribers.discard(q)

        if event_type == "claim" and self._webhook_callback is not None:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._webhook_callback(data))
            except RuntimeError:
                pass
