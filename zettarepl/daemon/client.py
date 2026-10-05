# -*- coding=utf-8 -*-
from __future__ import annotations

from collections.abc import Callable
import logging
import os
import queue
import signal
import threading
from typing import Any

from truenas_api_client import Client

from zettarepl.observer import ObserverMessage

logger = logging.getLogger(__name__)

__all__ = ["MiddlewareClient"]

NOTIFICATION_BATCH_SIZE = 200


class MiddlewareClient:
    def __init__(self, on_command: Callable[[dict[str, Any]], None]) -> None:
        self.on_command = on_command

        self.notifications: queue.Queue[ObserverMessage] = queue.Queue()

        self.client = Client(private_methods=True)
        self.client.subscribe("zettarepl.command", self._event_callback)

        threading.Thread(name="zr_notifier", target=self._notifier, daemon=True).start()
        threading.Thread(name="zr_watchdog", target=self._watch, daemon=True).start()

    def call(self, method: str, *params: Any, **kwargs: Any) -> Any:
        return self.client.call(method, *params, **kwargs)

    def _watch(self) -> None:
        self.client._closed.wait()
        logger.error("Lost the middleware connection, shutting down")
        os.kill(os.getpid(), signal.SIGTERM)

    def notify(self, message: ObserverMessage) -> None:
        self.notifications.put(message)

    def flush(self) -> None:
        self.notifications.join()

    def _event_callback(self, mtype: str, **message: Any) -> None:
        self.on_command(message.get("fields") or {})

    def _notifier(self) -> None:
        while True:
            batch = [self.notifications.get()]
            while len(batch) < NOTIFICATION_BATCH_SIZE:
                try:
                    batch.append(self.notifications.get_nowait())
                except queue.Empty:
                    break

            try:
                self.call("zettarepl.notify", [message.dump() for message in batch])
            except Exception:
                logger.error("Failed to send %d notifications", len(batch), exc_info=True)
            finally:
                for _ in batch:
                    self.notifications.task_done()
