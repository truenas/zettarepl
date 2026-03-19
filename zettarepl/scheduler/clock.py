# -*- coding=utf-8 -*-
from datetime import datetime, timedelta
import logging
import threading

logger = logging.getLogger(__name__)

__all__ = ["Clock"]


class Clock:
    def __init__(self, once: bool = False) -> None:
        self.once = once

        self.ticked: bool = False
        self.now: datetime = datetime.utcnow()

        self.interrupt_event: threading.Event = threading.Event()

    def tick(self) -> datetime | None:
        if self.once:
            if self.ticked:
                return None
            else:
                self.ticked = True
                return self.now

        while True:
            now = self._tick()
            if now is not None:
                return now

    def interrupt(self) -> None:
        self.interrupt_event.set()

    def _tick(self) -> datetime | None:
        now = datetime.utcnow()

        try:
            if now < self.now:
                logger.warning("Time has stepped back (%r -> %r)", self.now, now)
                return None

            if self._minutetuple(self.now) == self._minutetuple(now):
                next_minute_begin = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
                if self.interrupt_event.wait(min(10, (next_minute_begin - now).total_seconds())):
                    logger.info("Interrupted")
                    self.interrupt_event.clear()
                    try:
                        return now
                    finally:
                        now = self.now  # To resume from the same moment next time

                return None

            return now
        finally:
            self.now = now

    def _minutetuple(self, d: datetime) -> tuple[int, int, int, int, int]:
        return (d.year, d.month, d.day, d.hour, d.minute)
