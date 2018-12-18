# -*- coding=utf-8 -*-
from datetime import datetime, timedelta
import logging
import threading

logger = logging.getLogger(__name__)

__all__ = ["Clock"]


class Clock:
    def __init__(self, once=False):
        self.once = once

        self.ticked = False
        self.now = datetime.utcnow()

        self.interrupt_event = threading.Event()

    def tick(self):
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

    def interrupt(self):
        self.interrupt_event.set()

    def _tick(self):
        now = datetime.utcnow()

        try:
            if now < self.now:
                logger.warning("Time has stepped back (%r -> %r)", self.now, now)
                return

            if self._minutetuple(self.now) == self._minutetuple(now):
                next_minute_begin = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
                if self.interrupt_event.wait(min(10, (next_minute_begin - now).total_seconds())):
                    logger.info("Interrupted")
                    self.interrupt_event.clear()
                    try:
                        return now
                    finally:
                        now = self.now  # To resume from the same moment next time
                return

            return now
        finally:
            self.now = now

    def _minutetuple(self, d: datetime):
        return (d.year, d.month, d.day, d.hour, d.minute)
