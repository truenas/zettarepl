# -*- coding=utf-8 -*-
from collections import namedtuple
import logging

import pytz

logger = logging.getLogger(__name__)

__all__ = ["TzClockDateTime", "TzClock"]

TzClockDateTime = namedtuple("TzClockDateTime", ["datetime", "offset_aware_datetime", "utc_datetime",
                                                 "legit_step_back"])


class TzClock:
    def __init__(self, timezone, utcnow):
        self.timezone = timezone

        self.utcnow = utcnow
        self.now = self._calculate_now(self.utcnow)
        self.now_naive = self.now.replace(tzinfo=None)

    def tick(self, utcnow):
        now = self._calculate_now(utcnow)
        now_naive = now.replace(tzinfo=None)
        try:
            if now_naive < self.now_naive and not (utcnow < self.utcnow):
                return TzClockDateTime(
                    now_naive,
                    now,
                    utcnow,
                    (self.now_naive - now_naive) + (utcnow - self.utcnow),
                )

            return TzClockDateTime(now_naive, now, utcnow, None)
        finally:
            self.utcnow = utcnow
            self.now = now
            self.now_naive = now_naive

    def _calculate_now(self, utcnow):
        return utcnow.replace(tzinfo=pytz.UTC).astimezone(self.timezone)
