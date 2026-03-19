# -*- coding=utf-8 -*-
from collections import namedtuple
from datetime import datetime, tzinfo
import logging

import pytz

logger = logging.getLogger(__name__)

__all__ = ["TzClockDateTime", "TzClock"]

TzClockDateTime = namedtuple("TzClockDateTime", ["datetime", "offset_aware_datetime", "utc_datetime",
                                                 "legit_step_back"])


class TzClock:
    def __init__(self, timezone: tzinfo, utcnow: datetime) -> None:
        self.timezone: tzinfo = timezone

        self.utcnow: datetime = utcnow
        self.now: datetime = self._calculate_now(self.utcnow)
        self.now_naive: datetime = self.now.replace(tzinfo=None)

    def tick(self, utcnow: datetime) -> TzClockDateTime:
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

    def _calculate_now(self, utcnow: datetime) -> datetime:
        return utcnow.replace(tzinfo=pytz.UTC).astimezone(self.timezone)
