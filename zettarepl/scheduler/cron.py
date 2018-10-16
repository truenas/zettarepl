# -*- coding=utf-8 -*-
from datetime import datetime, timedelta
import logging

import isodate
from croniter import croniter

from zettarepl.definition.schema import schedule_validator

logger = logging.getLogger(__name__)

__all__ = ["CronSchedule"]


class CronSchedule:
    def __init__(self, minute, hour, day_of_month, month, day_of_week, start, end):
        self.expr_format = " ".join([str(minute), str(hour), str(day_of_month), str(month), str(day_of_week)])
        self.start = start
        self.end = end

    @classmethod
    def from_data(cls, data):
        schedule_validator.validate(data)

        data.setdefault("minute", "*")
        data.setdefault("hour", "*")
        data.setdefault("day-of-month", "*")
        data.setdefault("month", "*")
        data.setdefault("day-of-week", "*")
        data.setdefault("start", "00:00")
        data.setdefault("end", "23:59")

        return cls(data["minute"], data["hour"], data["day-of-month"], data["month"], data["day-of-week"],
                   isodate.parse_time(data["start"]), isodate.parse_time(data["end"]))

    def should_run(self, d: datetime):
        idealized = d.replace(second=0, microsecond=0, tzinfo=None)
        if not (self.start <= idealized.time() <= self.end):
            return False
        return croniter(self.expr_format, idealized - timedelta(seconds=1)).get_next(datetime) == idealized
