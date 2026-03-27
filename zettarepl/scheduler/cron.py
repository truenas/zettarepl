# -*- coding=utf-8 -*-
from datetime import datetime, time, timedelta
import logging
from typing import Any, Self

import isodate
from croniter import croniter

from zettarepl.definition.schema import schedule_validator
from zettarepl.utils.datetime import idealized_datetime

logger = logging.getLogger(__name__)

__all__ = ["CronSchedule"]


class CronSchedule:
    def __init__(self, minute: str, hour: str, day_of_month: str, month: str, day_of_week: str,
                 begin: time, end: time) -> None:
        self.expr_format: str = " ".join([str(minute), str(hour), str(day_of_month), str(month), str(day_of_week)])
        self.begin: time = begin
        self.end: time = end

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> Self:
        schedule_validator.validate(data)

        data.setdefault("minute", "*")
        data.setdefault("hour", "*")
        data.setdefault("day-of-month", "*")
        data.setdefault("month", "*")
        data.setdefault("day-of-week", "*")
        data.setdefault("begin", "00:00")
        data.setdefault("end", "23:59")

        return cls(data["minute"], data["hour"], data["day-of-month"], data["month"], data["day-of-week"],
                   isodate.parse_time(data["begin"]), isodate.parse_time(data["end"]))

    def should_run(self, d: datetime) -> bool:
        idealized = idealized_datetime(d)
        if self.begin < self.end:
            if not (self.begin <= idealized.time() <= self.end):
                return False
        else:
            if not (idealized.time() >= self.begin or idealized.time() <= self.end):
                return False

        next_datetime = croniter(self.expr_format, idealized - timedelta(seconds=1)).get_next(datetime)
        return next_datetime == idealized  # type: ignore
