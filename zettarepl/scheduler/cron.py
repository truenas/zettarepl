# -*- coding=utf-8 -*-
from datetime import datetime, timedelta
import logging

from croniter import croniter

from zettarepl.definition.schema import schedule_validator

logger = logging.getLogger(__name__)

__all__ = ["CronSchedule"]


class CronSchedule:
    def __init__(self, minute, hour, day_of_month, month, day_of_week):
        self.expr_format = " ".join([str(minute), str(hour), str(day_of_month), str(month), str(day_of_week)])

    @classmethod
    def from_data(cls, data):
        schedule_validator.validate(data)

        return cls(data["minute"], data["hour"], data["day-of-month"], data["month"], data["day-of-week"])

    def should_run(self, d: datetime):
        idealized = d.replace(second=0, microsecond=0, tzinfo=None)
        return croniter(self.expr_format, idealized - timedelta(seconds=1)).get_next(datetime) == idealized
