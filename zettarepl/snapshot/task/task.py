# -*- coding=utf-8 -*-
from datetime import timedelta
import logging

import isodate

from zettarepl.definition.schema import periodic_snapshot_task_validator
from zettarepl.scheduler.cron import CronSchedule

from .naming_schema import validate_snapshot_naming_schema

logger = logging.getLogger(__name__)

__all__ = ["PeriodicSnapshotTask"]


class PeriodicSnapshotTask:
    def __init__(self, id, dataset: str, recursive: bool, exclude: [str], lifetime: timedelta,
                 naming_schema: str, schedule: CronSchedule, allow_empty: bool):
        self.id = id
        self.dataset = dataset
        self.recursive = recursive
        self.exclude = exclude
        self.lifetime = lifetime
        self.naming_schema = naming_schema
        self.schedule = schedule
        self.allow_empty = allow_empty

        validate_snapshot_naming_schema(self.naming_schema)

    @classmethod
    def from_data(cls, data):
        periodic_snapshot_task_validator.validate(data)

        data.setdefault("exclude", [])
        data.setdefault("allow-empty", True)

        if "lifetime" in data:
            lifetime = isodate.parse_duration(data["lifetime"])
        else:
            # timedelta.max is not good here because operations with it would result in
            # OverflowError: date value out of range
            lifetime = timedelta(days=36500)

        return cls(
            data["id"], data["dataset"], data["recursive"], data["exclude"], lifetime,
            data["naming-schema"], CronSchedule.from_data(data["schedule"]), data["allow-empty"])
