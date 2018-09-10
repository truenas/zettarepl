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
                 naming_schema: str, schedule: CronSchedule):
        self.id = id
        self.dataset = dataset
        self.recursive = recursive
        self.exclude = exclude
        self.lifetime = lifetime
        self.naming_schema = naming_schema
        self.schedule = schedule

        validate_snapshot_naming_schema(self.naming_schema)

    @classmethod
    def from_data(cls, data):
        periodic_snapshot_task_validator.validate(data)

        data.setdefault("exclude", [])

        return cls(
            data["id"], data["dataset"], data["recursive"], data["exclude"], isodate.parse_duration(data["lifetime"]),
            data["naming-schema"], CronSchedule.from_data(data["schedule"]))
