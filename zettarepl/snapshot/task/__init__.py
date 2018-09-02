# -*- coding=utf-8 -*-
from datetime import timedelta
import logging

import isodate

from zettarepl.scheduler.cron import CronSchedule
from zettarepl.schema import periodic_snapshot_task_validator

from .naming_schema import validate_snapshot_naming_schema

logger = logging.getLogger(__name__)

__all__ = ["PeriodicSnapshotTask"]


class PeriodicSnapshotTask:
    def __init__(self, dataset: str, recursive: bool, recursive_exclude: [str], lifetime: timedelta, naming_schema: str,
                 schedule: CronSchedule):
        self.dataset = dataset
        self.recursive = recursive
        self.recursive_exclude = recursive_exclude
        self.lifetime = lifetime
        self.naming_schema = naming_schema
        self.schedule = schedule

        validate_snapshot_naming_schema(self.naming_schema)

    @classmethod
    def from_data(cls, data):
        periodic_snapshot_task_validator.validate(data)

        data.setdefault("exclude", [])

        return cls(
            data["dataset"], data["recursive"], data["exclude"], isodate.parse_duration(data["lifetime"]),
            data["namingSchema"], CronSchedule.from_data(data["schedule"]))
