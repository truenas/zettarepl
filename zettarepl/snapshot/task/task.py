# -*- coding=utf-8 -*-
from datetime import timedelta
import logging
from typing import Any, Self

import isodate

from zettarepl.definition.schema import periodic_snapshot_task_validator
from zettarepl.scheduler.cron import CronSchedule
from zettarepl.snapshot.name import validate_snapshot_naming_schema
from zettarepl.task import Task

logger = logging.getLogger(__name__)

__all__ = ["PeriodicSnapshotTask"]


class PeriodicSnapshotTask(Task):
    def __init__(self, id: str, dataset: str, recursive: bool, exclude: list[str], lifetime: timedelta,
                 naming_schema: str, schedule: CronSchedule, allow_empty: bool) -> None:
        self.id = id
        self.dataset = dataset
        self.recursive = recursive
        self.exclude = exclude
        self.lifetime = lifetime
        self.naming_schema = naming_schema
        self.schedule = schedule
        self.allow_empty = allow_empty

        validate_snapshot_naming_schema(self.naming_schema)

    def __repr__(self) -> str:
        return f"<Periodic Snapshot Task {self.id!r}>"

    @classmethod
    def from_data(cls, id: str, data: dict[str, Any]) -> Self:
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
            id, data["dataset"], data["recursive"], data["exclude"], lifetime,
            data["naming-schema"], CronSchedule.from_data(data["schedule"]), data["allow-empty"])
