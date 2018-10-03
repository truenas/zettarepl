# -*- coding=utf-8 -*-
from collections import Counter
import logging

from dateutil.tz import tzlocal
import pytz
import pytz.exceptions

from zettarepl.replication.task.task import ReplicationTask
from zettarepl.snapshot.task.task import PeriodicSnapshotTask

from .schema import schema_validator

logger = logging.getLogger(__name__)

__all__ = ["Definition"]


class Definition:
    def __init__(self, tasks, timezone):
        self.tasks = tasks
        self.timezone = timezone

    @classmethod
    def from_data(cls, data):
        schema_validator.validate(data)

        if "timezone" in data:
            try:
                timezone = pytz.timezone(data["timezone"])
            except pytz.exceptions.UnknownTimeZoneError:
                raise ValueError("Unknown timezone: {data['timezone']!r}")
        else:
            timezone = tzlocal()

        periodic_snapshot_tasks = []
        for task in data.get("periodic-snapshot-tasks", []):
            try:
                periodic_snapshot_tasks.append(PeriodicSnapshotTask.from_data(task))
            except ValueError as e:
                raise ValueError(f"When parsing periodic snapshot task {task['id']!r}: {e!s}")

        for item, count in Counter([task.id for task in periodic_snapshot_tasks]).items():
            if count > 1:
                raise ValueError(f"Duplicate periodic snapshot task id: {task['id']!r}")

        replication_tasks = []
        for task in data.get("replication-tasks", []):
            try:
                replication_tasks.append(ReplicationTask.from_data(task, periodic_snapshot_tasks))
            except ValueError as e:
                raise ValueError(f"When parsing replication task {task['id']!r}: {e!s}")

        for item, count in Counter([task.id for task in replication_tasks]).items():
            if count > 1:
                raise ValueError(f"Duplicate replication task id: {task['id']!r}")

        return cls(periodic_snapshot_tasks + replication_tasks, timezone)
