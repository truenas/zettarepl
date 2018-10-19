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
        for id, task in data.get("periodic-snapshot-tasks", {}).items():
            try:
                periodic_snapshot_tasks.append(PeriodicSnapshotTask.from_data(id, task))
            except ValueError as e:
                raise ValueError(f"When parsing periodic snapshot task {id}: {e!s}")

        transports = data.get("transports", {})

        replication_tasks = []
        for id, task in data.get("replication-tasks", {}).items():
            if not isinstance(task["transport"], dict):
                try:
                    task["transport"] = transports[task["transport"]]
                except KeyError:
                    raise ValueError(f"When parsing replication task {id!r}: "
                                     f"invalid transport {task['transport']!r}")

            try:
                replication_tasks.append(ReplicationTask.from_data(id, task, periodic_snapshot_tasks))
            except ValueError as e:
                raise ValueError(f"When parsing replication task {id!r}: {e!s}")

        for item, count in Counter([task.id for task in replication_tasks]).items():
            if count > 1:
                raise ValueError(f"Duplicate replication task id: {id!r}")

        return cls(periodic_snapshot_tasks + replication_tasks, timezone)
