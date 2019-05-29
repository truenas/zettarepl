# -*- coding=utf-8 -*-
import copy
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
    def validate(cls, data):
        schema_validator.validate(data)

    @classmethod
    def from_data(cls, data):
        data = copy.deepcopy(data)

        cls.validate(data)

        if "timezone" in data:
            try:
                timezone = pytz.timezone(data["timezone"])
            except pytz.exceptions.UnknownTimeZoneError:
                raise ValueError("Unknown timezone: {data['timezone']!r}") from None
        else:
            timezone = tzlocal()

        periodic_snapshot_tasks = []
        for id, task in data.get("periodic-snapshot-tasks", {}).items():
            try:
                periodic_snapshot_tasks.append(PeriodicSnapshotTask.from_data(id, task))
            except ValueError as e:
                raise ValueError(f"When parsing periodic snapshot task {id}: {e!s}") from None

        transports = data.get("transports", {})

        replication_tasks = []
        for id, task in data.get("replication-tasks", {}).items():
            if not isinstance(task["transport"], dict):
                try:
                    task["transport"] = transports[task["transport"]]
                except KeyError:
                    raise ValueError(
                        f"When parsing replication task {id!r}: invalid transport {task['transport']!r}"
                    ) from None

            try:
                replication_tasks.append(ReplicationTask.from_data(id, task, periodic_snapshot_tasks))
            except ValueError as e:
                raise ValueError(f"When parsing replication task {id!r}: {e!s}") from None

        return cls(periodic_snapshot_tasks + replication_tasks, timezone)
