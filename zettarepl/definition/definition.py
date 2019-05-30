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

__all__ = ["DefinitionErrors", "DefinitionError", "PeriodicSnapshotTaskDefinitionError",
           "ReplicationTaskDefinitionError", "Definition"]


class DefinitionErrors(ValueError):
    def __init__(self, errors):
        self.errors = errors

    def __str__(self):
        return "\n".join([str(e) for e in self.errors])


class DefinitionError(ValueError):
    pass


class PeriodicSnapshotTaskDefinitionError(DefinitionError):
    def __init__(self, task_id, error):
        self.task_id = task_id
        self.error = error

    def __str__(self):
        return f"When parsing periodic snapshot task {self.task_id}: {self.error!s}"


class ReplicationTaskDefinitionError(DefinitionError):
    def __init__(self, task_id, error):
        self.task_id = task_id
        self.error = error

    def __str__(self):
        return f"When parsing replication task {self.task_id}: {self.error!s}"


class Definition:
    def __init__(self, tasks, timezone, errors):
        self.tasks = tasks
        self.timezone = timezone

        self.errors = errors

    @classmethod
    def validate(cls, data):
        schema_validator.validate(data)

    @classmethod
    def from_data(cls, data, raise_on_error=True):
        data = copy.deepcopy(data)

        cls.validate(data)

        errors = []

        timezone = tzlocal()
        if "timezone" in data:
            try:
                timezone = pytz.timezone(data["timezone"])
            except pytz.exceptions.UnknownTimeZoneError:
                errors.append(DefinitionError("Unknown timezone: {data['timezone']!r}"))

        periodic_snapshot_tasks = []
        for id, task in data.get("periodic-snapshot-tasks", {}).items():
            try:
                periodic_snapshot_tasks.append(PeriodicSnapshotTask.from_data(id, task))
            except ValueError as e:
                errors.append(PeriodicSnapshotTaskDefinitionError(id, e))

        transports = data.get("transports", {})

        replication_tasks = []
        for id, task in data.get("replication-tasks", {}).items():
            if not isinstance(task["transport"], dict):
                try:
                    task["transport"] = transports[task["transport"]]
                except KeyError:
                    e = ValueError("Invalid transport {task['transport']!r}")
                    errors.append(ReplicationTaskDefinitionError(id, e))
                    continue

            try:
                replication_tasks.append(ReplicationTask.from_data(id, task, periodic_snapshot_tasks))
            except ValueError as e:
                errors.append(ReplicationTaskDefinitionError(id, e))

        if raise_on_error:
            raise DefinitionErrors(errors)

        return cls(periodic_snapshot_tasks + replication_tasks, timezone, errors)
