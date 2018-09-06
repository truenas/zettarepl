# -*- coding=utf-8 -*-
import logging

from dateutil.tz import tzlocal
import pytz

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
            timezone = pytz.timezone(data["timezone"])
        else:
            timezone = tzlocal()

        periodic_snapshot_tasks = [
            PeriodicSnapshotTask.from_data(task) for task in data.get("periodic-snapshot-tasks", [])]
        replication_tasks = [
            ReplicationTask.from_data(task, periodic_snapshot_tasks) for task in data.get("replication-tasks", [])]

        return cls(periodic_snapshot_tasks + replication_tasks, timezone)
