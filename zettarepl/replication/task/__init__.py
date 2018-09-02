# -*- coding=utf-8 -*-
import enum
import logging

from zettarepl.scheduler.cron import CronSchedule
from zettarepl.schema import replication_task_validator
from zettarepl.snapshot.task import PeriodicSnapshotTask

logger = logging.getLogger(__name__)

__all__ = ["ReplicationDirection", "ReplicationTask"]


class ReplicationDirection(enum.Enum):
    PUSH = "push"
    PULL = "pull"


class TargetSnapshotRetentionPolicy:
    pass


class ReplicationTask:
    def __init__(self, direction: ReplicationDirection, transport, source_dataset: str, target_dataset: str,
                 recursive: bool, exclude: [str], periodic_snapshot_tasks: [PeriodicSnapshotTask],
                 also_include_naming_schema: str, auto: bool, schedule: CronSchedule, restrict_schedule: CronSchedule,
                 only_matching_schedule: bool, allow_from_scratch: bool,
                 retention_policy: TargetSnapshotRetentionPolicy):
        self.direction = direction
        self.transport = transport
        self.source_dataset = source_dataset
        self.target_dataset = target_dataset
        self.recursive = recursive
        self.exclude = exclude
        self.periodic_snapshot_tasks = periodic_snapshot_tasks
        self.also_include_naming_schema = also_include_naming_schema
        self.auto = auto
        self.schedule = schedule
        self.restrict_schedule = restrict_schedule
        self.only_matching_schedule = only_matching_schedule
        self.allow_from_scratch = allow_from_scratch
        self.retention_policy = retention_policy

    @classmethod
    def from_data(cls, data: dict, periodic_snapshot_tasks: [PeriodicSnapshotTask]):
        replication_task_validator.validate(data)

        data.setdefault("exclude", [])
        data.setdefault("periodicSnapshotTasks", [])
        data.setdefault("alsoIncludeNamingSchema", None)
        data.setdefault("onlyMatchingSchedule", False)
        data.setdefault("allowFromScratch", False)
        data.setdefault("lifetime", None)

        resolved_periodic_snapshot_tasks = []
        for task in data["periodicSnapshotTasks"]:
            for t in periodic_snapshot_tasks:
                if t.id == task["id"]:
                    resolved_periodic_snapshot_tasks.append(t)
                    break
            else:
                raise ValueError(f"Periodic snapshot task {t.id!r} does not exist")

        data["direction"] = ReplicationDirection(data["direction"])

        schedule, restrict_schedule = cls._parse_schedules(data)

        return cls(data["direction"], create_transport(data["transport"]), data["sourceDataset"], data["targetDataset"],
                   data["recursive"], data["exclude"], resolved_periodic_snapshot_tasks,
                   data["alsoIncludeNamingSchema"], data["auto"], schedule, restrict_schedule,
                   data["onlyMatchingSchedule"], data["allowFromScratch"], retention_policy)

    @classmethod
    def _parse_schedules(cls, data):
        schedule = None
        restrict_schedule = None

        if "schedule" in data and not data["auto"]:
            raise ValueError("You can't have schedule for replication that does not run automatically")

        if data["direction"] == ReplicationDirection.PUSH:
            if "schedule" in data:
                if data["periodicSnapshotTasks"]:
                    raise ValueError("Push replication can't be bound to periodic snapshot task and have "
                                     "schedule at the same time")

                schedule = CronSchedule.from_data(data["schedule"])
            else:
                if data["auto"] and not data["periodicSnapshotTasks"]:
                    raise ValueError("Push replication that runs automatically must be either bound to periodic "
                                     "snapshot task or have schedule")

            if "restrictSchedule" in data:
                if not data["auto"]:
                    raise ValueError("You can only have restrictSchedule for replication that does not run "
                                     "automatically")

                if not data["periodicSnapshotTasks"]:
                    raise ValueError("You can only have restrictSchedule for replication that is bound to "
                                     "periodic snapshot tasks")

                restrict_schedule = CronSchedule.from_data(data["restrictSchedule"])

        if data["direction"] == ReplicationDirection.PULL:
            if "schedule" in data:
                schedule = CronSchedule.from_data(data["schedule"])

            if "restrictSchedule" in data:
                raise ValueError("Pull replication can't have restrictSchedule")

            if data["periodicSnapshotTasks"]:
                raise ValueError("Pull replication can't be bound to periodic snapshot task")

        return schedule, restrict_schedule
