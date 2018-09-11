# -*- coding=utf-8 -*-
import enum
import logging

from zettarepl.definition.schema import replication_task_validator
from zettarepl.scheduler.cron import CronSchedule
from zettarepl.snapshot.task.task import PeriodicSnapshotTask
from zettarepl.transport.create import create_transport

from .direction import ReplicationDirection

logger = logging.getLogger(__name__)

__all__ = ["ReplicationTask"]


class TargetSnapshotRetentionPolicy:
    pass


class ReplicationTask:
    def __init__(self, id, direction: ReplicationDirection, transport, source_dataset: str, target_dataset: str,
                 recursive: bool, exclude: [str], periodic_snapshot_tasks: [PeriodicSnapshotTask],
                 also_include_naming_schema: [str], auto: bool, schedule: CronSchedule, restrict_schedule: CronSchedule,
                 only_matching_schedule: bool, allow_from_scratch: bool,
                 retention_policy: TargetSnapshotRetentionPolicy):
        self.id = id
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
        data.setdefault("periodic-snapshot-tasks", [])
        data.setdefault("also-include-naming-schema", [])
        data.setdefault("only-matching-schedule", False)
        data.setdefault("allow-from-scratch", False)
        data.setdefault("lifetime", None)

        resolved_periodic_snapshot_tasks = []
        for task_id in data["periodic-snapshot-tasks"]:
            for task in periodic_snapshot_tasks:
                if task.id == task_id:
                    resolved_periodic_snapshot_tasks.append(task)
                    break
            else:
                raise ValueError(f"Periodic snapshot task {task.id!r} does not exist")

        for periodic_snapshot_task in resolved_periodic_snapshot_tasks:
            for exclude in periodic_snapshot_task.exclude:
                if exclude not in data["exclude"]:
                    raise ValueError(
                        f"Replication tasks should exclude everything their periodic snapshot tasks exclude "
                        f"(task {data['id']!r} does not exclude {exclude!r} from periodic snapshot task {task.id!r})")

        data["direction"] = ReplicationDirection(data["direction"])

        schedule, restrict_schedule = cls._parse_schedules(data)

        retention_policy = None

        return cls(data["id"],
                   data["direction"],
                   create_transport(data["transport"]),
                   data["source-dataset"], data["target-dataset"],
                   data["recursive"], data["exclude"], resolved_periodic_snapshot_tasks,
                   data["also-include-naming-schema"], data["auto"], schedule, restrict_schedule,
                   data["only-matching-schedule"], data["allow-from-scratch"], retention_policy)

    @classmethod
    def _parse_schedules(cls, data):
        schedule = None
        restrict_schedule = None

        if "schedule" in data and not data["auto"]:
            raise ValueError("You can't have schedule for replication that does not run automatically")

        if data["direction"] == ReplicationDirection.PUSH:
            if "schedule" in data:
                if data["periodic-snapshot-tasks"]:
                    raise ValueError("Push replication can't be bound to periodic snapshot task and have "
                                     "schedule at the same time")

                schedule = CronSchedule.from_data(data["schedule"])
            else:
                if data["auto"] and not data["periodic-snapshot-tasks"]:
                    raise ValueError("Push replication that runs automatically must be either bound to periodic "
                                     "snapshot task or have schedule")

            if "restrict-schedule" in data:
                if not data["auto"]:
                    raise ValueError("You can only have restrict-schedule for replication that does not run "
                                     "automatically")

                if not data["periodic-snapshot-tasks"]:
                    raise ValueError("You can only have restrict-schedule for replication that is bound to "
                                     "periodic snapshot tasks")

                restrict_schedule = CronSchedule.from_data(data["restrict-schedule"])

        if data["direction"] == ReplicationDirection.PULL:
            if "schedule" in data:
                schedule = CronSchedule.from_data(data["schedule"])

            if "restrict-schedule" in data:
                raise ValueError("Pull replication can't have restrict-schedule")

            if data["periodic-snapshot-tasks"]:
                raise ValueError("Pull replication can't be bound to periodic snapshot task")

        return schedule, restrict_schedule
