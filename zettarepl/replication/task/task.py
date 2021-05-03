# -*- coding=utf-8 -*-
import logging
import re

from zettarepl.dataset.relationship import is_child
from zettarepl.definition.schema import replication_task_validator
from zettarepl.scheduler.cron import CronSchedule
from zettarepl.snapshot.task.task import PeriodicSnapshotTask
from zettarepl.transport.create import create_transport

from .compression import *
from .direction import ReplicationDirection
from .encryption import ReplicationEncryption, KeyFormat
from .readonly_behavior import ReadOnlyBehavior
from .retention_policy import *

logger = logging.getLogger(__name__)

__all__ = ["ReplicationTask"]


class ReplicationTask:
    def __init__(self,
                 id,
                 direction: ReplicationDirection,
                 transport, source_datasets: [str],
                 target_dataset: str,
                 recursive: bool,
                 exclude: [str],
                 properties: bool,
                 properties_exclude: [str],
                 properties_override: {str: str},
                 replicate: bool,
                 encryption: ReplicationEncryption,
                 periodic_snapshot_tasks: [PeriodicSnapshotTask],
                 also_include_naming_schema: [str],
                 name_pattern: re.Pattern,
                 auto: bool,
                 schedule: CronSchedule,
                 restrict_schedule: CronSchedule,
                 only_matching_schedule: bool,
                 readonly: ReadOnlyBehavior,
                 allow_from_scratch: bool,
                 hold_pending_snapshots: bool,
                 retention_policy: TargetSnapshotRetentionPolicy,
                 compression: ReplicationCompression,
                 speed_limit: int,
                 dedup: bool,
                 large_block: bool,
                 embed: bool,
                 compressed: bool,
                 retries: int,
                 logging_level: int):
        self.id = id
        self.direction = direction
        self.transport = transport
        self.source_datasets = source_datasets
        self.target_dataset = target_dataset
        self.recursive = recursive
        self.exclude = exclude
        self.properties = properties
        self.properties_exclude = properties_exclude
        self.properties_override = properties_override
        self.replicate = replicate
        self.encryption = encryption
        self.periodic_snapshot_tasks = periodic_snapshot_tasks
        self.also_include_naming_schema = also_include_naming_schema
        self.name_pattern = name_pattern
        self.auto = auto
        self.schedule = schedule
        self.restrict_schedule = restrict_schedule
        self.only_matching_schedule = only_matching_schedule
        self.readonly = readonly
        self.allow_from_scratch = allow_from_scratch
        self.hold_pending_snapshots = hold_pending_snapshots
        self.retention_policy = retention_policy
        self.compression = compression
        self.speed_limit = speed_limit
        self.dedup = dedup
        self.large_block = large_block
        self.embed = embed
        self.compressed = compressed
        self.retries = retries
        self.logging_level = logging_level

    def __repr__(self):
        return f"<Replication Task {self.id!r}>"

    @classmethod
    def from_data(cls, id, data: dict, periodic_snapshot_tasks: [PeriodicSnapshotTask]):
        replication_task_validator.validate(data)

        for k in ["source-dataset", "naming-schema", "also-include-naming-schema"]:
            if k in data and isinstance(data[k], str):
                data[k] = [data[k]]

        data.setdefault("exclude", [])
        data.setdefault("properties", True)
        data.setdefault("properties-exclude", [])
        data.setdefault("properties-override", {})
        data.setdefault("replicate", False)
        data.setdefault("encryption", None)
        data.setdefault("periodic-snapshot-tasks", [])
        data.setdefault("name-regex", None)
        data.setdefault("only-matching-schedule", False)
        data.setdefault("readonly", "ignore")
        data.setdefault("allow-from-scratch", False)
        data.setdefault("hold-pending-snapshots", False)
        data.setdefault("compression", None)
        data.setdefault("speed-limit", None)
        data.setdefault("dedup", False)
        data.setdefault("large-block", False)
        data.setdefault("embed", False)
        data.setdefault("compressed", False)
        data.setdefault("retries", 5)
        data.setdefault("logging-level", "notset")

        resolved_periodic_snapshot_tasks = []
        for periodic_snapshot_task_id in data["periodic-snapshot-tasks"]:
            for periodic_snapshot_task in periodic_snapshot_tasks:
                if periodic_snapshot_task.id == periodic_snapshot_task_id:
                    resolved_periodic_snapshot_tasks.append(periodic_snapshot_task)
                    break
            else:
                raise ValueError(f"Periodic snapshot task {periodic_snapshot_task_id!r} does not exist")

        if data["recursive"]:
            cls._validate_exclude(data, resolved_periodic_snapshot_tasks)

        if data["replicate"]:
            if not data["recursive"]:
                raise ValueError(
                    "Replication tasks that replicate entire filesystem should be recursive"
                )
            if data["exclude"]:
                raise ValueError(
                    "Replication tasks that replicate entire filesystem can't exclude datasets"
                )
            if not data["properties"]:
                raise ValueError(
                    "Replication tasks that replicate entire filesystem can't exclude properties"
                )

        if data["encryption"]:
            encryption = ReplicationEncryption(
                data["encryption"]["key"],
                KeyFormat(data["encryption"]["key-format"]),
                data["encryption"]["key-location"],
            )
        else:
            encryption = None

        data["direction"] = ReplicationDirection(data["direction"])

        if data["direction"] == ReplicationDirection.PUSH:
            if "naming-schema" in data:
                raise ValueError("Push replication task can't have naming-schema")

            data.setdefault("also-include-naming-schema", [])

            if not resolved_periodic_snapshot_tasks and not data["also-include-naming-schema"] and not data["name-regex"]:
                raise ValueError(
                    "You must at least provide either periodic-snapshot-tasks or also-include-naming-schema or "
                    "name-regex for push replication task"
                )

        elif data["direction"] == ReplicationDirection.PULL:
            if "naming-schema" not in data and not data["name-regex"]:
                raise ValueError("You must provide naming-schema or name-regex for pull replication task")

            if "also-include-naming-schema" in data:
                raise ValueError("Pull replication task can't have also-include-naming-schema")

            data.setdefault("also-include-naming-schema", data.pop("naming-schema"))

        schedule, restrict_schedule = cls._parse_schedules(data)

        if data["direction"] == ReplicationDirection.PULL:
            if data["hold-pending-snapshots"]:
                raise ValueError("Pull replication tasks can't hold pending snapshots because they don't do source "
                                 "retention")

        if data["name-regex"]:
            try:
                name_pattern = re.compile(f"({data['name-regex']})$")
            except Exception as e:
                raise ValueError(f"Invalid name-regex: {e}")

            if data["also-include-naming-schema"]:
                raise ValueError("naming-schema/also-include-naming-schema can't be used with name-regex")
        else:
            name_pattern = None

        retention_policy = TargetSnapshotRetentionPolicy.from_data(data)

        compression = replication_compressions[data["compression"]] if data["compression"] else None

        return cls(id,
                   data["direction"],
                   create_transport(data["transport"]),
                   data["source-dataset"],
                   data["target-dataset"],
                   data["recursive"],
                   data["exclude"],
                   data["properties"],
                   data["properties-exclude"],
                   {k: str(v) for k, v in data["properties-override"].items()},
                   data["replicate"],
                   encryption,
                   resolved_periodic_snapshot_tasks,
                   data["also-include-naming-schema"],
                   name_pattern,
                   data["auto"],
                   schedule,
                   restrict_schedule,
                   data["only-matching-schedule"],
                   ReadOnlyBehavior(data["readonly"]),
                   data["allow-from-scratch"],
                   data["hold-pending-snapshots"],
                   retention_policy,
                   compression,
                   data["speed-limit"],
                   data["dedup"],
                   data["large-block"],
                   data["embed"],
                   data["compressed"],
                   data["retries"],
                   logging._nameToLevel[data["logging-level"].upper()])

    @classmethod
    def _validate_exclude(cls, data, resolved_periodic_snapshot_tasks):
        for source_dataset in data["source-dataset"]:
            for periodic_snapshot_task in resolved_periodic_snapshot_tasks:
                if is_child(source_dataset, periodic_snapshot_task.dataset):
                    for exclude in periodic_snapshot_task.exclude:
                        if is_child(exclude, source_dataset) and exclude not in data["exclude"]:
                            raise ValueError(
                                "Replication tasks should exclude everything their periodic snapshot tasks exclude "
                                f"(task does not exclude {exclude!r} from periodic snapshot task "
                                f"{periodic_snapshot_task.id!r})")

    @classmethod
    def _parse_schedules(cls, data):
        if "schedule" in data:
            schedule = CronSchedule.from_data(data["schedule"])
        else:
            schedule = None

        if "restrict-schedule" in data:
            restrict_schedule = CronSchedule.from_data(data["restrict-schedule"])
        else:
            restrict_schedule = None

        if data["direction"] == ReplicationDirection.PUSH:
            if schedule is None and data["auto"] and not data["periodic-snapshot-tasks"]:
                raise ValueError("Push replication that runs automatically must be either bound to a periodic "
                                 "snapshot task or have a schedule")

        if data["direction"] == ReplicationDirection.PULL:
            if schedule is None and data["auto"]:
                raise ValueError("Pull replication that runs automatically must have a schedule")

            if data["periodic-snapshot-tasks"]:
                raise ValueError("Pull replication can't be bound to a periodic snapshot task")

        if schedule:
            if not data["auto"]:
                raise ValueError("You can't have schedule for replication that does not run automatically")
        else:
            if data["only-matching-schedule"]:
                raise ValueError("You can't have only-matching-schedule without schedule")

        return schedule, restrict_schedule
