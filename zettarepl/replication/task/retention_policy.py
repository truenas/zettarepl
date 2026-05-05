# -*- coding=utf-8 -*-
from datetime import datetime, timedelta
import logging
from typing import Any, NamedTuple

from zettarepl.scheduler.cron import CronSchedule
from zettarepl.snapshot.name import ParsedSnapshotName
from zettarepl.utils.datetime import parse_duration

logger = logging.getLogger(__name__)

__all__ = ["TargetSnapshotRetentionPolicy", "SameAsSourceSnapshotRetentionPolicy", "CustomSnapshotRetentionPolicy",
           "NoneSnapshotRetentionPolicy"]


class TargetSnapshotRetentionPolicy:
    @classmethod
    def from_data(cls, data: dict[str, Any]) -> "TargetSnapshotRetentionPolicy":
        if data["retention-policy"] == "source":
            return SameAsSourceSnapshotRetentionPolicy()

        if data["retention-policy"] == "custom":
            if "lifetime" not in data:
                raise ValueError("lifetime is required for custom retention policy")

            return CustomSnapshotRetentionPolicy(
                parse_duration(data["lifetime"]),
                sorted(
                    [
                        CustomSnapshotRetentionPolicyLifetime(
                            CronSchedule.from_data(lifetime["schedule"]),
                            parse_duration(lifetime["lifetime"]),
                        )
                        for lifetime in data.get("lifetimes", {}).values()
                    ],
                    key=lambda lifetime: lifetime.lifetime,
                    reverse=True,
                ),
            )

        if data["retention-policy"] == "none":
            return NoneSnapshotRetentionPolicy()

        raise ValueError(f"Unknown retention policy: {data['retention-policy']!r}")

    def calculate_delete_snapshots(self,
                                   now: datetime,
                                   parsed_src_snapshots_names: list[ParsedSnapshotName],
                                   parsed_dst_snapshots_names: list[ParsedSnapshotName]) -> list[str]:
        raise NotImplementedError


class SameAsSourceSnapshotRetentionPolicy(TargetSnapshotRetentionPolicy):
    def calculate_delete_snapshots(self,
                                   now: datetime,
                                   parsed_src_snapshots_names: list[ParsedSnapshotName],
                                   parsed_dst_snapshots_names: list[ParsedSnapshotName]) -> list[str]:
        return [parsed_dst_snapshot.name for parsed_dst_snapshot in parsed_dst_snapshots_names
                if parsed_dst_snapshot not in parsed_src_snapshots_names]


class CustomSnapshotRetentionPolicyLifetime(NamedTuple):
    schedule: CronSchedule
    lifetime: timedelta


class CustomSnapshotRetentionPolicy(TargetSnapshotRetentionPolicy):
    def __init__(self, lifetime: timedelta, lifetimes: list[CustomSnapshotRetentionPolicyLifetime]) -> None:
        self.lifetime = lifetime
        self.lifetimes = lifetimes

    def calculate_delete_snapshots(self,
                                   now: datetime,
                                   parsed_src_snapshots_names: list[ParsedSnapshotName],
                                   parsed_dst_snapshots_names: list[ParsedSnapshotName]) -> list[str]:
        result = []
        for parsed_dst_snapshot in parsed_dst_snapshots_names:
            if parsed_dst_snapshot.datetime is None:
                raise ValueError("Parsed destination snapshot %r must have datetime set")

            for lifetime in self.lifetimes:
                if lifetime.schedule.should_run(parsed_dst_snapshot.datetime):
                    snapshot_lifetime = lifetime.lifetime
                    break
            else:
                snapshot_lifetime = self.lifetime

            if parsed_dst_snapshot.datetime < now - snapshot_lifetime:
                result.append(parsed_dst_snapshot.name)

        return result


class NoneSnapshotRetentionPolicy(TargetSnapshotRetentionPolicy):
    def calculate_delete_snapshots(self,
                                   now: datetime,
                                   parsed_src_snapshots_names: list[ParsedSnapshotName],
                                   parsed_dst_snapshots_names: list[ParsedSnapshotName]) -> list[str]:
        return []
