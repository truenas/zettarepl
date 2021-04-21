# -*- coding=utf-8 -*-
from collections import namedtuple
from datetime import datetime, timedelta
import logging

import isodate

from zettarepl.scheduler.cron import CronSchedule
from zettarepl.snapshot.name import ParsedSnapshotName

logger = logging.getLogger(__name__)

__all__ = ["TargetSnapshotRetentionPolicy", "SameAsSourceSnapshotRetentionPolicy", "CustomSnapshotRetentionPolicy",
           "NoneSnapshotRetentionPolicy"]


class TargetSnapshotRetentionPolicy:
    @classmethod
    def from_data(cls, data: dict):
        if data["retention-policy"] == "source":
            return SameAsSourceSnapshotRetentionPolicy()

        if data["retention-policy"] == "custom":
            if "lifetime" not in data:
                raise ValueError("lifetime is required for custom retention policy")

            return CustomSnapshotRetentionPolicy(
                isodate.parse_duration(data["lifetime"]),
                sorted(
                    [
                        CustomSnapshotRetentionPolicyLifetime(
                            CronSchedule.from_data(lifetime["schedule"]),
                            isodate.parse_duration(lifetime["lifetime"]),
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
                                   parsed_src_snapshots_names: [ParsedSnapshotName],
                                   parsed_dst_snapshots_names: [ParsedSnapshotName]):
        raise NotImplementedError


class SameAsSourceSnapshotRetentionPolicy(TargetSnapshotRetentionPolicy):
    def calculate_delete_snapshots(self,
                                   now: datetime,
                                   parsed_src_snapshots_names: [ParsedSnapshotName],
                                   parsed_dst_snapshots_names: [ParsedSnapshotName]):
        return [parsed_dst_snapshot.name for parsed_dst_snapshot in parsed_dst_snapshots_names
                if parsed_dst_snapshot not in parsed_src_snapshots_names]


CustomSnapshotRetentionPolicyLifetime = namedtuple("CustomSnapshotRetentionPolicy", ["schedule", "lifetime"])


class CustomSnapshotRetentionPolicy(TargetSnapshotRetentionPolicy):
    def __init__(self, lifetime: timedelta, lifetimes: [CustomSnapshotRetentionPolicyLifetime]):
        self.lifetime = lifetime
        self.lifetimes = lifetimes

    def calculate_delete_snapshots(self,
                                   now: datetime,
                                   parsed_src_snapshots_names: [ParsedSnapshotName],
                                   parsed_dst_snapshots_names: [ParsedSnapshotName]):
        result = []
        for parsed_dst_snapshot in parsed_dst_snapshots_names:
            for lifetime in self.lifetimes:
                if lifetime.schedule.should_run(parsed_dst_snapshot.datetime):
                    lifetime = lifetime.lifetime
                    break
            else:
                lifetime = self.lifetime

            if parsed_dst_snapshot.datetime < now - lifetime:
                result.append(parsed_dst_snapshot.name)

        return result


class NoneSnapshotRetentionPolicy(TargetSnapshotRetentionPolicy):
    def calculate_delete_snapshots(self,
                                   now: datetime,
                                   parsed_src_snapshots_names: [ParsedSnapshotName],
                                   parsed_dst_snapshots_names: [ParsedSnapshotName]):
        return []
