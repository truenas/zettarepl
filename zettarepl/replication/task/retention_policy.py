# -*- coding=utf-8 -*-
from datetime import timedelta
import logging

import isodate

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

            return CustomSnapshotRetentionPolicy(isodate.parse_duration(data["lifetime"]))

        if data["retention-policy"] == "none":
            return NoneSnapshotRetentionPolicy()

        raise ValueError(f"Unknown retention policy: {data['retention-policy']!r}")

    def calculate_delete_snapshots(self,
                                   parsed_src_snapshots: [ParsedSnapshotName],
                                   parsed_dst_snapshots: [ParsedSnapshotName]):
        raise NotImplementedError


class SameAsSourceSnapshotRetentionPolicy(TargetSnapshotRetentionPolicy):
    def calculate_delete_snapshots(self, parsed_src_snapshots: [ParsedSnapshotName],
                                   parsed_dst_snapshots: [ParsedSnapshotName]):
        return [parsed_dst_snapshot.name for parsed_dst_snapshot in parsed_dst_snapshots
                if parsed_dst_snapshot not in parsed_src_snapshots]


class CustomSnapshotRetentionPolicy(TargetSnapshotRetentionPolicy):
    def __init__(self, period: timedelta):
        self.period = period

    def calculate_delete_snapshots(self, parsed_src_snapshots: [ParsedSnapshotName],
                                   parsed_dst_snapshots: [ParsedSnapshotName]):
        if parsed_dst_snapshots:
            newest_snapshot = max(parsed_dst_snapshot.datetime for parsed_dst_snapshot in parsed_src_snapshots)
            return [parsed_dst_snapshot.name for parsed_dst_snapshot in parsed_dst_snapshots
                    if parsed_dst_snapshot.datetime < newest_snapshot - self.period]

        return []


class NoneSnapshotRetentionPolicy(TargetSnapshotRetentionPolicy):
    def calculate_delete_snapshots(self, parsed_src_snapshots: [ParsedSnapshotName],
                                   parsed_dst_snapshots: [ParsedSnapshotName]):
        return []
