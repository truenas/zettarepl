# -*- coding=utf-8 -*-
from datetime import datetime, timedelta
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


class CustomSnapshotRetentionPolicy(TargetSnapshotRetentionPolicy):
    def __init__(self, period: timedelta):
        self.period = period

    def calculate_delete_snapshots(self,
                                   now: datetime,
                                   parsed_src_snapshots_names: [ParsedSnapshotName],
                                   parsed_dst_snapshots_names: [ParsedSnapshotName]):
        return [parsed_dst_snapshot.name for parsed_dst_snapshot in parsed_dst_snapshots_names
                if parsed_dst_snapshot.datetime < now - self.period]


class NoneSnapshotRetentionPolicy(TargetSnapshotRetentionPolicy):
    def calculate_delete_snapshots(self,
                                   now: datetime,
                                   parsed_src_snapshots_names: [ParsedSnapshotName],
                                   parsed_dst_snapshots_names: [ParsedSnapshotName]):
        return []
