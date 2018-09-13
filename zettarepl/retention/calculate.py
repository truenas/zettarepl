# -*- coding=utf-8 -*-
import logging

from zettarepl.snapshot.name import parse_snapshots_names

from .snapshot_owner import SnapshotOwner

logger = logging.getLogger(__name__)

__all__ = ["calculate_snapshots_to_remove"]


def calculate_snapshots_to_remove(naming_schema, owners: [SnapshotOwner], snapshot_names):
    result = []
    for parsed_snapshot_name in parse_snapshots_names(snapshot_names, naming_schema):
        snapshot_owners = [
            owner
            for owner in owners
            if owner.owns(parsed_snapshot_name)
        ]
        if snapshot_owners and not any(owner.should_retain(parsed_snapshot_name) for owner in snapshot_owners):
            result.append(parsed_snapshot_name.name)

    return result
