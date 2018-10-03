# -*- coding=utf-8 -*-
import logging

from zettarepl.snapshot.list import group_snapshots_by_datasets
from zettarepl.snapshot.name import parse_snapshots_names_with_multiple_schemas
from zettarepl.snapshot.snapshot import Snapshot

from .snapshot_owner import SnapshotOwner

logger = logging.getLogger(__name__)

__all__ = ["calculate_snapshots_to_remove"]


def calculate_snapshots_to_remove(owners: [SnapshotOwner], snapshots: [Snapshot]):
    result = []
    for dataset, dataset_snapshots in group_snapshots_by_datasets(snapshots).items():
        dataset_owners = [owner for owner in owners if owner.owns_dataset(dataset)]
        result.extend([
            Snapshot(dataset, snapshot)
            for snapshot in calculate_dataset_snapshots_to_remove(dataset_owners, dataset, dataset_snapshots)
        ])
    return result


def calculate_dataset_snapshots_to_remove(owners: [SnapshotOwner], dataset: str, snapshots: [Snapshot]):
    try:
        parsed_snapshot_names = parse_snapshots_names_with_multiple_schemas(
            snapshots,
            set().union(*[set(owner.get_naming_schemas()) for owner in owners])
        )
    except ValueError as e:
        logger.warning("Error parsing snapshot names for dataset %r: %r", dataset, e)
        return []

    result = []
    for parsed_snapshot_name in parsed_snapshot_names:
        snapshot_owners = [
            owner
            for owner in owners
            if owner.owns_snapshot(parsed_snapshot_name)
        ]
        if snapshot_owners and not any(owner.should_retain(dataset, parsed_snapshot_name) for owner in snapshot_owners):
            logger.debug("No one of %r retains snapshot %r", snapshot_owners, parsed_snapshot_name.name)
            result.append(parsed_snapshot_name.name)

    return result
