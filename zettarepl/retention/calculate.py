# -*- coding=utf-8 -*-
from collections import defaultdict
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

    newest_snapshot_for_naming_schema = {}
    for parsed_snapshot_name in parsed_snapshot_names:
        if parsed_snapshot_name.naming_schema is None:
            continue

        if (
                parsed_snapshot_name.naming_schema not in newest_snapshot_for_naming_schema or
                (
                    newest_snapshot_for_naming_schema[parsed_snapshot_name.naming_schema].parsed_datetime <
                    parsed_snapshot_name.parsed_datetime
                )
        ):
            newest_snapshot_for_naming_schema[parsed_snapshot_name.naming_schema] = parsed_snapshot_name
    newest_snapshot_for_naming_schema = {k: v.name for k, v in newest_snapshot_for_naming_schema.items()}

    snapshots_left_for_naming_schema = defaultdict(set)
    for parsed_snapshot_name in parsed_snapshot_names:
        snapshots_left_for_naming_schema[parsed_snapshot_name.naming_schema].add(parsed_snapshot_name.name)

    result = []
    for parsed_snapshot_name in parsed_snapshot_names:
        snapshot_owners = [
            owner
            for owner in owners
            if (
                # Owners owning `None` naming schema may own all snapshots
                {parsed_snapshot_name.naming_schema, None} & set(owner.get_naming_schemas()) and
                owner.owns_snapshot(dataset, parsed_snapshot_name)
            )
        ]
        if (
                snapshot_owners and
                any(owner.wants_to_delete() for owner in snapshot_owners) and
                not any(owner.should_retain(dataset, parsed_snapshot_name) for owner in snapshot_owners)
        ):
            logger.debug("No one of %r retains snapshot %r", snapshot_owners, parsed_snapshot_name.name)
            snapshots_left_for_naming_schema[parsed_snapshot_name.naming_schema].discard(parsed_snapshot_name.name)
            result.append(parsed_snapshot_name.name)

    for naming_schema, snapshots_left in snapshots_left_for_naming_schema.items():
        if naming_schema is None:
            # We do not want this behavior for snapshots with unknown naming schema
            continue

        if not snapshots_left:
            newest_snapshot = newest_snapshot_for_naming_schema[naming_schema]
            logger.info("Not destroying %r as it is the only snapshot left for naming schema %r",
                        newest_snapshot, naming_schema)
            result.remove(newest_snapshot)

    return result
