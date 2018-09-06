# -*- coding=utf-8 -*-
from collections import namedtuple
from datetime import datetime
import logging
from typing import Iterable

logger = logging.getLogger(__name__)

__all__ = ["parse_snapshots_names"]

ParsedSnapshotName = namedtuple("ParsedSnapshotName", ["name", "datetime"])


def parse_snapshots_names(names: Iterable[str], naming_schema: str) -> [ParsedSnapshotName]:
    result = []
    for name in names:
        try:
            d = datetime.strptime(name, naming_schema)
        except ValueError:
            pass
        else:
            result.append(ParsedSnapshotName(name, d))

    return result


def parse_snapshots_names_with_multiple_schemas(names: Iterable[str], naming_schemas: [str]) -> [ParsedSnapshotName]:
    parsed_snapshots = {}
    for naming_schema in naming_schemas:
        for parsed_snapshot in parse_snapshots_names(names, naming_schema):
            existing_parsed_snapshot = parsed_snapshots.get(parsed_snapshot.name)
            if existing_parsed_snapshot is None:
                parsed_snapshots[parsed_snapshot.name] = parsed_snapshot
            else:
                if existing_parsed_snapshot.datetime != parsed_snapshot.datetime:
                    raise ValueError(f"Snapshot name {parsed_snapshot.name} was parsed ambiguously: "
                                     f"as {existing_parsed_snapshot.datetime}, and, "
                                     f"with naming schema {naming_schema}, as {parsed_snapshot.datetime}")

    return list(parsed_snapshots.values())
