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
