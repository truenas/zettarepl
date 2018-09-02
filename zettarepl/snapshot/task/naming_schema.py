# -*- coding=utf-8 -*-
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)

__all__ = ["validate_snapshot_naming_schema"]


def validate_snapshot_naming_schema(schema: str):
    for s in ("%Y", "%m", "%d", "%H", "%M"):
        if not re.search(f"(^|[^%]){s}", schema):
            raise ValueError(f"{s} must be present in snapshot naming schema")

    d = datetime(2000, 2, 29, 19, 40)
    formatted = d.strftime(schema)
    parsed = datetime.strptime(formatted, schema)
    if d != parsed:
        raise ValueError(f"Failed to parse datetime using provided format: datetime={d:r}, formatted={formatted}, "
                         f"parsed={parsed}")
