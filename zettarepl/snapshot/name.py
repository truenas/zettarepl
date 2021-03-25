# -*- coding=utf-8 -*-
from collections import namedtuple
from datetime import datetime
import logging
import re
from typing import Iterable

import pytz

logger = logging.getLogger(__name__)

__all__ = ["ParsedSnapshotName", "get_snapshot_name", "parse_snapshot_name", "parse_snapshots_names",
           "parse_snapshots_names_with_multiple_schemas", "parsed_snapshot_sort_key",
           "naming_schema_has_utcoffset", "validate_snapshot_naming_schema"]

ParsedSnapshotName = namedtuple(
    "ParsedSnapshotName", ["naming_schema", "name", "parsed_datetime", "datetime", "tzinfo"]
)


def get_snapshot_name(now: datetime, naming_schema: str) -> str:
    return now.strftime(naming_schema).replace("+", ":")


def parse_snapshot_name(name: str, naming_schema: str) -> [ParsedSnapshotName]:
    if "%s" in naming_schema:
        try:
            if not (m := re.match(naming_schema.replace("%s", "(?P<s>[0-9]+)") + "$", name)):
                raise ValueError(f"time data {name!r} does not match format {naming_schema!r}")
        except re.error as e:
            raise ValueError(f"Invalid naming schema: {e.msg}")

        d = datetime.fromtimestamp(int(m.group("s")))
    else:
        strptime_name = name
        if naming_schema_has_utcoffset(naming_schema):
            strptime_name = strptime_name.replace(":", "+")

        try:
            d = datetime.strptime(strptime_name, naming_schema)
        except ValueError:
            raise
        except re.error as e:
            raise ValueError(f"Invalid naming schema: {e.msg}")
        except Exception as e:
            raise ValueError(f"Invalid naming schema: {e!r}")

    return ParsedSnapshotName(naming_schema, name, d, d.replace(tzinfo=None), d.tzinfo)


def parse_snapshots_names(names: Iterable[str], naming_schema: str) -> [ParsedSnapshotName]:
    result = []
    for name in names:
        try:
            result.append(parse_snapshot_name(name, naming_schema))
        except ValueError:
            pass

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


def parsed_snapshot_sort_key(parsed_snapshot: ParsedSnapshotName):
    return (
        parsed_snapshot.datetime,
        # First go snapshots with naive datetime
        0 if parsed_snapshot.tzinfo is None else 1,
        # Snapshot with same datetime but greater utcoffset was taken first
        0 if parsed_snapshot.tzinfo is None else (
            -parsed_snapshot.tzinfo.utcoffset(None).total_seconds()
        ),
        # Lexicographic order for snapshots with same datetime
        parsed_snapshot.name
    )


def naming_schema_has_utcoffset(schema: str):
    return re.search("(^|[^%])%z", schema) is not None


def validate_snapshot_naming_schema(schema: str):
    if "%%" in schema:
        raise ValueError("% is not an allowed character in ZFS snapshot name")

    if m := re.findall("[^0-9A-Za-z %_.:-]", schema):
        if len(m) == 1:
            raise ValueError(f"{m[0]} is not an allowed character in ZFS snapshot name")
        else:
            raise ValueError(f"{''.join(m)} are not allowed characters in ZFS snapshot name")

    if "%s" in schema:
        if re.search("%[^s]", schema):
            raise ValueError("No other placeholder can be used with %s in naming schema")
    else:
        for s in ("%Y", "%m", "%d", "%H", "%M"):
            if s not in schema:
                raise ValueError(f"{s} must be present in snapshot naming schema")

    has_utcoffset = naming_schema_has_utcoffset(schema)
    if has_utcoffset and ":" in schema:
        raise ValueError("%z and `:` can't be present in snapshot naming schema at the same time. "
                         "ZFS snapshot names can't contain `+` so we use `:` instead.")

    for d in [
        datetime(2000, 2, 29, 19, 40, tzinfo=pytz.timezone("Etc/GMT-10")),
        datetime(2000, 2, 29, 19, 40, tzinfo=pytz.timezone("Etc/GMT+10")),
    ]:
        formatted = get_snapshot_name(d, schema)
        parsed = parse_snapshot_name(formatted, schema)
        if has_utcoffset:
            if (d.replace(tzinfo=None) != parsed.datetime or
                        parsed.tzinfo is None or d.tzinfo.utcoffset(None) != parsed.tzinfo.utcoffset(None)):
                raise ValueError(
                    f"Failed to parse datetime using provided format: datetime={d!r}, formatted={formatted}, "
                    f"parsed={parsed.datetime!r}, parsed_tzinfo={parsed.tzinfo!r}"
                )
        else:
            if d.replace(tzinfo=None) != parsed.datetime:
                raise ValueError(
                    f"Failed to parse datetime using provided format: datetime={d!r}, formatted={formatted}, "
                    f"parsed={parsed.datetime!r}"
                )
