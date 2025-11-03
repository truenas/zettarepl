# -*- coding=utf-8 -*-
from datetime import datetime, timedelta, timezone
from unittest.mock import ANY

import pytest

from zettarepl.snapshot.name import (
    ParsedSnapshotName, parse_snapshot_name, parse_snapshots_names_with_multiple_schemas,
)


@pytest.mark.parametrize("name,naming_schema,parsed_datetime", [
    ("auto_hourly_2022-11-04_10-00_--0200", "auto_hourly_%Y-%m-%d_%H-%M_%z",
     datetime(2022, 11, 4, 10, 0, tzinfo=timezone(timedelta(seconds=7200)))),
    ("auto_hourly_2022-11-04_10-00_:0200", "auto_hourly_%Y-%m-%d_%H-%M_%z",
     datetime(2022, 11, 4, 10, 0, tzinfo=timezone(timedelta(seconds=7200)))),
])
def test_parse_snapshot_name(name, naming_schema, parsed_datetime):
    assert parse_snapshot_name(name, naming_schema).parsed_datetime == parsed_datetime


def test__parse_snapshots_names_with_multiple_schemas__multiple_schemas():
    assert set(
        parse_snapshots_names_with_multiple_schemas(
            [
                "snap-2018-09-06-11-30",
                "snap-2018-09-06-11_31",
            ],
            [
                "snap-%Y-%m-%d-%H_%M",
                "snap-%Y-%m-%d-%H-%M",
            ]
        )
    ) == {
        ParsedSnapshotName("snap-%Y-%m-%d-%H-%M", "snap-2018-09-06-11-30", datetime(2018, 9, 6, 11, 30),
                           datetime(2018, 9, 6, 11, 30), None),
        ParsedSnapshotName("snap-%Y-%m-%d-%H_%M", "snap-2018-09-06-11_31", datetime(2018, 9, 6, 11, 31),
                           datetime(2018, 9, 6, 11, 31), None),
    }


def test__parse_snapshots_names_with_multiple_schemas__multiple_schemas__inambiguous():
    assert set(
        parse_snapshots_names_with_multiple_schemas(
            [
                "snap-2018-09-06-11-30-1w",
                "snap-2018-09-06-11-30-2m",
            ],
            [
                "snap-%Y-%m-%d-%H-%M-1w",
                "snap-%Y-%m-%d-%H-%M-2m",
            ]
        )
    ) == {
        ParsedSnapshotName("snap-%Y-%m-%d-%H-%M-1w", "snap-2018-09-06-11-30-1w", datetime(2018, 9, 6, 11, 30),
                           datetime(2018, 9, 6, 11, 30), None),
        ParsedSnapshotName("snap-%Y-%m-%d-%H-%M-2m", "snap-2018-09-06-11-30-2m", datetime(2018, 9, 6, 11, 30),
                           datetime(2018, 9, 6, 11, 30), None),
    }


def test__parse_snapshots_names_with_multiple_schemas__multiple_schemas__ambiguous():
    with pytest.raises(ValueError) as e:
        parse_snapshots_names_with_multiple_schemas(
            [
                "snap-2018-09-06-11-30",
            ],
            [
                "snap-%Y-%m-%d-%H-%M",
                "snap-%Y-%d-%m-%H-%M",
            ]
        )

    assert e.value.args[0] == ("Snapshot name snap-2018-09-06-11-30 was parsed ambiguously: as 2018-09-06 11:30:00, "
                               "and, with naming schema snap-%Y-%d-%m-%H-%M, as 2018-06-09 11:30:00")


def test__parse_snapshots_name__with_timestamp():
    result = parse_snapshots_names_with_multiple_schemas(
        [
            "snap-2018-09-06-11-30",
            "snap-1536226260",
        ],
        [
            "snap-%Y-%m-%d-%H-%M",
            "snap-%s",
        ]
    )

    expected = [
        ParsedSnapshotName("snap-%Y-%m-%d-%H-%M", "snap-2018-09-06-11-30", datetime(2018, 9, 6, 11, 30),
                           datetime(2018, 9, 6, 11, 30), None),
        ParsedSnapshotName("snap-%s", "snap-1536226260", ANY, ANY, None),
    ]

    assert result == expected or result == expected[::-1]


@pytest.mark.parametrize("has_none", [True, False])
def test__parse_snapshots_names_with_multiple_schemas__none(has_none):
    naming_schemas = [
        "snap-%Y-%m-%d-%H_%M",
        "snap-%Y-%m-%d-%H-%M",
    ]
    result = {
        ParsedSnapshotName("snap-%Y-%m-%d-%H-%M", "snap-2018-09-06-11-30", datetime(2018, 9, 6, 11, 30),
                           datetime(2018, 9, 6, 11, 30), None),
        ParsedSnapshotName("snap-%Y-%m-%d-%H_%M", "snap-2018-09-06-11_31", datetime(2018, 9, 6, 11, 31),
                           datetime(2018, 9, 6, 11, 31), None),
    }
    if has_none:
        naming_schemas.append(None)
        result.add(ParsedSnapshotName(None, "snap-1", None, None, None))

    assert set(
        parse_snapshots_names_with_multiple_schemas(
            [
                "snap-2018-09-06-11-30",
                "snap-2018-09-06-11_31",
                "snap-1"
            ],
            naming_schemas
        )
    ) == result
