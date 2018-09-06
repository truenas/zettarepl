# -*- coding=utf-8 -*-
from datetime import datetime

import pytest

from zettarepl.snapshot.name import ParsedSnapshotName, parse_snapshots_names_with_multiple_schemas


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
        ParsedSnapshotName("snap-2018-09-06-11-30", datetime(2018, 9, 6, 11, 30)),
        ParsedSnapshotName("snap-2018-09-06-11_31", datetime(2018, 9, 6, 11, 31)),
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
        ParsedSnapshotName("snap-2018-09-06-11-30-1w", datetime(2018, 9, 6, 11, 30)),
        ParsedSnapshotName("snap-2018-09-06-11-30-2m", datetime(2018, 9, 6, 11, 30)),
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
