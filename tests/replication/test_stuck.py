# -*- coding=utf-8 -*-
from unittest.mock import Mock, patch

import pytest

from zettarepl.replication.error import StuckReplicationError
from zettarepl.replication.stuck import retry_stuck_replication
from zettarepl.transport.interface import ExecException

ERROR = ExecException(1, "destination data/work contains partially-complete state from \"zfs receive -s\".\n")


def test__normal():
    run = Mock()

    retry_stuck_replication(run, None)

    run.assert_called_once_with()


def test__contains_partially_complete_from_the_very_beginning():
    run = Mock(side_effect=ERROR)

    with pytest.raises(ExecException) as e:
        retry_stuck_replication(run, None)

    assert e.value == ERROR


def test__contains_partially_complete__other_error():
    run = Mock(side_effect=ERROR)

    with pytest.raises(ExecException) as e:
        retry_stuck_replication(run, ExecException(1, "Everything failed."))

    assert e.value == ERROR


def test__contains_partially_complete__stuck():
    run = Mock(side_effect=[ERROR, None])

    with patch("zettarepl.replication.stuck.time.sleep") as sleep:
        retry_stuck_replication(run, StuckReplicationError())

        sleep.assert_called_once_with(60)


def test__contains_partially_complete__stuck_forever():
    run = Mock(side_effect=[ERROR] * 60)

    with pytest.raises(ExecException) as e:
        with patch("zettarepl.replication.stuck.time.sleep"):
            retry_stuck_replication(run, StuckReplicationError())

    assert e.value == ERROR
