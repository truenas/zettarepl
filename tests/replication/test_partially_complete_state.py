# -*- coding=utf-8 -*-
from unittest.mock import Mock, patch

import pytest

from zettarepl.replication.error import ContainsPartiallyCompleteState
from zettarepl.replication.partially_complete_state import retry_contains_partially_complete_state
from zettarepl.transport.interface import ExecException

ERROR = ContainsPartiallyCompleteState()


def test__normal():
    run = Mock()

    retry_contains_partially_complete_state(run)

    run.assert_called_once_with()


def test__contains_partially_complete__other_error():
    other_error = ExecException(1, "Everything failed.")

    run = Mock(side_effect=other_error)

    with pytest.raises(ExecException) as e:
        retry_contains_partially_complete_state(run)

    assert e.value == other_error


def test__contains_partially_complete__contains_partially_complete_state():
    run = Mock(side_effect=[ERROR, None])

    with patch("zettarepl.replication.partially_complete_state.time.sleep") as sleep:
        retry_contains_partially_complete_state(run)

        sleep.assert_called_once_with(60)


def test__contains_partially_complete__contains_partially_complete_state__forever():
    run = Mock(side_effect=[ERROR] * 60)

    with pytest.raises(ContainsPartiallyCompleteState) as e:
        with patch("zettarepl.replication.partially_complete_state.time.sleep"):
            retry_contains_partially_complete_state(run)
