# -*- coding=utf-8 -*-
from datetime import datetime

from mock import Mock, patch
import pytest

from zettarepl.scheduler.clock import Clock


@pytest.fixture()
def sleep(monkeypatch):
    mock = Mock()
    monkeypatch.setattr("time.sleep", mock)
    return mock


def test__time_backward(sleep):
    with patch("zettarepl.scheduler.clock.datetime") as datetime_:
        clock = Clock()

        clock.now = datetime(2018, 8, 31, 13, 20, 25)

        datetime_.utcnow.return_value = datetime(2018, 8, 31, 13, 20, 20)
        assert clock._tick() is None

        sleep.assert_not_called()
        assert clock.now == datetime(2018, 8, 31, 13, 20, 20)


def test__sleep_max_10s(sleep):
    with patch("zettarepl.scheduler.clock.datetime") as datetime_:
        clock = Clock()

        clock.now = datetime(2018, 8, 31, 13, 20, 25)

        datetime_.utcnow.return_value = datetime(2018, 8, 31, 13, 20, 35)
        assert clock._tick() is None

        sleep.assert_called_once_with(10)
        assert clock.now == datetime(2018, 8, 31, 13, 20, 35)


def test__sleep_at_the_end_of_the_minute(sleep):
    with patch("zettarepl.scheduler.clock.datetime") as datetime_:
        clock = Clock()

        clock.now = datetime(2018, 8, 31, 13, 20, 45)

        datetime_.utcnow.return_value = datetime(2018, 8, 31, 13, 20, 55, 500000)
        assert clock._tick() is None

        sleep.assert_called_once_with(4.5)
        assert clock.now == datetime(2018, 8, 31, 13, 20, 55, 500000)


def test__time_forward(sleep):
    with patch("zettarepl.scheduler.clock.datetime") as datetime_:
        clock = Clock()

        clock.now = datetime(2018, 8, 31, 13, 20, 50)

        datetime_.utcnow.return_value = datetime(2018, 8, 31, 13, 21, 1, 2)
        assert clock._tick() == datetime(2018, 8, 31, 13, 21, 1, 2)

        sleep.assert_not_called()
        assert clock.now == datetime(2018, 8, 31, 13, 21, 1, 2)
