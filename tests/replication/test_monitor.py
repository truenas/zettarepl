# -*- coding=utf-8 -*-
import logging
from unittest.mock import Mock, patch

from zettarepl.replication.monitor import ReplicationMonitor


def test__replication_monitor__ok():
    logging.getLogger().setLevel(logging.DEBUG)

    get_receive_resume_token = Mock(side_effect=[None, "a", "a", "a", "a", "b"])

    with patch("zettarepl.replication.monitor.get_receive_resume_token", get_receive_resume_token):
        with patch("zettarepl.replication.monitor.threading.Event") as Event:
            event = Mock()
            event.wait.side_effect = [False, False, False, False, False, False, True]
            Event.return_value = event
            assert ReplicationMonitor(Mock(), Mock(), 60.0, 5).run() == True


def test__replication_monitor__not_ok():
    logging.getLogger().setLevel(logging.DEBUG)

    get_receive_resume_token = Mock(side_effect=[None, "a", "a", "a", "a", "a", "b"])

    with patch("zettarepl.replication.monitor.get_receive_resume_token", get_receive_resume_token):
        with patch("zettarepl.replication.monitor.threading.Event") as Event:
            event = Mock()
            event.wait.side_effect = [False, False, False, False, False, False, False, True]
            Event.return_value = event
            assert ReplicationMonitor(Mock(), Mock(), 60.0, 5).run() == False
