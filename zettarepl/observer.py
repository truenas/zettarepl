# -*- coding=utf-8 -*-
import logging

logger = logging.getLogger(__name__)

__all__ = ["notify", "PeriodicSnapshotTaskStart", "PeriodicSnapshotTaskSuccess", "PeriodicSnapshotTaskError",
           "ReplicationTaskStart", "ReplicationTaskSuccess", "ReplicationTaskError"]


def notify(observer, *args, **kwargs):
    if observer is not None:
        try:
            observer(*args, **kwargs)
        except Exception:
            logger.error("Unhandled exception in observer %r", observer, exc_info=True)


class ObserverMessage:
    pass


class PeriodicSnapshotTaskStart(ObserverMessage):
    def __init__(self, task_id):
        self.task_id = task_id


class PeriodicSnapshotTaskSuccess(ObserverMessage):
    def __init__(self, task_id):
        self.task_id = task_id


class PeriodicSnapshotTaskError(ObserverMessage):
    def __init__(self, task_id, error):
        self.task_id = task_id
        self.error = error


class ReplicationTaskStart(ObserverMessage):
    def __init__(self, task_id):
        self.task_id = task_id


class ReplicationTaskSuccess(ObserverMessage):
    def __init__(self, task_id):
        self.task_id = task_id


class ReplicationTaskError(ObserverMessage):
    def __init__(self, task_id, error):
        self.task_id = task_id
        self.error = error
