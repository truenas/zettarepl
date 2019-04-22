# -*- coding=utf-8 -*-
import logging

logger = logging.getLogger(__name__)

__all__ = ["notify", "PeriodicSnapshotTaskStart", "PeriodicSnapshotTaskSuccess", "PeriodicSnapshotTaskError",
           "ReplicationTaskScheduled", "ReplicationTaskStart", "ReplicationTaskSnapshotProgress",
           "ReplicationTaskSnapshotSuccess", "ReplicationTaskSuccess", "ReplicationTaskError"]


def notify(observer, message):
    result = None
    if observer is not None:
        try:
            result = observer(message)
        except Exception:
            logger.error("Unhandled exception in observer %r", observer, exc_info=True)

    if message.response is not None and result is None:
        result = message.response()

    return result


class ObserverMessage:
    response = None


class PeriodicSnapshotTaskStartResponse:
    def __init__(self, properties=None):
        self.properties = properties or {}


class PeriodicSnapshotTaskStart(ObserverMessage):
    response = PeriodicSnapshotTaskStartResponse

    def __init__(self, task_id):
        self.task_id = task_id


class PeriodicSnapshotTaskSuccess(ObserverMessage):
    def __init__(self, task_id):
        self.task_id = task_id


class PeriodicSnapshotTaskError(ObserverMessage):
    def __init__(self, task_id, error):
        self.task_id = task_id
        self.error = error


class ReplicationTaskScheduled(ObserverMessage):
    def __init__(self, task_id):
        self.task_id = task_id


class ReplicationTaskStart(ObserverMessage):
    def __init__(self, task_id):
        self.task_id = task_id


class ReplicationTaskSnapshotProgress(ObserverMessage):
    def __init__(self, task_id, dataset, snapshot, current, total):
        self.task_id = task_id
        self.dataset = dataset
        self.snapshot = snapshot
        self.current = current
        self.total = total


class ReplicationTaskSnapshotSuccess(ObserverMessage):
    def __init__(self, task_id, dataset, snapshot):
        self.task_id = task_id
        self.dataset = dataset
        self.snapshot = snapshot


class ReplicationTaskSuccess(ObserverMessage):
    def __init__(self, task_id):
        self.task_id = task_id


class ReplicationTaskError(ObserverMessage):
    def __init__(self, task_id, error):
        self.task_id = task_id
        self.error = error
