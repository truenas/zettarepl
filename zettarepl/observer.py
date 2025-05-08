# -*- coding=utf-8 -*-
import logging

logger = logging.getLogger(__name__)

__all__ = ["notify", "PeriodicSnapshotTaskStart", "PeriodicSnapshotTaskSuccess", "PeriodicSnapshotTaskError",
           "ReplicationTaskScheduled", "ReplicationTaskStart", "ReplicationTaskSnapshotStart",
           "ReplicationTaskSnapshotProgress", "ReplicationTaskSnapshotSuccess", "ReplicationTaskDataProgress",
           "ReplicationTaskSuccess", "ReplicationTaskError"]


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
    def __init__(self, task_id, dataset, snapshot):
        self.task_id = task_id
        self.dataset = dataset
        self.snapshot = snapshot


class PeriodicSnapshotTaskError(ObserverMessage):
    def __init__(self, task_id, error):
        self.task_id = task_id
        self.error = error


class ReplicationTaskScheduled(ObserverMessage):
    def __init__(self, task_id, waiting_reason):
        self.task_id = task_id
        self.waiting_reason = waiting_reason


class ReplicationTaskStart(ObserverMessage):
    def __init__(self, task_id):
        self.task_id = task_id


class ReplicationTaskSnapshotStart(ObserverMessage):
    def __init__(self, task_id, dataset, snapshot, snapshots_sent, snapshots_total):
        self.task_id = task_id
        self.dataset = dataset
        self.snapshot = snapshot
        self.snapshots_sent = snapshots_sent
        self.snapshots_total = snapshots_total


class ReplicationTaskSnapshotProgress(ObserverMessage):
    def __init__(self, task_id, dataset, snapshot, snapshots_sent, snapshots_total, bytes_sent, bytes_total):
        self.task_id = task_id
        self.dataset = dataset
        self.snapshot = snapshot
        self.snapshots_sent = snapshots_sent
        self.snapshots_total = snapshots_total
        self.bytes_sent = bytes_sent
        self.bytes_total = bytes_total


class ReplicationTaskSnapshotSuccess(ObserverMessage):
    def __init__(self, task_id, dataset, snapshot, snapshots_sent, snapshots_total):
        self.task_id = task_id
        self.dataset = dataset
        self.snapshot = snapshot
        self.snapshots_sent = snapshots_sent
        self.snapshots_total = snapshots_total


class ReplicationTaskDataProgress(ObserverMessage):
    def __init__(self, task_id, dataset, src_size, dst_size):
        self.task_id = task_id
        self.dataset = dataset
        self.src_size = src_size
        self.dst_size = dst_size


class ReplicationTaskSuccess(ObserverMessage):
    def __init__(self, task_id, warnings):
        self.task_id = task_id
        self.warnings = warnings


class ReplicationTaskError(ObserverMessage):
    def __init__(self, task_id, error):
        self.task_id = task_id
        self.error = error
