# -*- coding=utf-8 -*-
from __future__ import annotations

from collections.abc import Callable
import logging
from typing import Any, overload

logger = logging.getLogger(__name__)

__all__ = ["notify", "PeriodicSnapshotTaskStart", "PeriodicSnapshotTaskSuccess", "PeriodicSnapshotTaskError",
           "ReplicationTaskScheduled", "ReplicationTaskStart", "ReplicationTaskSnapshotStart",
           "ReplicationTaskSnapshotProgress", "ReplicationTaskSnapshotSuccess", "ReplicationTaskDataProgress",
           "ReplicationTaskSuccess", "ReplicationTaskError"]


@overload
def notify[T](  # type: ignore[overload-overlap]
    observer: Callable[[ObserverMessageWithResponse[T]], T] | None,
    message: ObserverMessageWithResponse[T],
) -> T: ...
@overload
def notify(observer: Callable[[ObserverMessage], None] | None, message: ObserverMessage) -> None: ...


def notify(observer: Callable[..., Any] | None, message: ObserverMessage) -> Any:
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
    response: type | None = None


class ObserverMessageWithResponse[T](ObserverMessage):
    response: type[T]


class PeriodicSnapshotTaskStartResponse:
    def __init__(self, properties: dict[str, str] | None = None) -> None:
        self.properties = properties or {}


class PeriodicSnapshotTaskStart(ObserverMessageWithResponse[PeriodicSnapshotTaskStartResponse]):
    response = PeriodicSnapshotTaskStartResponse

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id


class PeriodicSnapshotTaskSuccess(ObserverMessage):
    def __init__(self, task_id: str, dataset: str, snapshot: str, already_existed: bool) -> None:
        self.task_id = task_id
        self.dataset = dataset
        self.snapshot = snapshot
        self.already_existed = already_existed


class PeriodicSnapshotTaskError(ObserverMessage):
    def __init__(self, task_id: str, error: str) -> None:
        self.task_id = task_id
        self.error = error


class ReplicationTaskScheduled(ObserverMessage):
    def __init__(self, task_id: str, waiting_reason: str) -> None:
        self.task_id = task_id
        self.waiting_reason = waiting_reason


class ReplicationTaskStart(ObserverMessage):
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id


class ReplicationTaskSnapshotStart(ObserverMessage):
    def __init__(self, task_id: str, dataset: str, snapshot: str, snapshots_sent: int, snapshots_total: int) -> None:
        self.task_id = task_id
        self.dataset = dataset
        self.snapshot = snapshot
        self.snapshots_sent = snapshots_sent
        self.snapshots_total = snapshots_total


class ReplicationTaskSnapshotProgress(ObserverMessage):
    def __init__(self, task_id: str, dataset: str, snapshot: str, snapshots_sent: int,
                 snapshots_total: int, bytes_sent: int, bytes_total: int) -> None:
        self.task_id = task_id
        self.dataset = dataset
        self.snapshot = snapshot
        self.snapshots_sent = snapshots_sent
        self.snapshots_total = snapshots_total
        self.bytes_sent = bytes_sent
        self.bytes_total = bytes_total


class ReplicationTaskSnapshotSuccess(ObserverMessage):
    def __init__(self, task_id: str, dataset: str, snapshot: str, snapshots_sent: int, snapshots_total: int) -> None:
        self.task_id = task_id
        self.dataset = dataset
        self.snapshot = snapshot
        self.snapshots_sent = snapshots_sent
        self.snapshots_total = snapshots_total


class ReplicationTaskDataProgress(ObserverMessage):
    def __init__(self, task_id: str, dataset: str, src_size: int, dst_size: int) -> None:
        self.task_id = task_id
        self.dataset = dataset
        self.src_size = src_size
        self.dst_size = dst_size


class ReplicationTaskSuccess(ObserverMessage):
    def __init__(self, task_id: str, warnings: list[str]) -> None:
        self.task_id = task_id
        self.warnings = warnings


class ReplicationTaskError(ObserverMessage):
    def __init__(self, task_id: str, error: str) -> None:
        self.task_id = task_id
        self.error = error
