# -*- coding=utf-8 -*-
from __future__ import annotations

from collections.abc import Callable
import dataclasses
import logging
from typing import Any, ClassVar, overload

logger = logging.getLogger(__name__)

__all__ = ["notify", "Observer", "ObserverMessage",
           "PeriodicSnapshotTaskStart", "PeriodicSnapshotTaskSuccess", "PeriodicSnapshotTaskError",
           "ReplicationTaskScheduled", "ReplicationTaskStart", "ReplicationTaskLog",
           "ReplicationTaskSnapshotStart", "ReplicationTaskSnapshotProgress", "ReplicationTaskSnapshotSuccess",
           "ReplicationTaskDataProgress", "ReplicationTaskSuccess", "ReplicationTaskError"]


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


@dataclasses.dataclass
class ObserverMessage:
    response: ClassVar[type | None] = None

    registry: ClassVar[dict[str, type[ObserverMessage]]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        ObserverMessage.registry[cls.__name__] = cls

    def dump(self) -> dict[str, Any]:
        return {"type": type(self).__name__, **dataclasses.asdict(self)}

    @classmethod
    def load(cls, data: dict[str, Any]) -> ObserverMessage:
        fields = dict(data)
        name = fields.pop("type")
        try:
            subclass = ObserverMessage.registry[name]
        except KeyError:
            raise ValueError(f"Unknown observer message: {name!r}") from None

        return subclass(**fields)


@dataclasses.dataclass
class ObserverMessageWithResponse[T](ObserverMessage):
    response: ClassVar[type] = type(None)


Observer = Callable[[ObserverMessage], None] | None


@dataclasses.dataclass
class PeriodicSnapshotTaskStartResponse:
    properties: dict[str, str] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class PeriodicSnapshotTaskStart(ObserverMessageWithResponse[PeriodicSnapshotTaskStartResponse]):
    response: ClassVar[type] = PeriodicSnapshotTaskStartResponse

    task_id: str


@dataclasses.dataclass
class PeriodicSnapshotTaskSuccess(ObserverMessage):
    task_id: str
    dataset: str
    snapshot: str
    already_existed: bool


@dataclasses.dataclass
class PeriodicSnapshotTaskError(ObserverMessage):
    task_id: str
    error: str


@dataclasses.dataclass
class ReplicationTaskScheduled(ObserverMessage):
    task_id: str
    waiting_reason: str


@dataclasses.dataclass
class ReplicationTaskStart(ObserverMessage):
    task_id: str


@dataclasses.dataclass
class ReplicationTaskLog(ObserverMessage):
    task_id: str
    log: str


@dataclasses.dataclass
class ReplicationTaskSnapshotStart(ObserverMessage):
    task_id: str
    dataset: str
    snapshot: str
    snapshots_sent: int
    snapshots_total: int


@dataclasses.dataclass
class ReplicationTaskSnapshotProgress(ObserverMessage):
    task_id: str
    dataset: str
    snapshot: str
    snapshots_sent: int
    snapshots_total: int
    bytes_sent: int
    bytes_total: int


@dataclasses.dataclass
class ReplicationTaskSnapshotSuccess(ObserverMessage):
    task_id: str
    dataset: str
    snapshot: str
    snapshots_sent: int
    snapshots_total: int


@dataclasses.dataclass
class ReplicationTaskDataProgress(ObserverMessage):
    task_id: str
    dataset: str
    src_size: int
    dst_size: int


@dataclasses.dataclass
class ReplicationTaskSuccess(ObserverMessage):
    task_id: str
    warnings: list[str]


@dataclasses.dataclass
class ReplicationTaskError(ObserverMessage):
    task_id: str
    error: str
