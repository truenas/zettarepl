# -*- coding=utf-8 -*-
import pytest

from zettarepl.daemon.service import encode_definition_errors
from zettarepl.definition.definition import (
    DefinitionError,
    PeriodicSnapshotTaskDefinitionError,
    ReplicationTaskDefinitionError,
)
from zettarepl.observer import (
    ObserverMessage,
    PeriodicSnapshotTaskError,
    PeriodicSnapshotTaskStart,
    PeriodicSnapshotTaskSuccess,
    ReplicationTaskDataProgress,
    ReplicationTaskError,
    ReplicationTaskLog,
    ReplicationTaskScheduled,
    ReplicationTaskSnapshotProgress,
    ReplicationTaskSnapshotStart,
    ReplicationTaskSnapshotSuccess,
    ReplicationTaskStart,
    ReplicationTaskSuccess,
)

MESSAGES = [
    PeriodicSnapshotTaskStart("task_1"),
    PeriodicSnapshotTaskSuccess("task_1", "data/src", "snap-1", False),
    PeriodicSnapshotTaskError("task_1", "Something went wrong"),
    ReplicationTaskScheduled("task_1", "Waiting for retention to complete"),
    ReplicationTaskStart("task_1"),
    ReplicationTaskLog("task_1", "a line"),
    ReplicationTaskSnapshotStart("task_1", "data/src", "snap-1", 1, 2),
    ReplicationTaskSnapshotProgress("task_1", "data/src", "snap-1", 1, 2, 100, 200),
    ReplicationTaskSnapshotSuccess("task_1", "data/src", "snap-1", 1, 2),
    ReplicationTaskDataProgress("task_1", "data/src", 300, 400),
    ReplicationTaskSuccess("task_1", ["Be careful"]),
    ReplicationTaskError("task_1", "Something went wrong"),
]


@pytest.mark.parametrize("message", MESSAGES, ids=lambda m: type(m).__name__)
def test_round_trip(message):
    assert ObserverMessage.load(message.dump()) == message


def test_dump_names_the_type():
    assert ReplicationTaskStart("task_1").dump() == {"type": "ReplicationTaskStart", "task_id": "task_1"}


def test_load_rejects_an_unknown_type():
    with pytest.raises(ValueError, match="Unknown observer message"):
        ObserverMessage.load({"type": "something_else", "task_id": "task_1"})


def test_load_rejects_a_truncated_message():
    with pytest.raises(TypeError):
        ObserverMessage.load({"type": "ReplicationTaskError", "task_id": "task_1"})


def test_encode_definition_errors():
    assert encode_definition_errors([
        PeriodicSnapshotTaskDefinitionError("task_1", ValueError("bad schema")),
        ReplicationTaskDefinitionError("task_2", ValueError("bad transport")),
        DefinitionError("Unknown timezone: 'Mars/Olympus'"),
    ]) == [
        {"type": "periodic_snapshot_task", "task_id": "task_1",
         "error": "When parsing periodic snapshot task task_1: bad schema"},
        {"type": "replication_task", "task_id": "task_2",
         "error": "When parsing replication task task_2: bad transport"},
        {"type": "definition", "task_id": None, "error": "Unknown timezone: 'Mars/Olympus'"},
    ]


def test_registry_covers_every_message():
    assert {type(message).__name__ for message in MESSAGES} <= set(ObserverMessage.registry)
    for name, klass in ObserverMessage.registry.items():
        assert klass.__name__ == name
