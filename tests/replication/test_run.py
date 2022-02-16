# -*- coding=utf-8 -*-
from collections import defaultdict
from datetime import time
from unittest.mock import ANY, call, Mock, patch

import pytest

from zettarepl.observer import ReplicationTaskStart, ReplicationTaskSuccess
from zettarepl.replication.run import (
    run_replication_tasks,
    calculate_replication_step_templates,
    get_target_dataset,
    get_snapshots_to_send,
    replicate_snapshots,
    broken_pipe_error,
)
from zettarepl.replication.error import ReplicationError
from zettarepl.replication.snapshots_to_send import SnapshotsToSend
from zettarepl.replication.task.direction import ReplicationDirection
from zettarepl.scheduler.cron import CronSchedule


@pytest.mark.parametrize("tasks,parts", [
    (
        [
            Mock(direction=ReplicationDirection.PUSH, source_datasets=["work"], recursive=False),
            Mock(direction=ReplicationDirection.PUSH, source_datasets=["data/garbage"], recursive=True),
            Mock(direction=ReplicationDirection.PUSH, source_datasets=["data"], recursive=False),
            Mock(direction=ReplicationDirection.PUSH, source_datasets=["data"], recursive=True),
        ],
        [
            (3, "data"),
            (2, "data"),
            (1, "data/garbage"),
            (0, "work"),
        ]
    ),
    (
        [
            Mock(direction=ReplicationDirection.PUSH, source_datasets=["data/work"], recursive=True),
            Mock(direction=ReplicationDirection.PUSH, source_datasets=["data", "data/work/ix"], recursive=False),
        ],
        [
            (1, "data"),
            (0, "data/work"),
            (1, "data/work/ix"),
        ],
    ),
])
def test__run_replication_tasks(tasks, parts):
    for task in tasks:
        task.retries = 1

    with patch("zettarepl.replication.run.run_replication_task_part") as run_replication_task_part:
        run_replication_tasks(Mock(), Mock(), Mock(), Mock(), tasks)

        assert run_replication_task_part.mock_calls == [
            call(tasks[task_id], source_dataset, ANY, ANY, None)
            for task_id, source_dataset in parts
        ]


def test__run_replication_tasks__do_not_try_second_part_if_first_has_failed():
    task1 = Mock(direction=ReplicationDirection.PUSH, source_datasets=["data/work"], recursive=True,
                 retries=1)
    task2 = Mock(direction=ReplicationDirection.PUSH, source_datasets=["data", "data/work/ix"], recursive=False,
                 retries=1)

    def run_replication_task_part__side_effect(replication_task, source_dataset, src_context, dst_context, observer):
        if replication_task == task2:
            if source_dataset == "data":
                raise ReplicationError("This should fail")
            else:
                raise Exception("This should never be reached")

    with patch("zettarepl.replication.run.run_replication_task_part",
               Mock(side_effect=run_replication_task_part__side_effect)) as run_replication_task_part:
        run_replication_tasks(Mock(), Mock(), Mock(), Mock(), [task1, task2])

        assert run_replication_task_part.call_args_list == [
            call(task2, "data", ANY, ANY, None),
            call(task1, "data/work", ANY, ANY, None),
        ]


def test__run_replication_tasks__notifies_start_once():
    task1 = Mock(direction=ReplicationDirection.PUSH, retries=1)
    task2 = Mock(direction=ReplicationDirection.PUSH, retries=1)

    with patch("zettarepl.replication.run.calculate_replication_tasks_parts",
               Mock(return_value=[(task1, Mock()), (task2, Mock()), (task1, Mock())])):
        with patch("zettarepl.replication.run.run_replication_task_part"):
            observer = Mock()

            run_replication_tasks(Mock(), Mock(), Mock(), Mock(), [task1, task2], observer)

            assert [c[0][0].task_id for c in observer.call_args_list if isinstance(c[0][0], ReplicationTaskStart)] ==\
                   [task1.id, task2.id]


def test__run_replication_tasks__only_notify_success_after_last_part():
    task1 = Mock(direction=ReplicationDirection.PUSH, retries=1)
    task2 = Mock(direction=ReplicationDirection.PUSH, retries=1)

    with patch("zettarepl.replication.run.calculate_replication_tasks_parts",
               Mock(return_value=[(task1, Mock()), (task2, Mock()), (task1, Mock())])):
        with patch("zettarepl.replication.run.run_replication_task_part"):
            observer = Mock()

            run_replication_tasks(Mock(), Mock(), Mock(), Mock(), [task1, task2], observer)

            assert [c[0][0].task_id for c in observer.call_args_list if isinstance(c[0][0], ReplicationTaskSuccess)] ==\
                   [task2.id, task1.id]


@pytest.mark.parametrize("replication_task, src_datasets, replication_step_templates", [
    (
        Mock(source_datasets=["data/src"],
             target_dataset="data/dst",
             recursive=True,
             replicate=False,
             exclude=["data/src/trash"],
             properties_exclude=[],
             properties_override={}),
        {"data/src": [], "data/src/work": [], "data/src/work/archive": []},
        [
            ("data/src", "data/dst"),
            ("data/src/work", "data/dst/work"),
            ("data/src/work/archive", "data/dst/work/archive"),
        ]
    ),
])
def test__calculate_replication_step_templates(replication_task, src_datasets, replication_step_templates):
    with patch("zettarepl.replication.run.list_datasets_with_snapshots") as list_datasets_with_snapshots:
        with patch("zettarepl.replication.run.list_datasets_with_properties", Mock(return_value=[])):
            with patch("zettarepl.replication.run.list_snapshots_for_datasets") as list_snapshots_for_datasets:
                list_datasets_with_snapshots.return_value = src_datasets
                list_snapshots_for_datasets.return_value = src_datasets

                with patch("zettarepl.replication.run.ReplicationStepTemplate") as ReplicationStepTemplate:
                    calculate_replication_step_templates(replication_task, replication_task.source_datasets[0],
                                                         Mock(datasets=src_datasets), Mock())

                    assert ReplicationStepTemplate.mock_calls == [
                        call(replication_task, ANY, ANY, *replication_step_template, set())
                        for replication_step_template in replication_step_templates
                    ]


def test__get_target_dataset__1():
    assert get_target_dataset(
        Mock(source_datasets=["data/src"], target_dataset="data/dst"),
        "data/src"
    ) == "data/dst"


def test__get_target_dataset__2():
    assert get_target_dataset(
        Mock(source_datasets=["data/src"], target_dataset="data/dst"),
        "data/src/a/b"
    ) == "data/dst/a/b"


def test__get_snapshot_to_send__works():
    assert get_snapshots_to_send(
        ["2018-09-02_17-45", "2018-09-02_17-46", "2018-09-02_17-47"],
        ["2018-09-02_17-45"],
        Mock(periodic_snapshot_tasks=[Mock(naming_schema="%Y-%m-%d_%H-%M")],
             also_include_naming_schema=[],
             name_pattern=None,
             restrict_schedule=None,
             only_matching_schedule=False,
             retention_policy=Mock(calculate_delete_snapshots=Mock(return_value=[]))),
        Mock(), Mock(),
    ) == SnapshotsToSend("2018-09-02_17-45", ["2018-09-02_17-46", "2018-09-02_17-47"], False, False)


def test__get_snapshot_to_send__restrict_schedule():
    assert get_snapshots_to_send(
        ["2018-09-02_17-45", "2018-09-02_17-46", "2018-09-02_17-47"],
        ["2018-09-02_17-45"],
        Mock(periodic_snapshot_tasks=[Mock(naming_schema="%Y-%m-%d_%H-%M")],
             also_include_naming_schema=[],
             name_pattern=None,
             restrict_schedule=CronSchedule("*/2", "*", "*", "*", "*", time(0, 0), time(23, 59)),
             only_matching_schedule=False,
             retention_policy=Mock(calculate_delete_snapshots=Mock(return_value=[]))),
        Mock(), Mock(),
    ) == SnapshotsToSend("2018-09-02_17-45", ["2018-09-02_17-46"], False, False)


def test__get_snapshot_to_send__multiple_tasks():
    assert get_snapshots_to_send(
        ["1w-2018-09-02_00-00", "2d-2018-09-02_00-00", "2d-2018-09-02_12-00",
         "1w-2018-09-03_00-00", "2d-2018-09-03_12-00"],
        ["1w-2018-09-02_00-00", "2d-2018-09-02_00-00"],
        Mock(periodic_snapshot_tasks=[Mock(naming_schema="1w-%Y-%m-%d_%H-%M"),
                                      Mock(naming_schema="2d-%Y-%m-%d_%H-%M")],
             also_include_naming_schema=[],
             name_pattern=None,
             restrict_schedule=None,
             only_matching_schedule=False,
             retention_policy=Mock(calculate_delete_snapshots=Mock(return_value=[]))),
        Mock(), Mock(),
    ) == SnapshotsToSend("2d-2018-09-02_00-00", ["2d-2018-09-02_12-00", "1w-2018-09-03_00-00", "2d-2018-09-03_12-00"],
                         False, False)


def test__get_snapshot_to_send__multiple_tasks_retention_policy():
    retention_policy = lambda now, src_snapshots, dst_snapshots: [dst_snapshots[1]]  # 1w-2018-09-03_00-00
    assert get_snapshots_to_send(
        ["1w-2018-09-02_00-00", "2d-2018-09-02_00-00", "2d-2018-09-02_12-00",
         "1w-2018-09-03_00-00", "2d-2018-09-03_12-00"],
        ["1w-2018-09-02_00-00", "2d-2018-09-02_00-00"],
        Mock(periodic_snapshot_tasks=[Mock(naming_schema="1w-%Y-%m-%d_%H-%M"),
                                      Mock(naming_schema="2d-%Y-%m-%d_%H-%M")],
             also_include_naming_schema=[],
             name_pattern=None,
             restrict_schedule=None,
             only_matching_schedule=False,
             retention_policy=Mock(calculate_delete_snapshots=Mock(side_effect=retention_policy))),
        Mock(), Mock(),
    ) == SnapshotsToSend("2d-2018-09-02_00-00", ["2d-2018-09-02_12-00", "2d-2018-09-03_12-00"], False, False)


def test__replicate_snapshots():
    step1 = Mock()
    step2 = Mock()
    step_template = Mock(
        instantiate=Mock(side_effect=lambda incremental_base, snapshot, include_intermediate, encryption: {
            ("snap-5", "snap-6"): step1,
            ("snap-6", "snap-7"): step2,
        }[incremental_base, snapshot])
    )
    step_template.src_context.context = Mock(snapshots_sent_by_replication_step_template=defaultdict(lambda: 0),
                                             snapshots_total_by_replication_step_template=defaultdict(lambda: 0))

    with patch("zettarepl.replication.run.run_replication_step") as run_replication_step:
        replicate_snapshots(step_template, "snap-5", ["snap-6", "snap-7"], False, None, None)

        assert run_replication_step.call_count == 2
        # call arguments are checked by `step_template.instantiate` side effect


@pytest.mark.parametrize("error,wrapped", [
    ("No ECDSA host key is known.\nHost key verification failed.\n",
     "No ECDSA host key is known.\nHost key verification failed.\nBroken pipe."),
    ("No ECDSA host key is known.\nHost key verification failed.",
     "No ECDSA host key is known.\nHost key verification failed.\nBroken pipe."),
    ("Host key verification failed.", "Host key verification failed. Broken pipe."),
    ("Host key verification failed", "Host key verification failed. Broken pipe."),
])
def test_broken_pipe_error(error, wrapped):
    assert broken_pipe_error(error) == wrapped
