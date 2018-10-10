# -*- coding=utf-8 -*-
import copy
from unittest.mock import ANY, call, Mock, patch

import pytest

from zettarepl.replication.run import (
    run_replication_tasks,
    calculate_replication_step_templates,
    get_target_dataset,
    resume_replications,
    get_snapshots_to_send,
    replicate_snapshots,
)
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
        run_replication_tasks(Mock(), Mock(), Mock(), tasks)

        assert run_replication_task_part.mock_calls == [
            call(tasks[task_id], source_dataset, ANY, ANY)
            for task_id, source_dataset in parts
        ]


@pytest.mark.parametrize("replication_task, src_datasets, replication_step_templates", [
    (
        Mock(source_datasets=["data/src"],
             target_dataset="data/dst",
             recursive=True,
             exclude=["data/src/trash"]),
        {"data/src": [], "data/src/work": [], "data/src/work/archive": []},
        [
            ("data/src", "data/dst", False),
            ("data/src/work", "data/dst/work", False),
            ("data/src/work/archive", "data/dst/work/archive", False),
        ]
    ),
])
def test__calculate_replication_step_templates(replication_task, src_datasets, replication_step_templates):
    with patch("zettarepl.replication.run.list_datasets_with_snapshots") as list_datasets_with_snapshots:
        list_datasets_with_snapshots.return_value = src_datasets

        with patch("zettarepl.replication.run.ReplicationStepTemplate") as ReplicationStepTemplate:
            calculate_replication_step_templates(replication_task, replication_task.source_datasets[0],
                                                 Mock(datasets=src_datasets), Mock())

            assert ReplicationStepTemplate.mock_calls == [call(replication_task, ANY, ANY, *replication_step_template)
                                                          for replication_step_template in replication_step_templates]


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


def test__resume_replications__resume():
    dst_context = Mock(datasets=["data/dst", "data/dst/work"])
    dst = Mock(dst_context=dst_context, dst_dataset="data/dst")
    dst_work = Mock(dst_context=dst_context, dst_dataset="data/dst/work")
    dst_zzzz = Mock(dst_context=dst_context, dst_dataset="data/dst/zzz")
    with patch("zettarepl.replication.run.get_receive_resume_token") as get_receive_resume_token:
        get_receive_resume_token.side_effect = lambda _, dataset: {"data/dst/work": "token"}.get(dataset)

        step = Mock()
        dst_work.instantiate.return_value = step
        with patch("zettarepl.replication.run.run_replication_step") as run_replication_step:
            result = resume_replications([dst, dst_work, dst_zzzz])

            dst_work.instantiate.assert_called_once_with(receive_resume_token="token")
            run_replication_step.assert_called_once_with(step)

            assert result is True


def test__resume_replications__no_resume():
    dst_context = Mock(datasets=["data/dst", "data/dst/work"])
    dst = Mock(dst_context=dst_context, dst_dataset="data/dst")
    dst_work = Mock(dst_context=dst_context, dst_dataset="data/dst/work")
    dst_zzzz = Mock(dst_context=dst_context, dst_dataset="data/dst/zzzz")
    with patch("zettarepl.replication.run.get_receive_resume_token") as get_receive_resume_token:
        get_receive_resume_token.return_value = None
        with patch("zettarepl.replication.run.run_replication_step") as run_replication_step:
            result = resume_replications([dst, dst_work, dst_zzzz])

            run_replication_step.assert_not_called()

            assert result is False


def test__get_snapshot_to_send__works():
    assert get_snapshots_to_send(
        ["2018-09-02_17-45", "2018-09-02_17-46", "2018-09-02_17-47"],
        ["2018-09-02_17-45"],
        Mock(periodic_snapshot_tasks=[Mock(naming_schema="%Y-%m-%d_%H-%M")],
             also_include_naming_schema=[],
             restrict_schedule=None,
             only_matching_schedule=False,
             retention_policy=Mock(calculate_delete_snapshots=Mock(return_value=[]))),
    ) == ("2018-09-02_17-45", ["2018-09-02_17-46", "2018-09-02_17-47"])


def test__get_snapshot_to_send__restrict_schedule():
    assert get_snapshots_to_send(
        ["2018-09-02_17-45", "2018-09-02_17-46", "2018-09-02_17-47"],
        ["2018-09-02_17-45"],
        Mock(periodic_snapshot_tasks=[Mock(naming_schema="%Y-%m-%d_%H-%M")],
             also_include_naming_schema=[],
             restrict_schedule=CronSchedule("*/2", "*", "*", "*", "*"),
             only_matching_schedule=False,
             retention_policy=Mock(calculate_delete_snapshots=Mock(return_value=[]))),
    ) == ("2018-09-02_17-45", ["2018-09-02_17-46"])


def test__get_snapshot_to_send__multiple_tasks():
    assert get_snapshots_to_send(
        ["1w-2018-09-02_00-00", "2d-2018-09-02_00-00", "2d-2018-09-02_12-00",
         "1w-2018-09-03_00-00", "2d-2018-09-03_12-00"],
        ["1w-2018-09-02_00-00", "2d-2018-09-02_00-00"],
        Mock(periodic_snapshot_tasks=[Mock(naming_schema="1w-%Y-%m-%d_%H-%M"),
                                      Mock(naming_schema="2d-%Y-%m-%d_%H-%M")],
             also_include_naming_schema=[],
             restrict_schedule=None,
             only_matching_schedule=False,
             retention_policy=Mock(calculate_delete_snapshots=Mock(return_value=[]))),
    ) == ("2d-2018-09-02_00-00", ["2d-2018-09-02_12-00", "1w-2018-09-03_00-00", "2d-2018-09-03_12-00"])


def test__get_snapshot_to_send__multiple_tasks_retention_policy():
    retention_policy = lambda now, src_snapshots, dst_snapshots: [dst_snapshots[1]]  # 1w-2018-09-03_00-00
    assert get_snapshots_to_send(
        ["1w-2018-09-02_00-00", "2d-2018-09-02_00-00", "2d-2018-09-02_12-00",
         "1w-2018-09-03_00-00", "2d-2018-09-03_12-00"],
        ["1w-2018-09-02_00-00", "2d-2018-09-02_00-00"],
        Mock(periodic_snapshot_tasks=[Mock(naming_schema="1w-%Y-%m-%d_%H-%M"),
                                      Mock(naming_schema="2d-%Y-%m-%d_%H-%M")],
             also_include_naming_schema=[],
             restrict_schedule=None,
             only_matching_schedule=False,
             retention_policy=Mock(calculate_delete_snapshots=Mock(side_effect=retention_policy))),
    ) == ("2d-2018-09-02_00-00", ["2d-2018-09-02_12-00", "2d-2018-09-03_12-00"])


def test__replicate_snapshots():
    step1 = Mock()
    step2 = Mock()
    step_template = Mock(instantiate=Mock(side_effect=lambda incremental_base, snapshot: {
        ("snap-5", "snap-6"): step1,
        ("snap-6", "snap-7"): step2,
    }[incremental_base, snapshot]))

    with patch("zettarepl.replication.run.run_replication_step") as run_replication_step:
        replicate_snapshots(step_template, "snap-5", ["snap-6", "snap-7"])

        assert run_replication_step.call_count == 2
        # call arguments are checked by `step_template.instantiate` side effect
