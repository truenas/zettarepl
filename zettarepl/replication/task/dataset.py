# -*- coding=utf-8 -*-
import logging
import os

from .task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["get_source_dataset", "get_target_dataset"]


def get_source_dataset_base(replication_task: ReplicationTask):
    commonprefix = os.path.commonpath(replication_task.source_datasets).rstrip("/")
    if not all(source_dataset == commonprefix or source_dataset.startswith(f"{commonprefix}/")
               for source_dataset in replication_task.source_datasets):
        commonprefix = commonprefix[:commonprefix.rfind("/") + 1]

    return commonprefix


def relpath(path: str, base: str):
    rel = os.path.relpath(path, base)
    if rel.startswith(".."):
        raise ValueError(f"Dataset {path!r} is not an ancestor of {base!r}")
    return rel


def get_source_dataset(replication_task: ReplicationTask, dst_dataset: str):
    return os.path.normpath(os.path.join(get_source_dataset_base(replication_task),
                                         relpath(dst_dataset, replication_task.target_dataset)))


def get_target_dataset(replication_task: ReplicationTask, src_dataset: str):
    return os.path.normpath(os.path.join(replication_task.target_dataset,
                                         relpath(src_dataset, get_source_dataset_base(replication_task))))
