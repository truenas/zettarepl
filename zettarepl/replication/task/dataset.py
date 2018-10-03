# -*- coding=utf-8 -*-
import logging
import os

from .task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["get_source_dataset", "get_target_dataset"]


def get_source_dataset(replication_task: ReplicationTask, dst_dataset: str):
    return os.path.normpath(
        os.path.join(replication_task.source_dataset, os.path.relpath(dst_dataset, replication_task.target_dataset)))


def get_target_dataset(replication_task: ReplicationTask, src_dataset: str):
    return os.path.normpath(
        os.path.join(replication_task.target_dataset, os.path.relpath(src_dataset, replication_task.source_dataset)))
