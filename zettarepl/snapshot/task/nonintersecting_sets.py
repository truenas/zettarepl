# -*- coding=utf-8 -*-
import logging
import os

from .task import PeriodicSnapshotTask

logger = logging.getLogger(__name__)

__all__ = ["calculate_nonintersecting_sets"]


def calculate_nonintersecting_sets(tasks: [PeriodicSnapshotTask]):
    sets = []
    for task in tasks:
        added = False
        for set_tasks in sets:
            for set_task in set_tasks:
                if tasks_intersect(task, set_task):
                    set_tasks.append(task)
                    added = True
                    break
            if added:
                break
        if not added:
            sets.append([task])

    return sets


def tasks_intersect(t1: PeriodicSnapshotTask, t2: PeriodicSnapshotTask):
    if t1.dataset == t2.dataset:
        return True

    commonprefix = os.path.commonpath([t1.dataset, t2.dataset])

    # data/work, data/windows
    if not commonprefix.endswith("/"):
        # data, data/windows
        if t1.dataset == commonprefix or t2.dataset == commonprefix:
            pass
        else:
            commonprefix = commonprefix[:commonprefix.rfind("/") + 1]

    # completely different datasets
    if commonprefix == "":
        return False

    # data/a, data/b
    if commonprefix.endswith("/"):
        return False

    t1, t2 = sorted([t1, t2], key=lambda t: len(t.dataset))
    return t1.recursive
