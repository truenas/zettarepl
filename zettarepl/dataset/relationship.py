# -*- coding=utf-8 -*-
import logging
import os

import zettarepl.dataset.exclude

logger = logging.getLogger(__name__)

__all__ = ["is_child", "belongs_to_tree"]


def is_child(child: str, parent: str):
    rel = os.path.relpath(child, parent)
    return rel == "." or not rel.startswith("..")


def belongs_to_tree(dataset: str, root: str, recursive: bool, exclude: [str]):
    return (
        is_child(dataset, root) and not zettarepl.dataset.exclude.should_exclude(dataset, exclude)
        if recursive
        else dataset == root
    )
