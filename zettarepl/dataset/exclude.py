# -*- coding=utf-8 -*-
import logging
import os

logger = logging.getLogger(__name__)

__all__ = ["should_exclude"]


def should_exclude(dataset: str, exclude: [str]):
    for excl in exclude:
        rel = os.path.relpath(dataset, excl)
        if rel == "." or not rel.startswith(".."):
            return True

    return False
