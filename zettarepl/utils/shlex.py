# -*- coding=utf-8 -*-
import logging
import shlex

logger = logging.getLogger(__name__)

__all__ = ["pipe"]


def pipe(*cmds):
    return ["sh", "-c", " | ".join([" ".join([shlex.quote(arg) for arg in args]) for args in cmds])]
