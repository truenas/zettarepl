# -*- coding=utf-8 -*-
import logging
import shlex

logger = logging.getLogger(__name__)

__all__ = ["implode", "pipe"]


def implode(args):
    return " ".join([shlex.quote(arg) for arg in args])


def pipe(*cmds):
    return ["sh", "-c", " | ".join([implode(args) for args in cmds])]
