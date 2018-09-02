# -*- coding=utf-8 -*-
import logging

logger = logging.getLogger(__name__)

__all__ = ["Shell", "ExecException"]


class Shell:
    def exec(self, args, encoding="utf8"):
        raise NotImplementedError


class ExecException(Exception):
    pass
