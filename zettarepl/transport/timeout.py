# -*- coding=utf-8 -*-
import logging
from threading import local

logger = logging.getLogger(__name__)

__all__ = ["get_shell_timeout", "ShellTimeoutContext"]

shell_timeout = local()


def get_shell_timeout():
    return getattr(shell_timeout, "timeout", None)


class ShellTimeoutContext:
    def __init__(self, timeout):
        self.timeout = timeout
        self.prev_timeout = None

    def __enter__(self):
        self.prev_timeout = get_shell_timeout()
        shell_timeout.timeout = self.timeout

    def __exit__(self, exc_type, exc_val, exc_tb):
        shell_timeout.timeout = self.prev_timeout
