# -*- coding=utf-8 -*-
import logging

from .base_ssh import BaseSshTransport
from .interface import Transport

logger = logging.getLogger(__name__)

__all__ = ["are_same_host"]


def are_same_host(t1: Transport, t2: Transport):
    if isinstance(t1, BaseSshTransport) and isinstance(t2, BaseSshTransport):
        return t1.hostname == t2.hostname

    return False
