# -*- coding=utf-8 -*-
import logging

from .local import LocalTransport
from .ssh import SshTransport

logger = logging.getLogger(__name__)

__all__ = ["create_transport"]


def create_transport(data):
    return {
        "local": LocalTransport,
        "ssh": SshTransport,
    }[data.pop("type")].from_data(data)
