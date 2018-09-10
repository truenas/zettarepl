# -*- coding=utf-8 -*-
import logging

from .local import LocalTransport
from .ssh import SshTransport
from .ssh_netcat import SshNetcatTransport

logger = logging.getLogger(__name__)

__all__ = ["create_transport"]


def create_transport(data):
    return {
        "local": LocalTransport,
        "ssh": SshTransport,
        "ssh+netcat": SshNetcatTransport,
    }[data.pop("type")].from_data(data)
