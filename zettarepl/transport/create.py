# -*- coding=utf-8 -*-
from __future__ import annotations

import logging
from typing import Any, TYPE_CHECKING

from .local import LocalTransport
from .ssh import SshTransport
from .ssh_netcat import SshNetcatTransport

if TYPE_CHECKING:
    from .interface import Transport

logger = logging.getLogger(__name__)

__all__ = ["create_transport"]


def create_transport(data: dict[str, Any]) -> Transport:
    return {  # type: ignore
        "local": LocalTransport,
        "ssh": SshTransport,
        "ssh+netcat": SshNetcatTransport,
    }[data.pop("type")].from_data(data)
