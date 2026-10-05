# -*- coding=utf-8 -*-
from __future__ import annotations

from datetime import datetime
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from zettarepl.daemon.client import MiddlewareClient

logger = logging.getLogger(__name__)

__all__ = ["get_removal_dates"]


def get_removal_dates(client: MiddlewareClient | None = None) -> dict[str, datetime] | None:
    if client is None:
        return None

    return client.call("zettarepl.get_removal_dates")  # type: ignore[no-any-return]
