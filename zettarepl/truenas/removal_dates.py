# -*- coding=utf-8 -*-
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

__all__ = ["get_removal_dates"]


def get_removal_dates() -> dict[str, datetime] | None:
    from truenas_api_client import Client
    with Client(private_methods=True) as c:
        return c.call("zettarepl.get_removal_dates")  # type: ignore[no-any-return]
