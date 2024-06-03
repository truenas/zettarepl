# -*- coding=utf-8 -*-
import logging

logger = logging.getLogger(__name__)

__all__ = ["get_removal_dates"]


def get_removal_dates():
    from truenas_api_client import Client
    with Client() as c:
        return c.call("zettarepl.get_removal_dates")
