# -*- coding=utf-8 -*-
import logging

from .local import LocalTransport

logger = logging.getLogger(__name__)

__all__ = ["create_transport"]


def create_transport(data):
    return {
        "local": LocalTransport
    }[data["type"]].from_data(data)
