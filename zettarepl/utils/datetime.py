# -*- coding=utf-8 -*-
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

__all__ = ["idealized_datetime"]


def idealized_datetime(d: datetime):
    return d.replace(minute=0, microsecond=0, tzinfo=None)
