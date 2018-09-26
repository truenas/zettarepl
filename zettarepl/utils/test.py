# -*- coding=utf-8 -*-
import logging
from unittest.mock import PropertyMock

logger = logging.getLogger(__name__)

__all__ = ["mock_name"]


def mock_name(mock, name):
    type(mock).name = PropertyMock(return_value=name)
    return mock
