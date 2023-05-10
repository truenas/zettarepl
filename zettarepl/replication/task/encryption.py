# -*- coding=utf-8 -*-
from collections import namedtuple
import enum
import logging

logger = logging.getLogger(__name__)

__all__ = ["ReplicationEncryption", "KeyFormat"]

ReplicationEncryption = namedtuple("ReplicationEncryption", ["inherit", "key", "key_format", "key_location"])


class KeyFormat(enum.Enum):
    HEX = "hex"
    PASSPHRASE = "passphrase"
