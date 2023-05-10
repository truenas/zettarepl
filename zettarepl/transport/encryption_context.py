# -*- coding=utf-8 -*-
import io
import json
import logging
import random
import string

from zettarepl.replication.task.encryption import KeyFormat

logger = logging.getLogger(__name__)

__all__ = ["EncryptionContext"]


class EncryptionContext:
    def __init__(self, replication_process, shell):
        self.replication_process = replication_process
        self.shell = shell

        self.tmp_key_location = None

    def enter(self):
        if self.replication_process.encryption.inherit:
            return ["encryption"], {}
        else:
            if self.replication_process.encryption.key_location == "$TrueNAS":
                self.tmp_key_location = "/tmp/zettarepl-key-" + (
                    "".join([random.choice(string.ascii_letters + string.digits) for _ in range(32)])
                )
                key_location = self.tmp_key_location
            else:
                key_location = self.replication_process.encryption.key_location

            self.shell.put_file(io.BytesIO(self.replication_process.encryption.key.encode("utf-8")), key_location)

            return [], {
                "encryption": "on",
                "keyformat": self.replication_process.encryption.key_format.value,
                "keylocation": f"file://{key_location}"
            }

    def exit(self, success):
        if self.tmp_key_location is not None:
            self.shell.exec(["rm", self.tmp_key_location])

        if (
            success and
            self.replication_process.encryption.key_location == "$TrueNAS" and
            self.replication_process.encryption.key_format != KeyFormat.PASSPHRASE
        ):
            self.shell.exec(["midclt", "call", "pool.dataset.insert_or_update_encrypted_record", json.dumps({
                "name": self.replication_process.target_dataset,
                "encryption_key": self.replication_process.encryption.key,
                "key_format": self.replication_process.encryption.key_format.value.upper(),
            })])
