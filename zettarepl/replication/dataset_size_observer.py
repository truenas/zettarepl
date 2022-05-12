# -*- coding=utf-8 -*-
import logging
import socket
import threading

import paramiko.ssh_exception

from zettarepl.transport.zfscli import get_property
from zettarepl.transport.zfscli.exception import DatasetDoesNotExistException

logger = logging.getLogger(__name__)

__all__ = ["DatasetSizeObserver"]


class DatasetSizeObserver:
    INTERVAL = 30

    def __init__(self, src_shell, dst_shell, src_dataset, dst_dataset, observer):
        self.src_shell = src_shell
        self.dst_shell = dst_shell
        self.src_dataset = src_dataset
        self.dst_dataset = dst_dataset
        self.observer = observer
        self.event = threading.Event()
        self.lock = threading.Lock()

    def __enter__(self):
        threading.Thread(
            daemon=True,
            name=f"{threading.current_thread().name}.dataset_size_observer",
            target=self._run,
        ).start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self.lock:
            self.event.set()

    def _run(self):
        self.event.wait(self.INTERVAL)

        while not self.event.is_set():
            try:
                self._run_once()
            except (socket.error, paramiko.ssh_exception.SSHException, OSError) as e:
                logger.error("Dataset size observer error: %r", e)
            except Exception:
                logger.error("Unhandled exception in dataset size observer", exc_info=True)

            self.event.wait(self.INTERVAL)

    def _run_once(self):
        src_used = get_property(self.src_shell, self.src_dataset, "used", int)

        try:
            dst_used = get_property(self.dst_shell, self.dst_dataset, "used", int)
        except DatasetDoesNotExistException:
            logger.info("Destination dataset %r on shell %r does not exist yet", self.dst_dataset, self.dst_shell)
            dst_used = 0

        with self.lock:
            if not self.event.is_set():
                self.observer(src_used, dst_used)
