# -*- coding=utf-8 -*-
import logging
import threading

from zettarepl.transport.interface import ReplicationProcess

from .error import StuckReplicationError
from .monitor import ReplicationMonitor

logger = logging.getLogger(__name__)

__all__ = ["ReplicationProcessRunner"]


class ReplicationProcessRunner:
    def __init__(self, replication_process: ReplicationProcess, monitor: ReplicationMonitor) -> None:
        self.replication_process = replication_process
        self.monitor = monitor

        self.event = threading.Event()
        self.process_exception: Exception | None = None
        self.process_stuck: bool = False

    def run(self) -> None:
        self.replication_process.run()

        threading.Thread(daemon=True, name=f"{threading.current_thread().name}.process",
                         target=self._wait_process).start()
        threading.Thread(daemon=True, name=f"{threading.current_thread().name}.monitor",
                         target=self._run_monitor).start()

        self.event.wait()  # Wait for at least one of the threads to finish (`finally` block in one will stop the other)
        if self.process_stuck:
            raise StuckReplicationError("Replication has stuck")
        if self.process_exception:
            raise self.process_exception

    def _wait_process(self) -> None:
        try:
            self.replication_process.wait()
        except Exception as e:
            self.process_exception = e
        finally:
            self.event.set()
            self.monitor.stop()

    def _run_monitor(self) -> None:
        try:
            self.process_stuck = not self.monitor.run()
            if self.process_stuck:
                logger.warning("Stopping stuck replication process")
                self.replication_process.stop()
        except Exception:
            logger.error("Unhandled exception in monitor", exc_info=True)
        finally:
            self.event.set()
            self.replication_process.stop()
