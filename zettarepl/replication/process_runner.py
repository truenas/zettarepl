# -*- coding=utf-8 -*-
import logging
import threading

from .error import StuckReplicationError

logger = logging.getLogger(__name__)

__all__ = ["ReplicationProcessRunner"]


class ReplicationProcessRunner:
    def __init__(self, replication_process, monitor):
        self.replication_process = replication_process
        self.monitor = monitor

        self.event = threading.Event()
        self.process_exception = None
        self.process_stuck = False

    def run(self):
        self.replication_process.run()

        threading.Thread(daemon=True, name=f"{threading.current_thread().name}.process",
                         target=self._wait_process).start()
        threading.Thread(daemon=True, name=f"{threading.current_thread().name}.monitor",
                         target=self._run_monitor).start()

        self.event.wait()  # Wait for at least one of the threads to finish (`finally` block in one will stop the other)
        if self.process_stuck:
            raise StuckReplicationError()
        if self.process_exception:
            raise self.process_exception

    def _wait_process(self):
        try:
            return self.replication_process.wait()
        except Exception as e:
            self.process_exception = e
        finally:
            self.event.set()
            self.monitor.stop()

    def _run_monitor(self):
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
