# -*- coding=utf-8 -*-
import logging
import threading

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
        wait_process_thread = threading.Thread(daemon=True, target=self._wait_process)
        wait_process_thread.start()
        wait_monitor_thread = threading.Thread(daemon=True, target=self._wait_monitor)
        wait_monitor_thread.start()

        self.event.wait()
        if self.process_stuck:
            raise Exception("Replication was stuck")
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

    def _wait_monitor(self):
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
