# -*- coding=utf-8 -*-
from collections import namedtuple
import logging
import queue
import threading

from .interface import AsyncExec, ExecException

logger = logging.getLogger(__name__)

__all__ = ["AsyncExecTee", "PrematureExit"]

DataEvent = namedtuple("DataEvent", ["data"])
DataDrainEvent = namedtuple("DataDrainEvent", [])
ExitEvent = namedtuple("ExitEvent", ["returncode"])
ExceptionEvent = namedtuple("ExceptionEvent", ["exception"])


class PrematureExit(Exception):
    def __init__(self, stdout):
        self.stdout = stdout


class AsyncExecTee(AsyncExec):
    def __init__(self, shell, args, encoding="utf8", stdout=None):
        assert stdout is None
        super().__init__(shell, args, encoding, stdout)

        self.queue = queue.Queue()
        self.returncode = None
        self.output = ""
        self.complete_event = threading.Event()

        self.async_exec = None

    def run(self):
        q = queue.Queue()

        self.async_exec = self.shell.exec_async(self.args, self.encoding, q)

        threading.Thread(daemon=True, name=f"{threading.current_thread().name}.async_exec_tee.read",
                         target=self._read, args=(q,)).start()
        threading.Thread(daemon=True, name=f"{threading.current_thread().name}.async_exec_tee.wait",
                         target=self._wait).start()

    def head(self, callback, timeout):
        while True:
            try:
                event = self.queue.get(timeout=timeout)
            except queue.Empty:
                self.async_exec.stop()
                raise TimeoutError("Timeout in head()") from None

            if isinstance(event, DataEvent):
                try:
                    result = callback(event.data)
                except Exception:
                    self.async_exec.stop()
                    raise

                if result is not None:
                    return result

                self.output += event.data

            if isinstance(event, ExitEvent):
                if event.returncode == 0:
                    raise PrematureExit(self.output)

                raise ExecException(event.returncode, self.output)

            if isinstance(event, ExceptionEvent):
                self.async_exec.stop()
                raise event.exception

    def wait(self, timeout=None):
        if timeout is not None:
            raise NotImplementedError("AsyncExecTee.wait with timeout is not implemented yet")

        data_drained = False
        exit_event = None

        while not (data_drained and exit_event is not None):
            event = self.queue.get()

            if isinstance(event, DataEvent):
                self.output += event.data

            if isinstance(event, DataDrainEvent):
                data_drained = True

            if isinstance(event, ExitEvent):
                exit_event = event

            if isinstance(event, ExceptionEvent):
                self.async_exec.stop()
                raise event.exception

        if exit_event.returncode == 0:
            self.logger.debug("AsyncExecTee success: %r", self.output)
            return self.output

        self.logger.debug("Error %r: %r", exit_event.returncode, self.output)
        raise ExecException(exit_event.returncode, self.output)

    def stop(self):
        self.async_exec.stop()

    def _read(self, q: queue.Queue):
        try:
            while True:
                data = q.get()
                if data is None:
                    break

                self.queue.put(DataEvent(data))
        finally:
            self.queue.put(DataDrainEvent())

    def _wait(self):
        try:
            try:
                self.async_exec.wait()
            except ExecException as e:
                self.queue.put(ExitEvent(e.returncode))
            else:
                self.queue.put(ExitEvent(0))
        except Exception as e:
            self.queue.put(ExceptionEvent(e))
