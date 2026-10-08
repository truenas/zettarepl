# -*- coding=utf-8 -*-
from __future__ import annotations

import logging
import queue
import signal
import threading
from typing import Any

from dateutil.tz import tzlocal

from zettarepl.definition.definition import (
    Definition,
    DefinitionError,
    PeriodicSnapshotTaskDefinitionError,
    ReplicationTaskDefinitionError,
)
from zettarepl.observer import (
    ObserverMessage,
    PeriodicSnapshotTaskError,
    PeriodicSnapshotTaskStart,
    PeriodicSnapshotTaskStartResponse,
    PeriodicSnapshotTaskSuccess,
    ReplicationTaskError,
)
from zettarepl.scheduler.clock import Clock
from zettarepl.scheduler.scheduler import Scheduler
from zettarepl.scheduler.tz_clock import TzClock
from zettarepl.transport.local import LocalShell
from zettarepl.utils.logging import (
    LOG_FORMAT,
    LOG_TIME_FORMAT,
    LongStringsFilter,
    ReplicationTaskLoggingLevelFilter,
)
from zettarepl.zettarepl import Zettarepl

from .client import MiddlewareClient
from .log_handler import ReplicationTaskLogHandler

logger = logging.getLogger(__name__)

__all__ = ["Daemon"]


def encode_definition_errors(errors: list[DefinitionError]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for error in errors:
        if isinstance(error, PeriodicSnapshotTaskDefinitionError):
            result.append({"type": "periodic_snapshot_task", "task_id": error.task_id, "error": str(error)})
        elif isinstance(error, ReplicationTaskDefinitionError):
            result.append({"type": "replication_task", "task_id": error.task_id, "error": str(error)})
        else:
            result.append({"type": "definition", "task_id": None, "error": str(error)})

    return result


class Daemon:
    def __init__(self, default_logging_level: int) -> None:
        self.default_logging_level = default_logging_level

        self.stop_event = threading.Event()

        self.client = MiddlewareClient(self._on_command)
        self.commands: queue.Queue[dict[str, Any]] = queue.Queue()

        self.clock = Clock()
        tz_clock = TzClock(tzlocal(), self.clock.now)
        self.zettarepl = Zettarepl(Scheduler(self.clock, tz_clock), LocalShell(), middleware_client=self.client)
        self.zettarepl.set_observer(self._observer)

    def run(self) -> None:
        self._setup_logging()

        signal.signal(signal.SIGTERM, self._signal)
        signal.signal(signal.SIGINT, self._signal)

        threading.Thread(name="zr_commands", target=self._process_commands, daemon=True).start()
        self._reload()
        self._notify_status()

        while not self.stop_event.is_set():
            try:
                self.zettarepl.run()
            except Exception:
                logger.error("Unhandled exception", exc_info=True)
                self.stop_event.wait(10)

    def _setup_logging(self) -> None:
        handler = ReplicationTaskLogHandler(self.client)
        handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_TIME_FORMAT))
        handler.addFilter(LongStringsFilter())
        handler.addFilter(ReplicationTaskLoggingLevelFilter(self.default_logging_level))
        logging.getLogger("zettarepl").addHandler(handler)

    def _signal(self, signum: int, frame: Any) -> None:
        logger.info("Received signal %d, shutting down", signum)
        self.stop_event.set()
        self.clock.stop()

    def _on_command(self, command: dict[str, Any]) -> None:
        self.commands.put(command)

    def _process_commands(self) -> None:
        while not self.stop_event.is_set():
            try:
                command = self.commands.get(timeout=1)
            except queue.Empty:
                continue

            try:
                self._process_command(command)
            except Exception:
                logger.error("Unhandled exception processing command %r", command, exc_info=True)

    def _process_command(self, command: dict[str, Any]) -> None:
        match command.get("command"):
            case "reload":
                self._reload()
            case "run_task":
                self._run_task(command["class_name"], command["task_id"])
            case "notify_status":
                self._notify_status()
            case other:
                logger.warning("Unknown command %r", other)

    def _reload(self) -> None:
        data = self.client.call("zettarepl.get_definition")

        definition = Definition.from_data(data["definition"], raise_on_error=False)
        self.zettarepl.set_config(definition.max_parallel_replication_tasks, definition.timezone)
        self.zettarepl.set_tasks(definition.tasks)

        self.client.call("zettarepl.notify_definition_read", {
            "errors": encode_definition_errors(definition.errors),
        })

    def _run_task(self, class_name: str, task_id: str) -> None:
        for task in self.zettarepl.tasks:
            if task.__class__.__name__ == class_name and task.id == task_id:
                logger.debug("Running task %r", task)
                self.zettarepl.scheduler.interrupt([task])
                return

        logger.warning("Task %s(%r) not found", class_name, task_id)
        if class_name == "PeriodicSnapshotTask":
            self.client.notify(PeriodicSnapshotTaskError(task_id, "Task not found"))
        if class_name == "ReplicationTask":
            self.client.notify(ReplicationTaskError(task_id, "Task not found"))

    def _notify_status(self) -> None:
        with self.zettarepl.tasks_lock:
            running = [task.id for task in self.zettarepl.running_tasks]
            pending = [task.id for _, task in self.zettarepl.pending_tasks]

        self.client.call("zettarepl.notify_status", {"running": running, "pending": pending})

    def _observer(self, message: ObserverMessage) -> Any:
        if isinstance(message, PeriodicSnapshotTaskStart):
            return self._periodic_snapshot_task_start(message)

        self.client.notify(message)

        if isinstance(message, (PeriodicSnapshotTaskSuccess, PeriodicSnapshotTaskError)):
            self._periodic_snapshot_task_end(message.task_id)

        return None

    def _periodic_snapshot_task_start(self, message: PeriodicSnapshotTaskStart) -> Any:
        self.client.flush()

        result = self.client.call("zettarepl.periodic_snapshot_task_start", message.task_id, timeout=None)
        return PeriodicSnapshotTaskStartResponse(result["properties"])

    def _periodic_snapshot_task_end(self, task_id: str) -> None:
        self.client.call("zettarepl.periodic_snapshot_task_end", task_id, timeout=None)
