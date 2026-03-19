# -*- coding=utf-8 -*-
from __future__ import annotations

from collections import namedtuple
from collections.abc import Generator
import logging
import threading

from zettarepl.scheduler.clock import Clock
from zettarepl.scheduler.tz_clock import TzClock
from zettarepl.task import Task

logger = logging.getLogger(__name__)

__all__ = ["Scheduler"]

SchedulerResult = namedtuple("SchedulerResult", ["datetime", "tasks", "interrupted"])


class Scheduler:
    def __init__(self, clock: Clock, tz_clock: TzClock) -> None:
        self.clock: Clock = clock
        self.tz_clock: TzClock = tz_clock

        self.tasks: list[Task] = []

        self.interrupt_lock: threading.Lock = threading.Lock()
        self.interrupt_tasks: list[Task] = []

    def set_tasks(self, tasks: list[Task]) -> None:
        self.tasks = tasks

    def schedule(self) -> Generator[SchedulerResult, None, None]:
        while True:
            utcnow = self.clock.tick()
            if utcnow is None:
                break

            now = self.tz_clock.tick(utcnow)

            tasks = []
            interrupted = False
            with self.interrupt_lock:
                if self.interrupt_tasks:
                    tasks = self.interrupt_tasks
                    interrupted = True
                    self.interrupt_tasks = []

            if not interrupted:
                # Only add these tasks if not interrupted. The interruption event will always arrive after the natural
                # `tick()` event.
                for task in self.tasks.copy():
                    if task.schedule.should_run(now.datetime):
                        tasks.append(task)

            yield SchedulerResult(now, tasks, interrupted)

    def interrupt(self, tasks: list[Task]) -> None:
        with self.interrupt_lock:
            self.interrupt_tasks = tasks

        self.clock.interrupt()
