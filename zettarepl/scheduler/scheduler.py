# -*- coding=utf-8 -*-
from collections import namedtuple
import logging
import threading

logger = logging.getLogger(__name__)

__all__ = ["Scheduler"]

SchedulerResult = namedtuple("SchedulerResult", ["datetime", "tasks", "interrupted"])


class Scheduler:
    def __init__(self, clock, tz_clock):
        self.clock = clock
        self.tz_clock = tz_clock

        self.tasks = []

        self.interrupt_lock = threading.Lock()
        self.interrupt_tasks = []

    def set_tasks(self, tasks):
        self.tasks = tasks

    def schedule(self):
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

    def interrupt(self, tasks):
        with self.interrupt_lock:
            self.interrupt_tasks = tasks
        self.clock.interrupt()
