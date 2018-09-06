# -*- coding=utf-8 -*-
from collections import namedtuple
import logging

logger = logging.getLogger(__name__)

__all__ = ["Scheduler"]

SchedulerResult = namedtuple("SchedulerResult", ["datetime", "tasks"])


class Scheduler:
    def __init__(self, clock, tz_clock):
        self.clock = clock
        self.tz_clock = tz_clock

        self.tasks = []

    def set_tasks(self, tasks):
        self.tasks = tasks

    def schedule(self):
        while True:
            utcnow = self.clock.tick()
            if utcnow is None:
                break

            now = self.tz_clock.tick(utcnow)

            tasks = []
            for task in self.tasks.copy():
                if task.schedule.should_run(now.datetime):
                    tasks.append(task)

            yield SchedulerResult(now, tasks)
