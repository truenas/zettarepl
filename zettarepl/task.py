from zettarepl.scheduler.cron import CronSchedule

__all__ = ["Task"]


class Task:
    schedule: CronSchedule
