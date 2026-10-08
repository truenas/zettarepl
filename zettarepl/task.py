from zettarepl.scheduler.cron import CronSchedule

__all__ = ["Task"]


class Task:
    id: str
    schedule: CronSchedule | None
