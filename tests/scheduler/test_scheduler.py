from unittest.mock import Mock

from zettarepl.scheduler.scheduler import Scheduler


def test_multiple_interruptions():
    scheduler = Scheduler(Mock(), Mock())
    gen = scheduler.schedule()

    task1 = Mock()
    task2 = Mock()
    scheduler.interrupt([task1])
    scheduler.interrupt([task2])

    result = next(gen)
    assert result.tasks == [task1, task2]


def test_multiple_interruptions_with_the_same_task():
    scheduler = Scheduler(Mock(), Mock())
    gen = scheduler.schedule()

    task1 = Mock()
    scheduler.interrupt([task1])
    scheduler.interrupt([task1])

    result = next(gen)
    assert result.tasks == [task1]
