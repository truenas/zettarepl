# -*- coding=utf-8 -*-
from unittest.mock import Mock

from zettarepl.snapshot.task.nonintersecting_sets import tasks_intersect


def test__tasks_intersect__same_dataset():
    t1 = Mock(dataset="data/work")
    t2 = Mock(dataset="data/work")
    assert tasks_intersect(t1, t2)


def test__tasks_intersect__different_dataset_1():
    t1 = Mock(dataset="data/work")
    t2 = Mock(dataset="data/windows")
    assert not tasks_intersect(t1, t2)


def test__tasks_intersect__different_dataset_2():
    t1 = Mock(dataset="data/work/etc")
    t2 = Mock(dataset="data/workaholics")
    assert not tasks_intersect(t1, t2)


def test__tasks_intersect__common_parent():
    t1 = Mock(dataset="data/a")
    t2 = Mock(dataset="data/b")
    assert not tasks_intersect(t1, t2)


def test__tasks_intersect__longest_is_recursive_1():
    t1 = Mock(dataset="data/work/python", recursive=True)
    t2 = Mock(dataset="data/work", recursive=False)
    assert not tasks_intersect(t1, t2)


def test__tasks_intersect__longest_is_recursive_2():
    t1 = Mock(dataset="data/work", recursive=False)
    t2 = Mock(dataset="data/work/python", recursive=True)
    assert not tasks_intersect(t1, t2)


def test__tasks_intersect__shortest_is_recursive_1():
    t1 = Mock(dataset="data/work/python", recursive=False)
    t2 = Mock(dataset="data/work", recursive=True)
    assert tasks_intersect(t1, t2)


def test__tasks_intersect__shortest_is_recursive_2():
    t1 = Mock(dataset="data/work", recursive=True)
    t2 = Mock(dataset="data/work/python", recursive=False)
    assert tasks_intersect(t1, t2)
