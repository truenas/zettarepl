# -*- coding=utf-8 -*-
import itertools
import logging

logger = logging.getLogger(__name__)

__all__ = ["bisect", "bisect_by_class", "select_by_class", "sortedgroupby"]


def bisect(condition, iterable):
    a = []
    b = []
    for val in iterable:
        if condition(val):
            a.append(val)
        else:
            b.append(val)

    return a, b


def bisect_by_class(klass, iterable):
    return bisect(lambda v: isinstance(v, klass), iterable)


def select_by_class(klass, iterable):
    return list(filter(lambda v: isinstance(v, klass), iterable))


def sortedgroupby(iterable, key):
    return [(a, list(b)) for a, b in itertools.groupby(sorted(iterable, key=key), key=key)]
