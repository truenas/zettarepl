# -*- coding=utf-8 -*-
from collections.abc import Callable, Iterable
import itertools
import logging

logger = logging.getLogger(__name__)

__all__ = ["bisect", "bisect_by_class", "select_by_class", "sortedgroupby"]


def bisect[T](condition: Callable[[T], bool], iterable: Iterable[T]) -> tuple[list[T], list[T]]:
    a: list[T] = []
    b: list[T] = []
    for val in iterable:
        if condition(val):
            a.append(val)
        else:
            b.append(val)

    return a, b


def bisect_by_class[T](klass: type[T], iterable: Iterable[object]) -> tuple[list[T], list[object]]:
    return bisect(lambda v: isinstance(v, klass), iterable)  # type: ignore[return-value]


def select_by_class[T](klass: type[T], iterable: Iterable[object]) -> list[T]:
    return list(filter(lambda v: isinstance(v, klass), iterable))  # type: ignore[arg-type]


def sortedgroupby[T, K](iterable: Iterable[T], key: Callable[[T], K],
                        comparable: bool = True) -> list[tuple[K, list[T]]]:
    return [
        (a, list(b))
        for a, b in itertools.groupby(
            sorted(iterable, key=key if comparable else lambda v: hash(key(v))),  # type: ignore
            key=key
        )
    ]
