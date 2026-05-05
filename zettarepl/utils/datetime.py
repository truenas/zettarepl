# -*- coding=utf-8 -*-
from datetime import datetime, timedelta
import logging
import re

logger = logging.getLogger(__name__)

__all__ = ["idealized_datetime", "parse_duration"]


def idealized_datetime(d: datetime) -> datetime:
    return d.replace(second=0, microsecond=0, tzinfo=None)


_DURATION_RE = re.compile(
    r"^P(?!$)"
    r"(?:(?P<weeks>\d+(?:\.\d+)?)W)?"
    r"(?:(?P<days>\d+(?:\.\d+)?)D)?"
    r"(?:T(?=.)"
    r"(?:(?P<hours>\d+(?:\.\d+)?)H)?"
    r"(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?"
    r")?$"
)


def parse_duration(s: str) -> timedelta:
    """Parse an ISO 8601 duration into a timedelta.

    Accepts weeks, days, hours, minutes, and seconds (e.g. P7W, P1DT12H30M).
    Year and month components are not supported since they cannot be
    represented as a fixed timedelta.
    """
    m = _DURATION_RE.fullmatch(s)
    if m is None:
        raise ValueError(f"Invalid ISO 8601 duration: {s!r}")
    return timedelta(**{k: float(v) for k, v in m.groupdict().items() if v is not None})
