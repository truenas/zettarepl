# -*- coding=utf-8 -*-
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

__all__ = ["re_search_to"]


def re_search_to(m: dict[int | str, str], *args: Any, **kwargs: Any) -> bool:
    result = re.search(*args, **kwargs)
    if result:
        m[0] = result.group(0)
        m.update({i + 1: v for i, v in enumerate(result.groups())})
        m.update(result.groupdict())  # type: ignore[arg-type]
        return True
    else:
        return False
