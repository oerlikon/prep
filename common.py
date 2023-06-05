import datetime
import sys
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Protocol, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@dataclass
class Symbol:
    name: str
    market: str | None = None
    time: str | None = None
    start: datetime.datetime | None = None


@dataclass
class Action:
    name: str
    using: str | None = None


class Cmd(Protocol):
    def run(self, *args: str, **kwargs: Any) -> Tuple[int | None, str | Exception | None]:
        ...


@lru_cache
def tzinfo(tzname: str) -> ZoneInfo | None:
    try:
        return ZoneInfo(tzname)
    except ZoneInfoNotFoundError:
        return None


def p(*what: Any, **mods: Any) -> None:
    print(*what, **mods, file=sys.stderr, flush=True)


if __name__ == "__main__":
    import tzdata  # type: ignore

    _ = dir(tzdata)
