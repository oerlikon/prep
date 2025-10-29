import datetime
import sys
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from typing import Protocol, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@dataclass
class Symbol:
    name: str
    market: str | None = None
    time: str | None = None
    start: datetime.datetime | None = None


class Cmd(Protocol):
    def run(self, *args, **kwargs) -> Tuple[int | None, str | Exception | None]: ...


@dataclass
class Action(Cmd):
    name: str
    using: str | None = None
    fn: Callable[..., Tuple[int | None, str | Exception | None]] | None = None

    def run(self, *args, **kwargs) -> Tuple[int | None, str | Exception | None]:
        if self.fn is not None:
            return self.fn(self, *args, **kwargs)
        return None, None


@lru_cache
def timezone(key: str) -> ZoneInfo | None:
    try:
        return ZoneInfo(key)
    except ZoneInfoNotFoundError:
        return None


def p(*what, **mods) -> None:
    print(*what, **mods, file=sys.stderr, flush=True)


if __name__ == "__main__":
    import tzdata  # type: ignore

    _ = dir(tzdata)
