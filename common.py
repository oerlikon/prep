import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@dataclass
class Symbol:
    name: str
    market: str | None = None
    time: str | None = None
    start: datetime | None = None


class Cmd(Protocol):
    def run(self, *args, **kwargs) -> tuple[int | None, str | Exception | None]: ...


@dataclass
class Action(Cmd):
    name: str
    using: str | None = None
    fn: Callable[..., tuple[int | None, str | Exception | None]] | None = None

    def run(self, *args, **kwargs) -> tuple[int | None, str | Exception | None]:
        if self.fn is not None:
            return self.fn(self, *args, **kwargs)
        return None, None


@lru_cache
def tz(key: str) -> ZoneInfo | None:
    try:
        return ZoneInfo(key)
    except ZoneInfoNotFoundError:
        return None


def ts(dt: datetime) -> str:
    if dt.tzinfo is None or dt.tzinfo is timezone.utc or dt.tzname() in ("GMT", "UTC"):
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt.isoformat("T", "seconds")


def zx(s: str | int | float) -> str:
    if isinstance(s, int):
        return str(s)
    if isinstance(s, float):
        s = f"{s:.18g}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
        if s:
            return s
        return "0"
    return s


def p(*what, **mods) -> None:
    print(*what, **mods, file=sys.stderr, flush=True)


if __name__ == "__main__":
    import tzdata  # type: ignore

    _ = dir(tzdata)
