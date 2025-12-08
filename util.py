import re
import sys
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@lru_cache(9)
def parse_timedelta(s: str) -> timedelta:
    sec = i = 0
    for n, u in re.findall(r"(\d+)(s|m|h)", s):
        sec, i = sec + int(n) * {"s": 1, "m": 60, "h": 3600}[u], i + len(n) + len(u)
    if i == 0 or i != len(s):
        raise ValueError(f"invalid duration {s!r}")
    return timedelta(seconds=sec)


@lru_cache
def tz(key: str) -> ZoneInfo | None:
    try:
        return ZoneInfo(key)
    except ZoneInfoNotFoundError:
        return None


def tss(dt: datetime) -> str:
    if dt.tzinfo is None or dt.tzinfo is timezone.utc or dt.tzname() in ("GMT", "UTC"):
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt.isoformat("T", "seconds")


def tsp(dt: datetime) -> str:
    if dt.tzinfo is None or dt.tzinfo is timezone.utc or dt.tzname() in ("GMT", "UTC"):
        return dt.strftime(f"%Y-%m-%d %H:%M:%S")
    return dt.isoformat(" ", "seconds")


def parse_ts(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


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
