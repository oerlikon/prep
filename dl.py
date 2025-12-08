import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from common import Symbol
from util import parse_ts, tss


@dataclass
class Record:
    ts: datetime  # timestamp
    p: str  # price
    b: str  # buy volume
    s: str  # sell volume
    m: str  # market volume
    l: str  # limit volume
    id: int  # trade id


def filename(path: str | os.PathLike[str], symbol: Symbol) -> Path:
    assert symbol.market is not None
    return Path(path) / f"{symbol.market.lower()}.{symbol.name.lower()}.trades.csv"


def lasts(path: str | os.PathLike[str], symbol: Symbol) -> tuple[float, int, Exception | None]:
    dl = filename(path, symbol)

    if dl.exists():
        try:
            with dl.open("rb") as f:
                f.seek(0, 2)
                size = f.tell()
                if size == 0:
                    return 0, 0, None
                start = max(0, size - 4096)
                f.seek(start)
                lines = f.read(size - start).splitlines()
                if not lines:
                    return 0, 0, None
                for b in reversed(lines):
                    fields = b.strip().decode("utf-8").split(",")
                    if len(fields) < 7:
                        return 0, 0, ValueError(f"unexpected {fields}")
                    try:
                        dt, last_id = parse_ts(fields[0]), int(fields[6])
                    except ValueError as err:
                        return 0, 0, err
                    return dt.timestamp(), last_id, None
        except OSError as err:
            return 0, 0, err

    return 0, 0, None


def append(path: str | os.PathLike[str], symbol: Symbol, trades: list[Record]) -> Exception | None:
    dl = filename(path, symbol)

    if not dl.exists():
        try:
            Path(path).mkdir(mode=0o755, parents=True, exist_ok=True)
            dl.touch(mode=0o644)
        except OSError as err:
            return err

    try:
        with dl.open("a", newline="\n", encoding="utf-8") as f:
            for rec in trades:
                f.write(",".join((tss(rec.ts), rec.p, rec.b, rec.s, rec.m, rec.l, str(rec.id))) + "\n")
    except OSError as err:
        return err

    return None


def tails(
    path: str | os.PathLike[str],
    symbol: Symbol,
    start: datetime,
) -> tuple[list[Record], Exception | None]:
    """
    Read trades for `symbol` from a csv file under `path`, starting at `start`.
    """
    dl = filename(path, symbol)

    if not dl.exists():
        return [], FileNotFoundError(f"file not found: {dl}")

    try:
        with dl.open("rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size == 0:
                return [], None

            prestart = start - timedelta(days=1)

            left, right, offset, block_size = 0, size, 0, 4096

            while right - left > block_size:
                offset = (right + left) // 2
                f.seek(offset)
                data = f.read(min(block_size, right - offset))
                assert data

                lines = data.splitlines()
                assert lines

                if offset > 0:
                    assert len(lines) > 1
                    lines = lines[1:]

                ts: datetime | None = None
                for raw in lines:
                    fields = raw.strip().decode("utf-8").split(",")
                    assert fields
                    try:
                        ts = parse_ts(fields[0])
                    except ValueError as err:
                        return [], err
                    break
                assert ts is not None

                if prestart <= ts < start:
                    break

                if start < ts:
                    left, right, offset = left, offset, left
                else:
                    left, right = offset, right

            records: list[Record] = []

            f.seek(offset)
            if offset > 0:
                f.readline()

            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue

                fields = raw.decode("utf-8").split(",")
                if len(fields) < 7:
                    return [], ValueError(f"unexpected {fields}")

                try:
                    ts = parse_ts(fields[0])
                except ValueError as err:
                    return [], err

                if ts < start:
                    continue

                try:
                    rec = Record(
                        ts,
                        fields[1],
                        fields[2],
                        fields[3],
                        fields[4],
                        fields[5],
                        int(fields[6]),
                    )
                except (ValueError, TypeError) as err:
                    return [], err

                records.append(rec)

            return records[:], None

    except OSError as err:
        return [], err


def extend(a: list[Record], b: list[Record]) -> list[Record]:
    if not a:
        return b
    last_id = a[-1].id
    for i, rec in enumerate(b):
        if rec.id > last_id:
            a.extend(b[i:])
            return a
    return a
