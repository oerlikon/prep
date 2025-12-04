import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from common import Symbol
from util import parse_ts, ts, zx


@dataclass
class Record:
    ts: datetime  # timestamp
    p: str | int | float  # price
    b: str | int | float  # buy volume
    s: str | int | float  # sell volume
    m: str | int | float  # market volume
    l: str | int | float  # limit volume
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
                    b = b.strip()
                    if not b:
                        continue
                    fields = b.decode("utf-8", errors="replace").split(",")
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
                f.write(
                    ",".join(
                        (ts(rec.ts), zx(rec.p), zx(rec.b), zx(rec.s), zx(rec.m), zx(rec.l), str(rec.id))
                    )
                    + "\n"
                )
    except OSError as err:
        return err

    return None
