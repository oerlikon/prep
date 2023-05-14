import datetime
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from common import tzinfo


@dataclass
class Block:
    symbol: str
    market: str | None
    start: datetime.date
    records: pd.DataFrame

    def __post_init__(self) -> None:
        assert len(self.records.columns) > 0
        assert pd.api.types.is_datetime64_any_dtype(self.records.iloc[:, 0])


class Store:
    def __init__(self, path: str | os.PathLike[str]):
        self._path = Path(path)
        self._utc = tzinfo("UTC")

    def put(self, block: Block) -> str | Exception | None:
        df = block.records.copy()

        def ts(dt: datetime.datetime) -> str:
            if dt.tzinfo is None or dt.tzinfo == self._utc:
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            return dt.isoformat("T", "seconds")

        df[df.columns[0]] = df.iloc[:, 0].map(ts)
        if block.market:
            df.insert(0, "Symbol", f"{block.symbol}:{block.market}")
        else:
            df.insert(0, "Symbol", block.symbol)
        return self.__store(block, df)

    def __store(self, block: Block, df: pd.DataFrame) -> str | Exception | None:
        csv = df.to_csv(header=False, index=False, lineterminator="\n").encode()
        path = self._path / f"{block.start.year}" / f"{block.start.month:02d}" / f"{block.start.day:02d}"
        path.mkdir(mode=0o755, parents=True, exist_ok=True)
        path = path / self.__make_filename(block.symbol, block.market, block.start)
        if not path.exists():
            path.write_bytes(csv)
            return None
        csv_sha1 = hashlib.sha1(csv).hexdigest()
        file_sha1 = hashlib.sha1(path.read_bytes()).hexdigest()
        if csv_sha1 == file_sha1:
            return None
        path.write_bytes(csv)
        return None

    @staticmethod
    def __make_filename(symbol: str, market: str | None, start: datetime.date) -> str:
        if market is not None:
            return f"{market.lower()}.{symbol.lower()}.{start.strftime('%Y%m%d')}.csv"
        return f"{symbol.lower()}.{start.strftime('%Y%m%d')}.csv"
