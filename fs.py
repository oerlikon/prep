import datetime
import os
from dataclasses import dataclass
from pathlib import Path

import polars as pl


@dataclass
class Block:
    symbol: str
    market: str | None
    start: datetime.date
    records: pl.DataFrame

    def __post_init__(self) -> None:
        assert self.records.width > 0
        assert self.records.dtypes[0] == pl.Datetime


class Store:
    def __init__(self, path: str | os.PathLike[str]):
        self._path = Path(path)

    def put(self, block: Block) -> Exception | None:
        df = block.records.clone()

        def ts(dt: datetime.datetime) -> str:
            if dt.tzinfo is None or dt.tzinfo is datetime.timezone.utc or dt.tzname() in ("GMT", "UTC"):
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            return dt.isoformat("T", "seconds")

        if block.market:
            symbol_value = f"{block.market}:{block.symbol}"
        else:
            symbol_value = block.symbol

        df = df.with_columns(
            pl.lit(symbol_value).alias("Symbol"),
            pl.col(df.columns[0]).map_elements(ts, return_dtype=pl.Utf8).alias(df.columns[0]),
        )
        df = df.select(["Symbol", *[c for c in df.columns if c != "Symbol"]])
        return self.__store(block, df)

    def __store(self, block: Block, df: pl.DataFrame) -> Exception | None:
        try:
            csv = df.write_csv(None, include_header=False, line_terminator="\n").encode()
            path = self._path / f"{block.start.year}" / f"{block.start.month:02d}"
            path.mkdir(mode=0o755, parents=True, exist_ok=True)
            path = path / self.__make_filename(block.symbol, block.market, block.start)
            if not path.exists() or csv != path.read_bytes():
                path.write_bytes(csv)
            return None
        except OSError as err:
            return err

    @staticmethod
    def __make_filename(symbol: str, market: str | None, start: datetime.date) -> str:
        if market is not None:
            return f"{market.lower()}.{symbol.lower()}.{start.strftime('%Y%m')}.csv"
        return f"{symbol.lower()}.{start.strftime('%Y%m')}.csv"
