import os
import zipfile
from typing import IO, Any, Tuple

import polars as pl

from common import Cmd, Symbol, p
from fs import Block, Store


class Import(Cmd):
    def run(self, *args: str, **kwargs: Any) -> Tuple[int | None, str | Exception | None]:
        symbols: dict[str, Symbol] | None = kwargs.get("symbols")
        if not symbols:
            return None, None
        for symbol in symbols.values():
            if symbol.time is not None and symbol.time != "UTC":
                return 2, f"config: {symbol.name} time zone not UTC: {symbol.time}"
        path: str | os.PathLike[str] | None = kwargs.get("path")
        path = path if path is not None else ""
        store = Store(path)
        for arg in args:
            p(f"Processing {arg}... ", end="")
            err = self.__process_arg(arg, symbols, store)
            if isinstance(err, FileNotFoundError):
                p("file not found")
                return 1, None
            if err is not None:
                p()
                return 2, err
            p("done.")
        return None, None

    def __process_arg(self, arg: str, symbols: dict[str, Symbol], store: Store) -> str | Exception | None:
        try:
            with zipfile.ZipFile(arg, "r") as z:
                for filename in z.namelist():
                    sym, ok = self.__parse_filename(filename)
                    if not ok:
                        continue
                    if sym.lower() not in symbols:
                        continue
                    with z.open(filename, "r") as f:
                        err = self.__process_file(f, symbols[sym.lower()], store)
                        if err is not None:
                            return err
        except (OSError, zipfile.BadZipFile) as e:
            return e
        return None

    @staticmethod
    def __process_file(file: IO[bytes], symbol: Symbol, store: Store) -> str | Exception | None:
        df = pl.read_csv(
            file,
            has_header=False,
            infer_schema=False,
            columns=range(7),
            new_columns=("ts", "o", "h", "l", "c", "v", "t"),
        )
        start = int(symbol.start.timestamp()) if symbol.start is not None else 0
        df = (
            df.with_columns(pl.col("ts").cast(pl.Int64).alias("ut"))
            .filter(pl.col("ut") >= start)
            .with_columns(pl.from_epoch(pl.col("ut"), time_unit="s").alias("dt"))
            .filter(pl.col("v") != "0")
            .filter(pl.col("t") != "0")
            .select("dt", "o", "h", "l", "c", "v", "t")
        )
        for ts, gf in df.group_by_dynamic(index_column="dt", every="1mo", closed="left"):
            dt, pf = ts[0].date(), gf.to_pandas()  # type: ignore[attr-defined]
            err = store.put(Block(symbol.name, symbol.market, dt, pf))
            if err is not None:
                return err
        return None

    @staticmethod
    def __parse_filename(filename: str) -> Tuple[str, bool]:
        if not filename.endswith("_1.csv"):
            return "", False
        return filename.removesuffix("_1.csv"), True


def getcmd(name: str) -> Tuple[Cmd | None, str | None]:
    match name:
        case "import":
            return Import(), None
        case _:
            return None, f"command {name} not found in module {__name__}"
