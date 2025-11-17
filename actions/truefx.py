import os
from pathlib import Path

import numpy as np
import polars as pl

from common import Cmd, Symbol, p
from fs import Block, Store


class Import(Cmd):
    def run(self, *args, **kwargs) -> tuple[int | None, str | Exception | None]:
        symbols: dict[str, Symbol] | None = kwargs.get("symbols")
        if not symbols:
            return None, None
        path: str | os.PathLike[str] | None = kwargs.get("path")
        path = path if path is not None else ""
        store = Store(path)
        for arg in args:
            sym = self._parse_arg(arg)
            if sym.lower() not in symbols:
                p(f"Skipping {arg}")
                continue
            p(f"Processing {arg}... ", end="")
            err = self._process_arg(arg, symbols[sym.lower()], store)
            if err is not None:
                p()
                return 2, err
            p("done.")
        return None, None

    @staticmethod
    def _parse_arg(arg: str) -> str:
        sym, _, _ = Path(arg).stem.partition("-")
        return sym

    @staticmethod
    def _process_arg(arg: str, symbol: Symbol, store: Store) -> str | Exception | None:
        try:
            df = pl.read_csv(
                arg,
                has_header=False,
                infer_schema=False,
                columns=(1, 2, 3),
                new_columns=("ts", "bid", "ask"),
            )
        except OSError as e:
            return e
        df = (
            df.with_columns(
                pl.col("ts")
                .str.to_datetime(format="%Y%m%d %H:%M:%S%.3f", time_zone=symbol.time)
                .alias("dt")
            )
            .sort("dt")
            .with_columns(pl.Series(np.random.default_rng(1).integers(0, 2, df.height)).alias("hit"))
            .with_columns(
                pl.when(pl.col("hit") == 0).then(pl.col("bid")).otherwise(pl.col("ask")).alias("price")
            )
            .with_columns(pl.col("price").cast(pl.Float32).alias("price_value"))
        )
        df = df.group_by_dynamic(index_column="dt", every="23s", closed="left").agg(
            pl.col("price").first().alias("open"),
            pl.col("price")
            .filter(pl.col("price_value") == pl.col("price_value").max())
            .first()
            .alias("high"),
            pl.col("price")
            .filter(pl.col("price_value") == pl.col("price_value").min())
            .first()
            .alias("low"),
            pl.col("price").last().alias("close"),
        )
        err = store.put(Block(symbol.name, symbol.market, df.item(1, "dt"), df))
        if err is not None:
            return err
        return None


def get_cmd(name: str) -> tuple[Cmd | None, str | None]:
    match name:
        case "import":
            return Import(), None
        case _:
            return None, f"command {name} not found in module {__name__}"
