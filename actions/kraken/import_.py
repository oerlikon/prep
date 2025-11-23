import os
from pathlib import Path

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
            sym, err = self._parse_arg(arg)
            if err is not None:
                return 1, err
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
    def _parse_arg(arg: str) -> tuple[str, Exception | None]:
        try:
            prefix, sym, _ = Path(arg).stem.split(".", maxsplit=2)
            if prefix == "kraken":
                return sym, None
            return "", ValueError(f"unexpected: {arg}")
        except ValueError as err:
            return "", err

    @staticmethod
    def _process_arg(arg: str, symbol: Symbol, store: Store) -> Exception | None:
        try:
            reader = pl.read_csv_batched(
                arg,
                has_header=False,
                low_memory=True,
                infer_schema_length=0,
                columns=list(range(6)),
                new_columns=["ts", "price", "b", "s", "m", "l"],
                batch_size=10000,
                rechunk=False,
            )
        except OSError as err:
            return err
        except Exception as err:
            return err

        acc_df: pl.DataFrame | None = None
        acc_y: int | None = None
        acc_m: int | None = None

        while True:
            batches = reader.next_batches(1)
            if batches is None:
                break
            try:
                batch = (
                    batches[0]
                    .with_columns(pl.col("ts").str.to_datetime(time_zone="UTC").alias("dt"))
                    .with_columns(pl.col("price").cast(pl.Float32).alias("price_value"))
                )
            except Exception as err:
                return err

            try:
                times = batch["dt"].to_list()
            except Exception as err:
                return err

            if not times:
                continue

            start_idx, n_rows = 0, len(times)

            while start_idx < n_rows:
                y = times[start_idx].year
                m = times[start_idx].month

                idx = start_idx
                while idx < n_rows and times[idx].year == y and times[idx].month == m:
                    idx += 1

                part = batch.slice(start_idx, idx - start_idx)

                if acc_df is None:
                    acc_df, acc_y, acc_m = part, y, m
                elif acc_y == y and acc_m == m:
                    try:
                        acc_df = acc_df.vstack(part)
                    except Exception as err:
                        return err
                else:
                    err_ = Import._process_month(acc_df, symbol, store)
                    if err_ is not None:
                        return err_
                    acc_df, acc_y, acc_m = part, y, m

                start_idx = idx

        if acc_df is not None:
            err_ = Import._process_month(acc_df, symbol, store)
            if err_ is not None:
                return err_

        return None

    @staticmethod
    def _process_month(
        df: pl.DataFrame,
        symbol: Symbol,
        store: Store,
    ) -> Exception | None:
        df = (
            df.group_by_dynamic(index_column="dt", every="23s", closed="left")
            .agg(
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
                pl.col("b").cast(pl.Float64).sum().alias("b_value"),
                pl.col("s").cast(pl.Float64).sum().alias("s_value"),
                pl.col("m").cast(pl.Float64).sum().alias("m_value"),
                pl.col("l").cast(pl.Float64).sum().alias("l_value"),
                (pl.col("b") != "0").cast(pl.UInt32).sum().alias("b_count"),
                (pl.col("s") != "0").cast(pl.UInt32).sum().alias("s_count"),
                (pl.col("m") != "0").cast(pl.UInt32).sum().alias("m_count"),
                (pl.col("l") != "0").cast(pl.UInt32).sum().alias("l_count"),
            )
            .with_columns(
                pl.when(pl.col("b_value") == pl.col("b_value").cast(pl.Int64).cast(pl.Float64))
                .then(pl.col("b_value").cast(pl.Int64).cast(pl.Utf8))
                .otherwise(pl.col("b_value").cast(pl.Utf8))
                .alias("b_value"),
                pl.when(pl.col("s_value") == pl.col("s_value").cast(pl.Int64).cast(pl.Float64))
                .then(pl.col("s_value").cast(pl.Int64).cast(pl.Utf8))
                .otherwise(pl.col("s_value").cast(pl.Utf8))
                .alias("s_value"),
                pl.when(pl.col("m_value") == pl.col("m_value").cast(pl.Int64).cast(pl.Float64))
                .then(pl.col("m_value").cast(pl.Int64).cast(pl.Utf8))
                .otherwise(pl.col("m_value").cast(pl.Utf8))
                .alias("m_value"),
                pl.when(pl.col("l_value") == pl.col("l_value").cast(pl.Int64).cast(pl.Float64))
                .then(pl.col("l_value").cast(pl.Int64).cast(pl.Utf8))
                .otherwise(pl.col("l_value").cast(pl.Utf8))
                .alias("l_value"),
            )
        )
        err = store.put(Block(symbol.name, symbol.market, df.item(1, "dt"), df))
        if err is not None:
            return err
        return None
