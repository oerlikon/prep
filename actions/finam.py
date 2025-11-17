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
        symbol, sep, _ = Path(arg).stem.partition("_M1_")
        if sep != "_M1_":
            return "", ValueError(f"unexpected: {arg}")
        return symbol, None

    @staticmethod
    def _process_arg(arg: str, symbol: Symbol, store: Store) -> Exception | None:
        try:
            reader = pl.read_csv_batched(
                arg,
                has_header=False,
                skip_rows=1,
                separator="\t",
                low_memory=True,
                infer_schema_length=0,
                columns=list(range(8)),
                new_columns=["date", "time", "open", "high", "low", "close", "ticks", "volume"],
                batch_size=1000,
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
                assert symbol.time is not None
                batch = (
                    batches[0]
                    .with_columns(
                        pl.concat_str(["date", "time"], separator=" ")
                        .str.strptime(
                            pl.Datetime(time_zone=symbol.time),
                            "%Y.%m.%d %H:%M:%S",
                        )
                        .alias("dt")
                    )
                    .select("dt", "open", "high", "low", "close", "volume", "ticks")
                )
            except Exception as err:
                return err

            if (batch["volume"] == "0").any():
                batch = batch.with_columns(pl.col("ticks").alias("volume"))

            assert (batch["volume"] != "0").all()

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
                    err_ = store.put(Block(symbol.name, symbol.market, acc_df.item(1, "dt"), acc_df))
                    if err_ is not None:
                        return err_
                    acc_df, acc_y, acc_m = part, y, m

                start_idx = idx

        if acc_df is not None:
            err_ = store.put(Block(symbol.name, symbol.market, acc_df.item(1, "dt"), acc_df))
            if err_ is not None:
                return err_

        return None


def get_cmd(name: str) -> tuple[Cmd | None, str | None]:
    match name:
        case "import":
            return Import(), None
        case _:
            return None, f"command {name} not found in module {__name__}"
