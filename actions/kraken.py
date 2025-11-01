import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

import polars as pl
import requests

from common import Cmd, Symbol, p, ts, zx
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
        try:
            market, sym, _ = Path(arg).stem.split(".", maxsplit=2)
            if market == "kraken":
                return sym
            return ""
        except ValueError:
            return ""

    @staticmethod
    def _process_arg(arg: str, symbol: Symbol, store: Store) -> str | Exception | None:
        try:
            reader = pl.read_csv_batched(
                arg,
                has_header=False,
                low_memory=True,
                infer_schema_length=0,
                columns=[0, 1, 2, 3, 4, 5],
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


class Fetch(Cmd):
    def run(self, *args, **kwargs) -> tuple[int | None, str | Exception | None]:
        symbols: dict[str, Symbol] | None = kwargs.get("symbols")
        if not symbols:
            return None, None
        if not args:
            return 1, "dl path?"

        self._dl, self._client = Path(args[0]), Client()

        for symbol in symbols.values():
            assert symbol.market == "Kraken"

            err = self._fetch_symbol(symbol)
            if err is not None:
                return 2, err

        return None, None

    def _fetch_symbol(self, symbol: Symbol) -> str | Exception | None:
        assert symbol.start is not None

        p(f"Fetching {symbol.market}:{symbol.name}... ", end="")

        try:
            outpath = self._dl / f"kraken.{symbol.name.lower()}.trades.csv"
            if outpath.exists():
                start, last_id, err = self._parse_last_record(outpath)
                if err is not None:
                    p()
                    return err
                if start == 0:
                    start = symbol.start.timestamp()
            else:
                self._dl.mkdir(mode=0o755, parents=True, exist_ok=True)
                with open(outpath, "w") as f:
                    pass
                os.chmod(outpath, 0o644)
                start, last_id = symbol.start.timestamp(), 0

            with open(outpath, "a") as outfile:
                for trades, err in self._client._fetch_trades(symbol.name.upper(), start, last_id):
                    if err is not None:
                        p()
                        return err
                    for trade in trades:
                        outfile.write(
                            ",".join(
                                [ts(datetime.fromtimestamp(trade[0], tz=timezone.utc))]
                                + [zx(s) for s in trade[1:-1]]
                                + [str(trade[-1])]
                            )
                            + "\n"
                        )
                    outfile.flush()

        except OSError as err:
            p()
            return err

        p("done.")

        return None

    @staticmethod
    def _parse_last_record(path: str | os.PathLike[str]) -> tuple[float, int, Exception | None]:
        with open(path, "rb") as f:
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
                    ds = fields[0]
                    if ds.endswith("Z"):
                        ds = ds[:-1] + "+00:00"
                    dt, last_id = datetime.fromisoformat(ds), int(fields[6])
                except ValueError as err:
                    return 0, 0, err
                return dt.timestamp(), last_id, None
        return 0, 0, None


TradeRecord = tuple[
    float,  # timestamp
    str,  # price
    str,  # buy volume
    str,  # sell volume
    str,  # market volume
    str,  # limit volume
    int,  # trade id
]


class Client:

    BASE_URL = "https://api.kraken.com/0/public/Trades"

    def __init__(self) -> None:
        self._s = requests.Session()
        self._s.headers.update({"User-Agent": "prep/1.0"})
        self._delay = 0.0

    def _sleep(self, err: Any | None = None):
        time.sleep(self._delay)
        if err is None:
            self._delay = 1
        else:
            self._delay = min(max(1, self._delay * 2), 5.0)

    def _get_trades_page(
        self,
        pair: str,
        since: str,
        timeout: float = 10.0,
        max_retries: int = 5,
    ) -> tuple[dict[str, Any], Exception | None]:
        last_err: Exception | None = None

        for _ in range(max_retries):
            self._sleep()
            try:
                resp = self._s.get(self.BASE_URL, params={"pair": pair, "since": since}, timeout=timeout)
            except requests.RequestException as err:
                self._sleep(err)
                last_err = err
                continue
            try:
                page = resp.json()
            except ValueError as err:
                self._sleep(err)
                last_err = err
                continue
            api_err = page.get("error")
            if api_err:
                return {}, RuntimeError(f"Kraken error: {api_err}")
            return page, None

        return {}, last_err

    def _fetch_trades(
        self,
        pair: str,
        start: float,
        last_id: int = 0,
    ) -> Generator[tuple[list[TradeRecord], Exception | None], None, None]:
        since, end = str(start), time.time()

        while True:
            page, err = self._get_trades_page(pair, since)
            if err is not None:
                yield [], err
                return

            sym, trades, last, err = self._parse_page(page)
            if err is not None:
                yield [], err
                return

            if not trades:
                return

            i, n = 0, len(trades)
            while i < n and trades[i][6] <= last_id:
                i += 1
            if i > 0:
                trades = trades[i:]

            if not trades:
                return

            since, last_ts, last_id = last, trades[-1][0], trades[-1][6]

            yield trades, None

            if end < last_ts:
                return

    def _parse_page(self, page: dict[str, Any]) -> tuple[str, list[TradeRecord], str, Exception | None]:
        result = page.get("result")
        if not isinstance(result, dict):
            return "", [], "", RuntimeError("no result?")

        keys, last = [k for k in result.keys() if k != "last"], result.get("last")
        if not keys or not isinstance(last, str):
            return "", [], "", RuntimeError("missing something?")

        sym = keys[0]
        rows = result.get(sym)
        if not isinstance(rows, list):
            return "", [], "", RuntimeError("no trades?")

        trades: list[TradeRecord] = []
        for row in rows:
            if (
                not isinstance(row, list)
                or len(row) < 7
                or not isinstance(row[0], str)  # price
                or not isinstance(row[1], str)  # volume
                or not isinstance(row[2], (int, float))  # timestamp
                or not isinstance(row[6], int)  # trade id
            ):
                return "", [], "", RuntimeError(f"unexpected {row}")

            trades.append(
                (
                    row[2],
                    row[0],
                    row[1] if row[3] == "b" else "0",
                    row[1] if row[3] == "s" else "0",
                    row[1] if row[4] == "m" else "0",
                    row[1] if row[4] == "l" else "0",
                    row[6],
                )
            )

        return sym, trades, last, None


def get_cmd(name: str) -> tuple[Cmd | None, str | None]:
    match name:
        case "fetch":
            return Fetch(), None
        case "import":
            return Import(), None
        case _:
            return None, f"command {name} not found in module {__name__}"
