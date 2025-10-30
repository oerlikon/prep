import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

import requests

from common import Cmd, Symbol, p, ts, zx


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
        case _:
            return None, f"command {name} not found in module {__name__}"
