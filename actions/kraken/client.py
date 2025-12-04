import time
from datetime import datetime, timezone
from typing import Any, Generator

import requests

import dl


class Client:

    BASE_URL = "https://api.kraken.com/0/public/Trades"

    def __init__(self) -> None:
        self._s = requests.Session()
        self._s.headers.update({"User-Agent": "prep/1.0"})
        self._delay, self._n = 0, 0

    def fetch_trades(
        self,
        pair: str,
        start: float = 0,
        last_id: int = 0,
    ) -> Generator[tuple[list[dl.Record], Exception | None], None, None]:
        since, end = str(start) if start else "", datetime.now(tz=timezone.utc)

        while True:
            page, err = self._get_trades_page(pair, since)
            if err is not None:
                yield [], err
                return

            _, trades, last, err = self._parse_page(page)
            if err is not None:
                yield [], err
                return

            if not trades:
                return

            i, n = 0, len(trades)
            while i < n and trades[i].id <= last_id:
                i += 1
            if i > 0:
                trades = trades[i:]

            if not trades:
                return

            since, last_ts, last_id = last, trades[-1].ts, trades[-1].id

            yield trades, None

            if end < last_ts:
                return

    def _get_trades_page(
        self,
        pair: str,
        since: str = "",
        timeout: float = 10,
        max_retries: int = 5,
    ) -> tuple[dict[str, Any], Exception | None]:
        last_err: Exception | None = None

        for _ in range(max_retries):
            self._sleep()
            try:
                params = {"pair": pair, "since": since} if since else {"pair": pair}
                resp = self._s.get(self.BASE_URL, params=params, timeout=timeout)
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

    def _sleep(self, err: Any | None = None) -> None:
        time.sleep(self._delay)
        if err is None:
            self._delay, self._n = 0 if self._n < 22 else 1, self._n + 1
        else:
            self._delay, self._n = min(max(1, self._delay * 2), 5), 0

    def _parse_page(self, page: dict[str, Any]) -> tuple[str, list[dl.Record], str, Exception | None]:
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

        trades: list[dl.Record] = []
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
                dl.Record(
                    datetime.fromtimestamp(row[2], tz=timezone.utc),
                    row[0],
                    row[1] if row[3] == "b" else "0",
                    row[1] if row[3] == "s" else "0",
                    row[1] if row[4] == "m" else "0",
                    row[1] if row[4] == "l" else "0",
                    row[6],
                )
            )

        return sym, trades, last, None
