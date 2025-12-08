import asyncio
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import websockets

import dl
from common import Cmd, Result, Symbol
from util import p, parse_timedelta, parse_ts, tsp, zx

from .client import Client
from .common import config, wsname


class Serve(Cmd):

    WS_URL = "wss://ws.kraken.com/v2"

    def __init__(self) -> None:
        self._pairs: dict[str, str] = {}
        self._pending: set[str] | None = None

        self._trades: dict[str, list[dl.Record]] = defaultdict(list)
        self._warmup: dict[str, list[dl.Record]] | None = None

        self._tasks: set[asyncio.Task] = set()
        self._queue: asyncio.Queue[Result | None] = asyncio.Queue()

    def run(self, *args, **kwargs) -> tuple[int | None, str | Exception | None]:
        symbols: dict[str, Symbol] | None = kwargs.get("symbols")
        if not symbols:
            return None, None
        if not args:
            return 1, "dl path?"

        for symbol in symbols.values():
            assert symbol.market == "Kraken"

        try:
            asyncio.run(self._run_async(Path(args[0]), symbols))
        except KeyboardInterrupt:
            pass
        except Exception as err:
            return 2, err

        return None, None

    async def _run_async(self, path: Path, symbols: dict[str, Symbol]) -> None:
        for symbol in symbols.values():
            name, err = wsname(symbol.name)
            if err is not None:
                raise err
            self._pairs[name] = symbol.name

        p("Symbols:", ", ".join(sorted(self._pairs.values())))

        try:
            self._start_ws()

            while True:
                item = await self._queue.get()
                if item is None:
                    break

                if item.err is not None:
                    raise item.err

                if isinstance(item, self.Subscribed):
                    self._pending = None
                    p("Subscriptions confirmed, warming up...")
                    self._warmup = defaultdict(list)
                    self._start_load_dl(path, symbols)
                    continue

                if isinstance(item, self.Trades):
                    for sym, trades in item.trades.items():
                        self._trades[sym].extend(trades)
                        if len(self._trades[sym]) > 300000:
                            self._trades[sym] = self._trades[sym][-250000:]
                    # p(" ".join(f"{symbol}:{len(trades)}" for symbol, trades in self._trades.items()))
                    continue

                if isinstance(item, self.LoadedTrades):
                    assert self._warmup is not None
                    if item.trades:
                        for sym, trades in item.trades.items():
                            p(
                                f"Loaded {sym}:",
                                f"{tsp(trades[0].ts)} -> {tsp(trades[-1].ts)}",
                                f"({len(trades)} records)",
                            )
                            self._warmup[sym].extend(trades)
                    else:
                        self._start_fetch(symbols)
                    continue

                if isinstance(item, self.FetchedTrades):
                    assert self._warmup is not None
                    if item.trades:
                        for sym, trades in item.trades.items():
                            p(
                                f"Fetched {sym}:",
                                f"{tsp(trades[0].ts)} -> {tsp(trades[-1].ts)}",
                                f"({len(trades)} records)",
                            )
                            self._warmup[sym] = dl.extend(self._warmup[sym], trades)
                    else:
                        for sym, trades in self._trades.items():
                            self._warmup[sym] = dl.extend(self._warmup[sym], trades)
                        self._trades, self._warmup = self._warmup, None
                        p("Warmup complete.")
                    continue

        finally:
            await self._shutdown()

    def _add_task(self, task: asyncio.Task):
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _shutdown(self) -> None:
        p()
        if self._tasks:
            for t in list(self._tasks):
                if not t.done():
                    t.cancel()
            errs = await asyncio.gather(*self._tasks, return_exceptions=True)
            for err in errs:
                if isinstance(err, Exception) and not isinstance(err, asyncio.CancelledError):
                    p(err)
        p("Stopped.")

    def _start_ws(self) -> None:

        async def fn() -> None:
            try:
                p(f"Connecting to {self.WS_URL}...")
                async with websockets.connect(self.WS_URL, ping_interval=20) as ws:
                    sub_msg = {
                        "method": "subscribe",
                        "params": {
                            "channel": "trade",
                            "symbol": list(self._pairs.keys()),
                            "snapshot": False,
                        },
                        "req_id": 1,
                    }
                    await ws.send(json.dumps(sub_msg))
                    p("Connected, confirming feed subscriptions...")

                    self._pending = set(self._pairs.keys())

                    while True:
                        try:
                            raw = await ws.recv()
                        except websockets.exceptions.ConnectionClosed as e:
                            p(f"Connection closed: {e.code} {e.reason}")
                            await self._queue.put(None)
                            break

                        err = await self._process_ws_msg(raw)
                        if err is not None:
                            await self._queue.put(Result(err=err))
                            break

            except asyncio.CancelledError:
                raise
            except Exception as err:
                await self._queue.put(Result(err=err))

        self._add_task(asyncio.create_task(fn()))

    class Subscribed(Result):
        pass

    async def _process_ws_msg(self, raw: websockets.Data) -> Exception | None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return RuntimeError(f"bad JSON: {str(raw)!r}")
        if isinstance(msg, dict):
            channel = msg.get("channel")
            if channel is None:
                method = msg.get("method")
                if method == "subscribe":
                    assert self._pending is not None
                    result = msg.get("result") or {}
                    channel = result.get("channel")
                    if channel == "trade":
                        success = bool(msg.get("success"))
                        if not success:
                            err = msg.get("error", "unknown error")
                            return RuntimeError(f"subscribe error: {err!r}")
                        symbol = result.get("symbol")
                        if symbol is None or symbol not in self._pending:
                            return RuntimeError(f"unexpected: {symbol!r}")
                        self._pending.remove(symbol)
                        if not self._pending:
                            await self._queue.put(self.Subscribed())
                        return None
            if channel == "status":
                return None
            if channel == "heartbeat":
                return None
            if channel == "trade":
                return await self._process_trade_msg(msg)
        p(msg)
        return None

    @dataclass
    class Trades(Result):
        trades: dict[str, list[dl.Record]]

    async def _process_trade_msg(self, msg: dict) -> Exception | None:
        data = msg.get("data")
        if not isinstance(data, list):
            p(f"unexpected: {str(msg)!r}")
            return None

        trades = defaultdict(list)

        for obj in data:
            if not isinstance(obj, dict):
                p(f"unexpected: {obj!r}")
                continue

            ws_symbol, rec, err = self._trade_from_ws(obj)
            if err is not None:
                p(f"unexpected: {err!r}: {obj!r}")
                continue
            assert rec is not None

            symbol = self._pairs.get(ws_symbol)
            if symbol is None:
                p(f"unexpected: {ws_symbol!r}: {obj!r}")
                continue

            trades[symbol].append(rec)

        await self._queue.put(self.Trades(trades))

        return None

    @staticmethod
    def _trade_from_ws(obj: dict) -> tuple[str, dl.Record | None, Exception | None]:
        ts, symbol, price, qty = obj.get("timestamp"), obj.get("symbol"), obj.get("price"), obj.get("qty")
        side, ord_type, trade_id = obj.get("side"), obj.get("ord_type"), obj.get("trade_id")

        if (
            not isinstance(ts, str)
            or not isinstance(symbol, str)
            or not isinstance(price, (str, int, float))
            or not isinstance(qty, (str, int, float))
            or side not in ("buy", "sell")
            or ord_type not in ("limit", "market")
            or not isinstance(trade_id, int)
        ):
            return "", None, RuntimeError(f"field type mismatch: {obj!r}")

        price, qty = zx(price), zx(qty)

        b = qty if side == "buy" else "0"
        s = qty if side == "sell" else "0"
        m = qty if ord_type == "market" else "0"
        l = qty if ord_type == "limit" else "0"

        return symbol, dl.Record(parse_ts(ts), price, b, s, m, l, trade_id), None

    @dataclass
    class LoadedTrades(Result):
        trades: dict[str, list[dl.Record]]

    def _start_load_dl(self, path: Path, symbols: dict[str, Symbol]) -> None:

        async def fn() -> None:
            try:
                warmup = datetime.now(tz=timezone.utc) - parse_timedelta(config.Warmup)

                for symbol in symbols.values():
                    tails, err = await asyncio.to_thread(dl.tails, path, symbol, warmup)
                    if err is not None:
                        await self._queue.put(Result(err=err))
                        break
                    if tails:
                        await self._queue.put(self.LoadedTrades({symbol.name: tails}))

                await self._queue.put(self.LoadedTrades({}))

            except asyncio.CancelledError:
                raise
            except Exception as err:
                await self._queue.put(Result(err=err))

        self._add_task(asyncio.create_task(fn()))

    @dataclass
    class FetchedTrades(Result):
        trades: dict[str, list[dl.Record]]

    def _start_fetch(self, symbols: dict[str, Symbol]) -> None:

        async def fn() -> None:
            try:
                assert self._warmup is not None

                client = Client()

                for symbol in symbols.values():
                    tails = self._warmup[symbol.name]
                    assert tails
                    last_ts, last_id = tails[-1].ts.timestamp(), tails[-1].id

                    pages = await asyncio.to_thread(
                        lambda: list(client.fetch_trades(symbol.name, last_ts, last_id))
                    )
                    trades: list[dl.Record] = []
                    for page, err in pages:
                        if err is not None:
                            await self._queue.put(Result(err=err))
                            return
                        trades.extend(page)
                    if trades:
                        await self._queue.put(self.FetchedTrades({symbol.name: trades}))

                await self._queue.put(self.FetchedTrades({}))

            except asyncio.CancelledError:
                raise
            except Exception as err:
                await self._queue.put(Result(err=err))

        self._add_task(asyncio.create_task(fn()))
