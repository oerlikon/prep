import asyncio
import json
from collections import defaultdict
from pathlib import Path

import websockets

from common import Cmd, Symbol
from util import p, parse_ts

from .common import TradeRecord, wsname


class Serve(Cmd):

    WS_URL = "wss://ws.kraken.com/v2"

    def __init__(self) -> None:
        self._pairs: dict[str, str] = {}
        self._trades: dict[str, list[TradeRecord]] = defaultdict(list)

    def run(self, *args, **kwargs) -> tuple[int | None, str | Exception | None]:
        symbols: dict[str, Symbol] | None = kwargs.get("symbols")
        if not symbols:
            return None, None

        for symbol in symbols.values():
            assert symbol.market == "Kraken"

        try:
            dl: Path | None = Path(args[0]) if len(args) > 0 else None
            asyncio.run(self._run_async(dl, symbols))
        except KeyboardInterrupt:
            p()
        except Exception as err:
            return 2, err

        return None, None

    async def _run_async(self, dl: Path | None, symbols: dict[str, Symbol]) -> None:
        for sym in symbols.values():
            name, err = wsname(sym.name)
            if err is not None:
                raise err
            self._pairs[name] = sym.name

        p(f"Connecting to {self.WS_URL}... ", end="")
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
            p("done.")

            subscribing = set(self._pairs.keys())

            while True:
                try:
                    raw = await ws.recv()
                except websockets.exceptions.ConnectionClosed as e:
                    p(f"Connection closed: {e.code} {e.reason}")
                    break
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(msg, dict):
                    channel = msg.get("channel")
                    if channel is None:
                        method = msg.get("method")
                        if method == "subscribe":
                            result = msg.get("result") or {}
                            channel = result.get("channel")
                            if channel == "trade":
                                success = bool(msg.get("success"))
                                if not success:
                                    err = msg.get("error", "unknown error")
                                    raise RuntimeError(f"subscribe error: {err!r}")
                                symbol = result.get("symbol")
                                if symbol not in subscribing:
                                    raise RuntimeError(f"unexpected: {symbol!r}")
                                subscribing.remove(symbol)
                                if not subscribing:
                                    p(f"Subscribed to trade feed for: {', '.join(self._pairs.keys())}")
                                continue
                    if channel == "status":
                        continue
                    if channel == "heartbeat":
                        continue
                    if channel == "trade":
                        err = self._process_trade_msg(msg)
                        if err is not None:
                            p(f"Error: {err!r}")
                        else:
                            continue
                print(msg)

    def _process_trade_msg(self, msg: dict) -> Exception | None:
        data = msg.get("data")
        if not isinstance(data, list):
            return None

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

            self._trades[symbol].append(rec)

        return None

    @staticmethod
    def _trade_from_ws(obj: dict) -> tuple[str, TradeRecord | None, Exception | None]:
        ts, symbol, price, qty = obj.get("timestamp"), obj.get("symbol"), obj.get("price"), obj.get("qty")
        side, ord_type, trade_id = obj.get("side"), obj.get("ord_type"), obj.get("trade_id")

        if (
            not isinstance(ts, str)
            or not isinstance(symbol, str)
            or not isinstance(price, (int, float))
            or not isinstance(qty, (int, float))
            or side not in ("buy", "sell")
            or ord_type not in ("limit", "market")
            or not isinstance(trade_id, int)
        ):
            return "", None, RuntimeError("field type mismatch")

        b = qty if side == "buy" else 0
        s = qty if side == "sell" else 0
        m = qty if ord_type == "market" else 0
        l = qty if ord_type == "limit" else 0

        return symbol, (parse_ts(ts).timestamp(), price, b, s, m, l, trade_id), None
