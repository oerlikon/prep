import asyncio
import json
from collections import defaultdict
from pathlib import Path

import websockets

from common import Cmd, Symbol, p

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
            pass
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
                raw = await ws.recv()
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
                                    p(f"Subscribed to trade feed for: {', '.join(self._pairs)}")
                                continue
                    if channel == "status":
                        continue
                    if channel == "heartbeat":
                        continue
                    if channel == "trade":
                        pass
                print(msg)
