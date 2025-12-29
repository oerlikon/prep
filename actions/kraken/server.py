import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

import dl
from util import tss


@dataclass
class Block:
    symbol: str
    records: list[dl.Record]

    def __str__(self) -> str:
        return (
            self.symbol
            + "\n"
            + "\n".join(
                " ".join((tss(rec.ts), rec.p, rec.b, rec.s, rec.m, rec.l, str(rec.id)))
                for rec in self.records
            )
            + "\n==\n"
        )


@dataclass(eq=False)
class Feed:
    websocket: ServerConnection
    stash: list[str] | None = field(default_factory=list)
    queue: asyncio.Queue[str] = field(default_factory=lambda: asyncio.Queue(maxsize=5555))


class Hub:
    def __init__(self) -> None:
        self._clients: set[Feed] = set()
        self._lock = asyncio.Lock()

    async def register(self, client: Feed) -> None:
        async with self._lock:
            self._clients.add(client)

    async def unregister(self, client: Feed) -> None:
        async with self._lock:
            self._clients.discard(client)

    async def push(self, blocks: list[Block]) -> None:
        msg = await asyncio.to_thread(_encode, blocks)

        async with self._lock:
            clients = list(self._clients)

        for c in clients:
            if c.stash is not None:
                c.stash.append(msg)
                continue

            try:
                c.queue.put_nowait(msg)
            except asyncio.QueueFull:
                try:
                    asyncio.create_task(c.websocket.close(code=1011, reason="client too slow"))
                except Exception:
                    pass


def _encode(blocks: list[Block]) -> str:
    return "\n".join(str(block) for block in blocks)


async def handler(
    ws: ServerConnection,
    hub: Hub,
    snapshot: Callable[[], Awaitable[list[Block]]],
) -> None:
    client = Feed(ws)
    await hub.register(client)

    relay_task: asyncio.Task | None = None
    try:
        blocks = await snapshot()
        warmup = await asyncio.to_thread(_encode, blocks)

        client.queue.put_nowait(warmup)
        assert client.stash is not None
        stash, client.stash = client.stash, None
        for msg in stash:
            try:
                client.queue.put_nowait(msg)
            except asyncio.QueueFull:
                asyncio.create_task(ws.close(code=1011, reason="client too slow"))
                return

        relay_task = asyncio.create_task(relay(client))

        async for _ in ws:
            pass

    except ConnectionClosed:
        pass
    finally:
        await hub.unregister(client)
        if relay_task is not None:
            relay_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await relay_task


async def relay(client: Feed) -> None:
    """Send messages from per-client queue to the websocket."""
    while True:
        msg = await client.queue.get()
        try:
            await client.websocket.send(msg)
        except ConnectionClosed:
            return
        finally:
            client.queue.task_done()


async def run_server(
    hub: Hub,
    snapshot: Callable[[], Awaitable[list[Block]]],
    host: str = "0.0.0.0",
    port: int = 8765,
) -> None:
    async with serve(lambda ws: handler(ws, hub, snapshot), host, port):
        await asyncio.Future()  # run forever
