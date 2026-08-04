#!/usr/bin/env python3
"""
Ethereum TXID collector with live TXT writing.

Scans blocks 100..25,673,000 using multiple public Ethereum RPC endpoints.
For every completed batch:
1. new unique transaction hashes are committed to SQLite;
2. the same new TXIDs are appended immediately to ethereum_txids.txt;
3. TXT is flushed and fsynced;
4. terminal dashboard shows DB count and TXT written count.

Python 3.10+
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import os
import random
import signal
import sqlite3
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
import aiosqlite
from rich.console import Console
from rich.live import Live
from rich.table import Table


# ============================================================
# CONFIG
# ============================================================

RPC_URLS = [
    "https://ethereum-json-rpc.stakely.io",
                "https://0xrpc.io/eth",
                "https://eth1.lava.build",
                "https://rpc.nodeflare.app/eth/public",
                "https://ethereum-rpc.publicnode.com",
                "https://eth.blockrazor.xyz",
                "wss://ethereum-rpc.publicnode.com",
                "https://public-eth.nownodes.io",
                "wss://0xrpc.io/eth",
                "https://mainnet.gateway.tenderly.co",
                "https://gateway.tenderly.co/public/mainnet",
                "wss://one.valve.city/rpc/vk_demo/evm/1",
                "https://1rpc.io/eth",
                "https://rpc.mevblocker.io/fullprivacy",
                "https://rpc-eth.blockmachine.io",
                "https://rpc.mevblocker.io/fast",
                "https://eth-rpc.keccak.io",
                "https://rpc.fullsend.to",
                "https://rpc.flashbots.net",
                "https://mainnet.rpc.sentio.xyz",
                "https://eth-mainnet.nodereal.io/v1/1659dfb40aa24bbb8153a677b98064d7",
                "https://rpcfree.com/ethereum-rpc",
                "https://rpc.swiftnodes.io/rpc/eth",
                "wss://ethereum.callstaticrpc.com",
]

START_BLOCK = 1000000
END_BLOCK = 25_673_000

DB_FILE = Path("ethereum_txids.db")
TXT_FILE = Path("txids.txt")

WORKERS_PER_RPC = 1
REQUEST_TIMEOUT = 35
MAX_ATTEMPTS = 5

BLOCK_QUEUE_SIZE = 2000
RESULT_QUEUE_SIZE = 2000
# 1 = write TXIDs immediately after every completed block.
DB_BATCH_BLOCKS = 1

BASE_COOLDOWN = 15.0
MAX_COOLDOWN = 180.0

UI_REFRESH = 0.25
SPEED_WINDOW = 60.0

console = Console()


# ============================================================
# MODELS
# ============================================================

@dataclass(slots=True)
class BlockResult:
    block_number: int
    txids: set[str]
    tx_count: int
    rpc_index: int


@dataclass(slots=True)
class RpcState:
    index: int
    url: str
    enabled: bool = True
    latest_block: int | None = None
    chain_id: int | None = None
    completed: int = 0
    failures: int = 0
    consecutive_failures: int = 0
    cooldown_until: float = 0.0
    current_blocks: set[int] = field(default_factory=set)
    last_error: str = ""

    @property
    def host(self) -> str:
        return (
            self.url
            .replace("https://", "")
            .replace("http://", "")
            .replace("wss://", "")
            .replace("ws://", "")
            .rstrip("/")
        )

    @property
    def protocol(self) -> str:
        return "WSS" if self.url.startswith(("ws://", "wss://")) else "HTTP"

    @property
    def cooldown_left(self) -> float:
        return max(0.0, self.cooldown_until - time.monotonic())


@dataclass(slots=True)
class Stats:
    started_at: float = field(default_factory=time.monotonic)
    resume_block: int = START_BLOCK
    end_block: int = END_BLOCK
    last_contiguous: int = START_BLOCK - 1

    downloaded_blocks: int = 0
    committed_blocks: int = 0
    tx_count: int = 0
    rpc_errors: int = 0

    db_unique_txids: int = 0
    txt_written_total: int = 0
    txt_written_session: int = 0
    last_batch_new_txids: int = 0
    last_txt_write_at: float | None = None

    stopping: bool = False
    done: bool = False
    samples: deque[tuple[float, int]] = field(default_factory=deque)

    @property
    def total_blocks(self) -> int:
        return max(0, self.end_block - self.resume_block + 1)

    @property
    def elapsed(self) -> float:
        return max(0.001, time.monotonic() - self.started_at)

    @property
    def remaining(self) -> int:
        return max(0, self.end_block - self.last_contiguous)

    def sample(self) -> None:
        now = time.monotonic()
        self.samples.append((now, self.committed_blocks))
        cutoff = now - SPEED_WINDOW
        while len(self.samples) > 1 and self.samples[0][0] < cutoff:
            self.samples.popleft()

    @property
    def speed(self) -> float:
        if len(self.samples) < 2:
            return self.committed_blocks / self.elapsed
        t0, b0 = self.samples[0]
        t1, b1 = self.samples[-1]
        return (b1 - b0) / (t1 - t0) if t1 > t0 else 0.0

    @property
    def eta(self) -> float | None:
        return self.remaining / self.speed if self.speed > 0 else None


# ============================================================
# HELPERS
# ============================================================

def normalize_txid(value: Any) -> str | None:
    """Validate and normalize a 32-byte Ethereum transaction hash."""
    if not isinstance(value, str):
        return None

    value = value.strip().lower()

    if len(value) != 66 or not value.startswith("0x"):
        return None

    try:
        int(value[2:], 16)
    except ValueError:
        return None

    return value


def short_error(error: BaseException, limit: int = 72) -> str:
    text = " ".join(str(error).split()) or type(error).__name__
    return text if len(text) <= limit else text[: limit - 1] + "…"


def format_duration(seconds: float | None) -> str:
    if seconds is None or not math.isfinite(seconds):
        return "--"

    seconds = max(0, int(seconds))
    days, seconds = divmod(seconds, 86_400)
    hours, seconds = divmod(seconds, 3_600)
    minutes, seconds = divmod(seconds, 60)

    if days:
        return f"{days}d {hours:02d}h {minutes:02d}m"
    if hours:
        return f"{hours:02d}h {minutes:02d}m {seconds:02d}s"
    return f"{minutes:02d}m {seconds:02d}s"


def count_txt_lines(path: Path) -> int:
    if not path.exists():
        return 0

    count = 0
    with path.open("rb") as file:
        while chunk := file.read(8 * 1024 * 1024):
            count += chunk.count(b"\n")
    return count


def rebuild_txt_from_db(db_path: Path, txt_path: Path) -> int:
    """
    Atomically rebuild TXT from the authoritative SQLite database.
    Used if DB exists but TXT is missing or inconsistent.
    """
    txt_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = txt_path.with_suffix(txt_path.suffix + ".rebuild.tmp")

    connection = sqlite3.connect(db_path)
    total = 0

    try:
        cursor = connection.execute(
            "SELECT txid FROM txids ORDER BY txid"
        )

        with temp_path.open(
            "w",
            encoding="utf-8",
            newline="\n",
            buffering=1024 * 1024,
        ) as output:
            while True:
                rows = cursor.fetchmany(100_000)
                if not rows:
                    break

                output.writelines(txid + "\n" for (txid,) in rows)
                total += len(rows)

            output.flush()
            os.fsync(output.fileno())

        os.replace(temp_path, txt_path)

    finally:
        connection.close()
        with contextlib.suppress(FileNotFoundError):
            temp_path.unlink()

    return total



class RpcClient:
    """
    Unified JSON-RPC client for HTTP(S) and WebSocket endpoints.

    HTTP requests use normal POST calls.
    WSS requests use one persistent websocket connection protected by a lock.
    The lock guarantees request/response ordering for a shared websocket.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> None:
        self.session = session
        self.url = url
        self.is_websocket = url.startswith(("ws://", "wss://"))
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ws_lock = asyncio.Lock()
        self._request_id = random.randint(1, 1_000_000_000)

    def next_id(self) -> int:
        self._request_id += 1
        if self._request_id > 2_147_483_647:
            self._request_id = 1
        return self._request_id

    async def close(self) -> None:
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        self._ws = None

    async def _ensure_ws(self) -> aiohttp.ClientWebSocketResponse:
        if self._ws is not None and not self._ws.closed:
            return self._ws

        self._ws = await self.session.ws_connect(
            self.url,
            heartbeat=20,
            autoping=True,
            autoclose=True,
            timeout=aiohttp.ClientWSTimeout(
                ws_receive=REQUEST_TIMEOUT,
                ws_close=10.0,
            ),
            max_msg_size=16 * 1024 * 1024,
        )
        return self._ws

    async def call(
        self,
        method: str,
        params: list[Any],
    ) -> Any:
        request_id = self.next_id()

        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }

        if not self.is_websocket:
            async with self.session.post(
                self.url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as response:
                body = await response.text()

                if response.status != 200:
                    raise RuntimeError(
                        f"HTTP {response.status}: {body[:180]}"
                    )

                try:
                    decoded = json.loads(body)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"Invalid JSON: {body[:180]}"
                    ) from exc

                if decoded.get("id") not in (request_id, str(request_id)):
                    raise RuntimeError(
                        f"JSON-RPC id mismatch: expected={request_id}, "
                        f"received={decoded.get('id')}"
                    )

                if decoded.get("error") is not None:
                    raise RuntimeError(str(decoded["error"]))

                if "result" not in decoded:
                    raise RuntimeError("Missing JSON-RPC result")

                return decoded["result"]

        async with self._ws_lock:
            for ws_attempt in range(2):
                try:
                    ws = await self._ensure_ws()
                    await ws.send_json(payload)

                    while True:
                        message = await asyncio.wait_for(
                            ws.receive(),
                            timeout=REQUEST_TIMEOUT,
                        )

                        if message.type == aiohttp.WSMsgType.TEXT:
                            try:
                                decoded = json.loads(message.data)
                            except json.JSONDecodeError as exc:
                                raise RuntimeError(
                                    f"Invalid WSS JSON: {message.data[:180]}"
                                ) from exc

                            # Ignore subscription notifications or unrelated frames.
                            if decoded.get("id") not in (
                                request_id,
                                str(request_id),
                            ):
                                continue

                            if decoded.get("error") is not None:
                                raise RuntimeError(
                                    str(decoded["error"])
                                )

                            if "result" not in decoded:
                                raise RuntimeError(
                                    "Missing WSS JSON-RPC result"
                                )

                            return decoded["result"]

                        if message.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSING,
                        ):
                            raise RuntimeError("WSS connection closed")

                        if message.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError(
                                f"WSS error: {ws.exception()}"
                            )

                except (
                    aiohttp.ClientError,
                    asyncio.TimeoutError,
                    RuntimeError,
                ):
                    await self.close()
                    if ws_attempt == 1:
                        raise

            raise RuntimeError("WSS request failed")


# ============================================================
# RPC
# ============================================================

async def rpc_call(
    client: RpcClient,
    method: str,
    params: list[Any],
) -> Any:
    return await client.call(method, params)


async def inspect_rpc(
    client: RpcClient,
    state: RpcState,
) -> None:
    try:
        chain_hex, latest_hex = await asyncio.gather(
            rpc_call(client, "eth_chainId", []),
            rpc_call(client, "eth_blockNumber", []),
        )

        state.chain_id = int(chain_hex, 16)
        state.latest_block = int(latest_hex, 16)

        if state.chain_id != 1:
            state.enabled = False
            state.last_error = f"wrong chainId={state.chain_id}"

    except Exception as error:
        state.enabled = False
        state.last_error = short_error(error)


async def fetch_block(
    client: RpcClient,
    state: RpcState,
    block_number: int,
) -> dict[str, Any]:
    result = await rpc_call(
        client,
        "eth_getBlockByNumber",
        [hex(block_number), True],
    )

    if not isinstance(result, dict):
        raise RuntimeError("RPC returned null or invalid block")

    number = result.get("number")
    if number is not None and int(number, 16) != block_number:
        raise RuntimeError("RPC returned a different block")

    return result


# ============================================================
# DATABASE
# ============================================================

async def init_db(
    db_path: Path,
    start_block: int,
    end_block: int,
    reset: bool,
) -> None:
    if reset:
        for path in (
            db_path,
            Path(str(db_path) + "-wal"),
            Path(str(db_path) + "-shm"),
        ):
            with contextlib.suppress(FileNotFoundError):
                path.unlink()

    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA temp_store=MEMORY")

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS txids (
                txid TEXT PRIMARY KEY
            ) WITHOUT ROWID
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID
            """
        )

        await db.execute(
            """
            INSERT OR IGNORE INTO metadata(key, value)
            VALUES ('last_contiguous_block', ?)
            """,
            (str(start_block - 1),),
        )

        await db.executemany(
            """
            INSERT INTO metadata(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            [
                ("configured_start_block", str(start_block)),
                ("configured_end_block", str(end_block)),
            ],
        )

        await db.commit()


async def load_db_state(
    db_path: Path,
    start_block: int,
) -> tuple[int, int]:
    async with aiosqlite.connect(db_path) as db:
        progress_row = await (
            await db.execute(
                "SELECT value FROM metadata WHERE key='last_contiguous_block'"
            )
        ).fetchone()

        count_row = await (
            await db.execute("SELECT COUNT(*) FROM txids")
        ).fetchone()

    last_block = (
        int(progress_row[0])
        if progress_row
        else start_block - 1
    )
    unique_count = int(count_row[0]) if count_row else 0

    return last_block, unique_count


# ============================================================
# WORKERS
# ============================================================

async def worker(
    worker_number: int,
    state: RpcState,
    client: RpcClient,
    block_queue: asyncio.Queue[int | None],
    result_queue: asyncio.Queue[BlockResult | None],
    stats: Stats,
    stop_event: asyncio.Event,
) -> None:
    rng = random.Random(worker_number * 15_485_863)

    while True:
        block_number = await block_queue.get()

        if block_number is None:
            block_queue.task_done()
            return

        if stop_event.is_set():
            block_queue.task_done()
            continue

        state.current_blocks.add(block_number)
        success = False

        try:
            for attempt in range(1, MAX_ATTEMPTS + 1):
                if state.cooldown_left:
                    await asyncio.sleep(state.cooldown_left)

                try:
                    block = await fetch_block(
                        client,
                        state,
                        block_number,
                    )

                    transactions = block.get("transactions", [])
                    if not isinstance(transactions, list):
                        raise RuntimeError("transactions field is invalid")

                    txids: set[str] = set()

                    for tx in transactions:
                        if not isinstance(tx, dict):
                            continue

                        txid = normalize_txid(tx.get("hash"))
                        if txid:
                            txids.add(txid)

                    await result_queue.put(
                        BlockResult(
                            block_number=block_number,
                            txids=txids,
                            tx_count=len(transactions),
                            rpc_index=state.index,
                        )
                    )

                    state.completed += 1
                    state.consecutive_failures = 0
                    state.last_error = ""
                    stats.downloaded_blocks += 1
                    stats.tx_count += len(transactions)
                    success = True
                    break

                except (
                    aiohttp.ClientError,
                    asyncio.TimeoutError,
                    RuntimeError,
                ) as error:
                    state.failures += 1
                    state.consecutive_failures += 1
                    stats.rpc_errors += 1
                    state.last_error = short_error(error)

                    wait = min(2 ** (attempt - 1), 20)
                    await asyncio.sleep(wait + rng.uniform(0.0, 1.0))

            if not success and not stop_event.is_set():
                level = max(
                    1,
                    state.consecutive_failures // MAX_ATTEMPTS,
                )

                cooldown = min(
                    BASE_COOLDOWN * (2 ** (level - 1)),
                    MAX_COOLDOWN,
                )

                state.cooldown_until = (
                    time.monotonic() + cooldown
                )

                # Return failed block to the shared queue.
                await block_queue.put(block_number)

        finally:
            state.current_blocks.discard(block_number)
            block_queue.task_done()


async def monitor_worker_failures(
    worker_tasks: list[asyncio.Task[None]],
    stop_event: asyncio.Event,
) -> None:
    """
    Fail fast if any worker crashes unexpectedly.

    Without this monitor, a producer can wait forever on a full queue while
    all consumers have already terminated.
    """
    pending: set[asyncio.Task[None]] = set(worker_tasks)

    while pending and not stop_event.is_set():
        done, pending = await asyncio.wait(
            pending,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in done:
            if task.cancelled():
                continue

            error = task.exception()
            if error is not None:
                stop_event.set()
                raise RuntimeError(
                    f"Worker crashed: {type(error).__name__}: {error}"
                ) from error


async def producer(
    queue: asyncio.Queue[int | None],
    first_block: int,
    end_block: int,
    stop_event: asyncio.Event,
) -> None:
    for block_number in range(first_block, end_block + 1):
        if stop_event.is_set():
            return
        await queue.put(block_number)


# ============================================================
# RELIABLE DB + TXT WRITER
# ============================================================

async def db_and_txt_writer(
    db_path: Path,
    txt_path: Path,
    result_queue: asyncio.Queue[BlockResult | None],
    stats: Stats,
) -> None:
    """
    The DB is authoritative for deduplication.

    Each batch:
    - place candidate txids in a temporary SQLite table;
    - select only txids absent from the main txid table;
    - commit new txids and progress;
    - append exactly those new txids to TXT;
    - flush + fsync TXT before acknowledging the batch.
    """
    pending_blocks: set[int] = set()
    next_contiguous = stats.last_contiguous + 1

    txt_path.parent.mkdir(parents=True, exist_ok=True)

    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA temp_store=MEMORY")

        await db.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS candidate_txids (
                txid TEXT PRIMARY KEY
            ) WITHOUT ROWID
            """
        )
        await db.commit()

        with txt_path.open(
            "a",
            encoding="utf-8",
            newline="\n",
            buffering=1,
        ) as txt:
            while True:
                first = await result_queue.get()

                if first is None:
                    result_queue.task_done()
                    break

                batch = [first]

                while len(batch) < DB_BATCH_BLOCKS:
                    try:
                        item = result_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    if item is None:
                        result_queue.task_done()
                        await result_queue.put(None)
                        break

                    batch.append(item)

                candidate_set: set[str] = set()

                for result in batch:
                    candidate_set.update(result.txids)
                    pending_blocks.add(result.block_number)

                new_txids: list[str] = []

                try:
                    await db.execute("BEGIN IMMEDIATE")
                    await db.execute(
                        "DELETE FROM candidate_txids"
                    )

                    if candidate_set:
                        await db.executemany(
                            """
                            INSERT OR IGNORE INTO candidate_txids(txid)
                            VALUES (?)
                            """,
                            ((txid,) for txid in candidate_set),
                        )

                        cursor = await db.execute(
                            """
                            SELECT c.txid
                            FROM candidate_txids AS c
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM txids AS a
                                WHERE a.txid = c.txid
                            )
                            ORDER BY c.txid
                            """
                        )

                        rows = await cursor.fetchall()
                        new_txids = [row[0] for row in rows]

                        if new_txids:
                            await db.executemany(
                                """
                                INSERT INTO txids(txid)
                                VALUES (?)
                                """,
                                ((txid,) for txid in new_txids),
                            )

                    while next_contiguous in pending_blocks:
                        pending_blocks.remove(next_contiguous)
                        next_contiguous += 1

                    last_contiguous = next_contiguous - 1

                    await db.execute(
                        """
                        INSERT INTO metadata(key, value)
                        VALUES ('last_contiguous_block', ?)
                        ON CONFLICT(key)
                        DO UPDATE SET value=excluded.value
                        """,
                        (str(last_contiguous),),
                    )

                    await db.commit()

                except Exception:
                    await db.rollback()
                    raise

                # Write only after SQLite commit succeeds.
                if new_txids:
                    txt.writelines(
                        txid + "\n"
                        for txid in new_txids
                    )
                    txt.flush()
                    os.fsync(txt.fileno())

                    stats.txt_written_total += len(new_txids)
                    stats.txt_written_session += len(new_txids)
                    stats.last_txt_write_at = time.monotonic()

                stats.last_batch_new_txids = len(new_txids)
                stats.db_unique_txids += len(new_txids)
                stats.committed_blocks += len(batch)
                stats.last_contiguous = last_contiguous
                stats.sample()

                # Verify exact DB count periodically.
                if stats.committed_blocks % 1000 < len(batch):
                    count_row = await (
                        await db.execute(
                            "SELECT COUNT(*) FROM txids"
                        )
                    ).fetchone()

                    stats.db_unique_txids = int(count_row[0])

                for _ in batch:
                    result_queue.task_done()

            txt.flush()
            os.fsync(txt.fileno())

        count_row = await (
            await db.execute("SELECT COUNT(*) FROM txids")
        ).fetchone()

        stats.db_unique_txids = int(count_row[0])


# ============================================================
# DASHBOARD
# ============================================================

def dashboard(
    stats: Stats,
    rpc_states: list[RpcState],
    txt_path: Path,
) -> Table:
    total = max(1, stats.total_blocks)

    completed_range = max(
        0,
        stats.last_contiguous - stats.resume_block + 1,
    )

    progress = min(
        100.0,
        completed_range * 100.0 / total,
    )

    active_blocks = sorted(
        block
        for rpc in rpc_states
        for block in rpc.current_blocks
    )

    active_text = (
        ", ".join(f"{block:,}" for block in active_blocks[:12])
        + (" …" if len(active_blocks) > 12 else "")
    ) or "--"

    status = (
        "[yellow]STOPPING[/yellow]"
        if stats.stopping
        else "[green]DONE[/green]"
        if stats.done
        else "[cyan]RUNNING[/cyan]"
    )

    root = Table.grid(expand=True)
    root.add_column()

    summary = Table(
        title=f"Ethereum TXID collector — {status}",
        expand=True,
    )

    summary.add_column("Metric", style="bold")
    summary.add_column("Value")
    summary.add_column("Metric", style="bold")
    summary.add_column("Value")

    summary.add_row(
        "Block range",
        f"{stats.resume_block:,} → {stats.end_block:,}",
        "Current blocks",
        active_text,
    )

    summary.add_row(
        "Saved through",
        f"{stats.last_contiguous:,}",
        "Progress",
        f"{progress:.4f}%",
    )

    summary.add_row(
        "Speed",
        f"{stats.speed:,.2f} blocks/s",
        "ETA",
        format_duration(stats.eta),
    )

    summary.add_row(
        "Remaining blocks",
        f"{stats.remaining:,}",
        "Elapsed",
        format_duration(stats.elapsed),
    )

    summary.add_row(
        "Transactions found",
        f"{stats.tx_count:,}",
        "Blocks downloaded",
        f"{stats.downloaded_blocks:,}",
    )

    summary.add_row(
        "Blocks committed",
        f"{stats.committed_blocks:,}",
        "RPC errors",
        f"{stats.rpc_errors:,}",
    )

    summary.add_row(
        "[green]DB unique TXIDs[/green]",
        f"[bold green]{stats.db_unique_txids:,}[/bold green]",
        "[bold cyan]TXT written this run[/bold cyan]",
        f"[bold cyan]{stats.txt_written_session:,}[/bold cyan]",
    )

    last_write = (
        f"{time.monotonic() - stats.last_txt_write_at:.1f}s ago"
        if stats.last_txt_write_at is not None
        else "--"
    )

    summary.add_row(
        "[bold cyan]TXT total TXIDs[/bold cyan]",
        f"[bold cyan]{stats.txt_written_total:,}[/bold cyan]",
        "New TXIDs in last block",
        f"{stats.last_batch_new_txids:,}",
    )

    summary.add_row(
        "TXT last write",
        last_write,
        "TXT write mode",
        "immediate per block",
    )

    summary.add_row(
        "TXT file",
        str(txt_path.resolve()),
        "TXT file size",
        (
            f"{txt_path.stat().st_size / (1024 * 1024):,.2f} MB"
            if txt_path.exists()
            else "0 MB"
        ),
    )

    width = 50
    filled = int(width * progress / 100)
    bar = (
        "[green]"
        + "━" * filled
        + "[/green][dim]"
        + "━" * (width - filled)
        + f"[/dim] {progress:.4f}%"
    )

    rpc_table = Table(title="RPC status", expand=True)
    rpc_table.add_column("#", justify="right")
    rpc_table.add_column("RPC")
    rpc_table.add_column("Type")
    rpc_table.add_column("Status")
    rpc_table.add_column("Current")
    rpc_table.add_column("Done", justify="right")
    rpc_table.add_column("Errors", justify="right")
    rpc_table.add_column("Last error")

    for rpc in rpc_states:
        if not rpc.enabled:
            rpc_status = "[red]disabled[/red]"
        elif rpc.cooldown_left > 0:
            rpc_status = (
                f"[yellow]cooldown {rpc.cooldown_left:.0f}s[/yellow]"
            )
        elif rpc.current_blocks:
            rpc_status = "[cyan]working[/cyan]"
        else:
            rpc_status = "[green]ready[/green]"

        current = (
            ",".join(
                f"{block:,}"
                for block in sorted(rpc.current_blocks)
            )
            or "--"
        )

        rpc_table.add_row(
            str(rpc.index + 1),
            rpc.host,
            rpc.protocol,
            rpc_status,
            current,
            f"{rpc.completed:,}",
            f"{rpc.failures:,}",
            rpc.last_error or "--",
        )

    root.add_row(summary)
    root.add_row(bar)
    root.add_row(rpc_table)
    root.add_row(
        "[dim]Har bir blokdan keyin yangi TXIDlar TXT ga yozilib, flush va fsync qilinadi. "
        "Ctrl+C — xavfsiz to‘xtatish.[/dim]"
    )

    return root


async def run_dashboard(
    stats: Stats,
    rpc_states: list[RpcState],
    txt_path: Path,
    ui_stop: asyncio.Event,
) -> None:
    with Live(
        dashboard(stats, rpc_states, txt_path),
        console=console,
        refresh_per_second=4,
        transient=False,
    ) as live:
        while not ui_stop.is_set():
            stats.sample()
            live.update(
                dashboard(stats, rpc_states, txt_path)
            )
            await asyncio.sleep(UI_REFRESH)

        live.update(
            dashboard(stats, rpc_states, txt_path),
            refresh=True,
        )


# ============================================================
# MAIN
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--start", type=int, default=START_BLOCK)
    parser.add_argument("--end", type=int, default=END_BLOCK)
    parser.add_argument(
        "--workers-per-rpc",
        type=int,
        default=WORKERS_PER_RPC,
    )
    parser.add_argument("--db", type=Path, default=DB_FILE)
    parser.add_argument("--txt", type=Path, default=TXT_FILE)
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete DB and TXT and start over.",
    )

    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> int:
    if args.start < 0:
        raise ValueError("START block cannot be negative")

    if args.end < args.start:
        raise ValueError("END block must be >= START block")

    if args.workers_per_rpc < 1:
        raise ValueError("workers-per-rpc must be at least 1")

    args.db = args.db.resolve()
    args.txt = args.txt.resolve()

    if args.reset:
        for path in (
            args.txt,
            args.txt.with_suffix(args.txt.suffix + ".rebuild.tmp"),
        ):
            with contextlib.suppress(FileNotFoundError):
                path.unlink()

    await init_db(
        args.db,
        args.start,
        args.end,
        args.reset,
    )

    last_contiguous, db_count = await load_db_state(
        args.db,
        args.start,
    )

    # Ensure TXT is synchronized with DB before scanning.
    txt_lines = await asyncio.to_thread(
        count_txt_lines,
        args.txt,
    )

    if db_count != txt_lines:
        console.print(
            f"[yellow]DB/TXT mismatch detected:[/yellow] "
            f"DB={db_count:,}, TXT={txt_lines:,}. "
            "TXT is being rebuilt from DB."
        )

        txt_lines = await asyncio.to_thread(
            rebuild_txt_from_db,
            args.db,
            args.txt,
        )

        console.print(
            f"[green]TXT synchronized:[/green] "
            f"{txt_lines:,} txids → {args.txt}"
        )

    # Create TXT even when DB is empty.
    args.txt.parent.mkdir(parents=True, exist_ok=True)
    args.txt.touch(exist_ok=True)

    first_block = max(
        args.start,
        last_contiguous + 1,
    )

    stats = Stats(
        resume_block=first_block,
        end_block=args.end,
        last_contiguous=last_contiguous,
        db_unique_txids=db_count,
        txt_written_total=txt_lines,
    )
    stats.sample()

    stop_event = asyncio.Event()
    ui_stop = asyncio.Event()

    loop = asyncio.get_running_loop()

    def stop() -> None:
        if not stop_event.is_set():
            stats.stopping = True
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop)

    connector = aiohttp.TCPConnector(
        limit=max(
            20,
            len(RPC_URLS) * args.workers_per_rpc * 2,
        ),
        limit_per_host=max(
            2,
            args.workers_per_rpc + 1,
        ),
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "EthereumTxidCollector/2.0",
    }

    async with aiohttp.ClientSession(
        connector=connector,
        headers=headers,
    ) as session:
        rpc_states = [
            RpcState(index=i, url=url)
            for i, url in enumerate(RPC_URLS)
        ]

        rpc_clients = [
            RpcClient(session=session, url=state.url)
            for state in rpc_states
        ]

        await asyncio.gather(
            *(
                inspect_rpc(client, state)
                for client, state in zip(rpc_clients, rpc_states)
            )
        )

        healthy = [
            state
            for state in rpc_states
            if state.enabled
        ]

        healthy_pairs = [
            (state, rpc_clients[state.index])
            for state in healthy
        ]

        if not healthy:
            errors = "; ".join(
                f"{state.host}: {state.last_error}"
                for state in rpc_states
            )
            raise RuntimeError(
                f"No working Ethereum RPC: {errors}"
            )

        highest_latest = max(
            state.latest_block or 0
            for state in healthy
        )

        if args.end > highest_latest:
            console.print(
                f"[yellow]Warning:[/yellow] END_BLOCK "
                f"{args.end:,} is above current latest block "
                f"{highest_latest:,}. Future blocks will be retried."
            )

        dashboard_task = asyncio.create_task(
            run_dashboard(
                stats,
                rpc_states,
                args.txt,
                ui_stop,
            )
        )

        if first_block > args.end:
            stats.done = True
            ui_stop.set()
            await dashboard_task

            console.print(
                f"[green]Already complete.[/green] "
                f"TXT: {args.txt}"
            )
            return 0

        block_queue: asyncio.Queue[int | None] = (
            asyncio.Queue(maxsize=BLOCK_QUEUE_SIZE)
        )

        result_queue: asyncio.Queue[BlockResult | None] = (
            asyncio.Queue(maxsize=RESULT_QUEUE_SIZE)
        )

        writer_task = asyncio.create_task(
            db_and_txt_writer(
                args.db,
                args.txt,
                result_queue,
                stats,
            )
        )

        producer_task = asyncio.create_task(
            producer(
                block_queue,
                first_block,
                args.end,
                stop_event,
            )
        )

        worker_tasks: list[asyncio.Task[None]] = []
        worker_number = 0

        for state, client in healthy_pairs:
            # A shared WSS connection is serialized internally by a lock.
            # More than one worker is allowed, but WSS requests remain ordered.
            for _ in range(args.workers_per_rpc):
                worker_number += 1

                worker_tasks.append(
                    asyncio.create_task(
                        worker(
                            worker_number,
                            state,
                            client,
                            block_queue,
                            result_queue,
                            stats,
                            stop_event,
                        )
                    )
                )

        worker_monitor_task = asyncio.create_task(
            monitor_worker_failures(worker_tasks, stop_event)
        )

        done, _ = await asyncio.wait(
            {producer_task, worker_monitor_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if worker_monitor_task in done:
            await worker_monitor_task

        await producer_task

        if stop_event.is_set():
            # Remove queued but not started blocks.
            while True:
                try:
                    block_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                else:
                    block_queue.task_done()

        await block_queue.join()

        for _ in worker_tasks:
            await block_queue.put(None)

        await asyncio.gather(*worker_tasks)

        worker_monitor_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_monitor_task

        await result_queue.join()
        await result_queue.put(None)
        await writer_task

        stats.done = not stop_event.is_set()
        stats.stopping = stop_event.is_set()

        ui_stop.set()
        await dashboard_task

        await asyncio.gather(
            *(client.close() for client in rpc_clients),
            return_exceptions=True,
        )

    final_txt_lines = await asyncio.to_thread(
        count_txt_lines,
        args.txt,
    )

    console.print()
    console.print(
        f"[bold green]TXT file:[/bold green] {args.txt}"
    )
    console.print(
        f"[bold green]TXT TXIDs:[/bold green] "
        f"{final_txt_lines:,}"
    )
    console.print(
        f"[bold green]DB unique TXIDs:[/bold green] "
        f"{stats.db_unique_txids:,}"
    )

    if final_txt_lines != stats.db_unique_txids:
        console.print(
            "[yellow]Final mismatch detected. "
            "Rebuilding TXT from DB…[/yellow]"
        )

        final_txt_lines = await asyncio.to_thread(
            rebuild_txt_from_db,
            args.db,
            args.txt,
        )

        console.print(
            f"[green]TXT repaired:[/green] "
            f"{final_txt_lines:,} txids"
        )

    if stop_event.is_set():
        console.print(
            f"[yellow]Stopped safely.[/yellow] "
            f"Next block: {stats.last_contiguous + 1:,}"
        )
        return 130

    console.print("[green]Scanning finished.[/green]")
    return 0


def main() -> int:
    args = parse_args()

    try:
        return asyncio.run(async_main(args))

    except KeyboardInterrupt:
        return 130

    except Exception as error:
        console.print(
            f"[bold red]Fatal error:[/bold red] {error}"
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
