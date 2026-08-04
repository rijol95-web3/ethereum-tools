from __future__ import annotations

import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import rlp
from eth_keys import keys
from eth_utils import keccak
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


# ============================================================
# CONFIG
# ============================================================

INPUT_FILE = Path("addresses.txt")
OUTPUT_FILE = Path("public.txt")
NOT_FOUND_FILE = Path("not_found.txt")
ERROR_FILE = Path("errors.txt")

RESUME = True
SKIP_PREVIOUS_ERRORS = False

MAX_WORKERS = 4
REQUEST_TIMEOUT = 25
RPC_RETRIES = 3
EXPLORER_RETRIES = 4
EXPLORER_PAGE_SIZE = 100
MAX_EXPLORER_PAGES = 0
RATE_LIMIT_WAIT = 5.0
STATUS_REFRESH_PER_SECOND = 4

EXPLORER_APIS = [
    "https://eth.blockscout.com/api",
]

HTTP_RPCS = [
    "https://ethereum-rpc.publicnode.com",
    "https://mainnet.gateway.tenderly.co",
    "https://gateway.tenderly.co/public/mainnet",
    "https://ethereum-mainnet.gateway.tatum.io",
    "https://one.valve.city/rpc/vk_demo/evm/1",
    "https://ethereum.public.blockpi.network/v1/rpc/public",
    "https://eth.drpc.org",
    "https://eth-mainnet.nodereal.io/v1/1659dfb40aa24bbb8153a677b98064d7",
    "https://public-eth.nownodes.io",
    "https://0xrpc.io/eth",
    "https://eth.meowrpc.com",
    "https://api.zan.top/eth-mainnet",
    "https://eth.api.pocket.network",
    "https://rpc-eth.blockmachine.io",
    "https://rpc.flashbots.net",
    "https://eth1.lava.build",
    "https://public.1rpc.io/eth",
    "https://eth-mainnet.public.blastapi.io",
    "https://ethereum-public.nodies.app",
    "https://rpc.mevblocker.io",
    "https://rpc.fullsend.to",
    "https://rpc.flashbots.net/fast",
    "https://rpc.owlracle.info/eth/70d38ce1826c4a60bb2a8e05a6c8b20f",
    "https://rpc.mevblocker.io/noreverts",
    "https://mainnet.rpc.sentio.xyz",
    "https://rpc.mevblocker.io/fast",
    "https://rpc.mevblocker.io/fullprivacy",
    "https://eth.api.onfinality.io/public",
]


# ============================================================
# GLOBAL STATE
# ============================================================

console = Console()
state_lock = threading.Lock()
file_lock = threading.Lock()
stop_event = threading.Event()


@dataclass
class Stats:
    total: int = 0
    completed: int = 0
    found: int = 0
    not_found: int = 0
    errors: int = 0
    invalid: int = 0
    skipped: int = 0
    explorer_requests: int = 0
    rpc_requests: int = 0
    active_workers: int = 0
    active_rpc: int = 0
    start_time: float = 0.0
    current_address: str = "-"
    last_result: str = "-"
    last_tx: str = "-"
    last_rpc: str = "-"


stats = Stats()


@dataclass
class RPCState:
    url: str
    healthy: bool = False
    successes: int = 0
    failures: int = 0
    cooldown_until: float = 0.0
    last_error: str = ""


# ============================================================
# HELPERS
# ============================================================

def normalize_address(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None

    value = value.split("|", 1)[0].strip()
    if not value:
        return None

    value = value.split()[0].strip()
    if not value.startswith("0x"):
        value = "0x" + value

    if len(value) != 42:
        return None

    try:
        int(value[2:], 16)
    except ValueError:
        return None

    return value.lower()


def parse_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, bytes):
        return int.from_bytes(value, "big")

    text = str(value).strip()
    if text in ("", "0x"):
        return default
    return int(text, 16) if text.startswith(("0x", "0X")) else int(text)


def int_to_minimal_bytes(value: int) -> bytes:
    if value == 0:
        return b""
    return value.to_bytes((value.bit_length() + 7) // 8, "big")


def hex_to_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value

    text = str(value).strip()
    if text in ("", "0x"):
        return b""
    if text.startswith("0x"):
        text = text[2:]
    if len(text) % 2:
        text = "0" + text
    return bytes.fromhex(text)


def is_rate_limit_error(message: str) -> bool:
    message = message.lower()
    return any(x in message for x in ("429", "rate limit", "too many requests"))


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)

    if days:
        return f"{days}d {hours:02}:{minutes:02}:{seconds:02}"
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def shorten(value: str, length: int = 54) -> str:
    if len(value) <= length:
        return value
    left = max(10, length // 2 - 2)
    right = max(8, length - left - 3)
    return value[:left] + "..." + value[-right:]


def append_line(path: Path, line: str) -> None:
    with file_lock:
        with path.open("a", encoding="utf-8") as f:
            f.write(line.rstrip("\n") + "\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass


def update_stats(**kwargs: Any) -> None:
    with state_lock:
        for key, value in kwargs.items():
            setattr(stats, key, value)


def increment_stat(name: str, amount: int = 1) -> None:
    with state_lock:
        setattr(stats, name, getattr(stats, name) + amount)


# ============================================================
# TERMINAL DASHBOARD
# ============================================================

def build_dashboard() -> Panel:
    with state_lock:
        total = stats.total
        completed = stats.completed
        found = stats.found
        not_found = stats.not_found
        errors = stats.errors
        invalid = stats.invalid
        skipped = stats.skipped
        explorer_requests = stats.explorer_requests
        rpc_requests = stats.rpc_requests
        active_workers = stats.active_workers
        active_rpc = stats.active_rpc
        current_address = stats.current_address
        last_result = stats.last_result
        last_tx = stats.last_tx
        last_rpc = stats.last_rpc
        elapsed = time.time() - stats.start_time if stats.start_time else 0

    speed = completed / elapsed if elapsed > 0 else 0.0
    remaining = max(0, total - completed)
    eta = remaining / speed if speed > 0 else 0
    percent = (completed / total * 100) if total else 100.0

    main = Table.grid(expand=True)
    main.add_column(ratio=1)
    main.add_column(ratio=1)
    main.add_column(ratio=1)

    main.add_row(
        Text(f"Tekshirilmoqda\n{completed:,}/{total:,}", style="bold yellow"),
        Text(f"Found Public key\n{found:,}", style="bold green"),
        Text(f"Not found\n{not_found:,}", style="bold red"),
    )
    main.add_row(
        Text(f"Errors\n{errors:,}", style="bold magenta"),
        Text(f"Progress\n{percent:6.2f}%", style="bold cyan"),
        Text(f"Speed\n{speed:.3f} address/s", style="bold white"),
    )
    main.add_row(
        Text(f"Contunies\n{format_duration(elapsed)}", style="cyan"),
        Text(f"Time end\n{format_duration(eta) if speed else '--:--:--'}", style="cyan"),
        Text(f"Active worker / RPC\n{active_workers} / {active_rpc}", style="cyan"),
    )

    details = Table(show_header=False, box=None, expand=True, padding=(0, 1))
    details.add_column(style="bold blue", width=22)
    details.add_column(style="white")
    details.add_row("Now address", shorten(current_address, 70))
    details.add_row("Last result", last_result)
    details.add_row("Last TX", shorten(last_tx, 70))
    details.add_row("Last RPC", shorten(last_rpc, 70))
    details.add_row(
        "Requests",
        f"Explorer: {explorer_requests:,} | RPC: {rpc_requests:,}",
    )
    details.add_row(
        "Additional",
        f"Invalid: {invalid:,} | Previously checked: {skipped:,}",
    )
    details.add_row(
        "Files",
        "public.txt | not_found.txt | errors.txt",
    )

    outer = Table.grid(expand=True)
    outer.add_row(main)
    outer.add_row("")
    outer.add_row(details)

    return Panel(
        outer,
        title="[bold cyan]Ethereum Address → Public Key Recovery[/bold cyan]",
        subtitle="[dim]Explorer + RPC | the screen refreshes somewhere[/dim]",
        border_style="bright_blue",
    )


# ============================================================
# RPC POOL
# ============================================================

class RPCPool:
    def __init__(self, urls: list[str]) -> None:
        self.states = [RPCState(url=u) for u in dict.fromkeys(urls)]
        self.lock = threading.Lock()
        self.local = threading.local()
        self.request_id = 0

    def session(self) -> requests.Session:
        session = getattr(self.local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "Ethereum-Public-Key-Recovery/5.0",
            })
            self.local.session = session
        return session

    def next_id(self) -> int:
        with self.lock:
            self.request_id += 1
            return self.request_id

    def direct_call(
        self,
        state: RPCState,
        method: str,
        params: list[Any],
        timeout: int = REQUEST_TIMEOUT,
    ) -> Any:
        increment_stat("rpc_requests")

        payload = {
            "jsonrpc": "2.0",
            "id": self.next_id(),
            "method": method,
            "params": params,
        }

        response = self.session().post(
            state.url,
            json=payload,
            timeout=timeout,
        )

        if response.status_code == 429:
            raise RuntimeError("HTTP 429 rate limit")
        if response.status_code in (401, 403):
            raise RuntimeError(f"HTTP {response.status_code}: permission required")

        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError("RPC did not return JSON") from exc

        if not isinstance(data, dict):
            raise RuntimeError("RPC answer error")

        if data.get("error"):
            error = data["error"]
            if isinstance(error, dict):
                raise RuntimeError(
                    f"RPC {error.get('code')}: {error.get('message', error)}"
                )
            raise RuntimeError(f"RPC error: {error}")

        return data.get("result")

    def healthy_states(self) -> list[RPCState]:
        now = time.time()
        with self.lock:
            result = [
                s for s in self.states
                if s.healthy and s.cooldown_until <= now
            ]

        random.shuffle(result)
        result.sort(key=lambda s: (s.failures - s.successes, s.failures))
        return result

    def mark_success(self, state: RPCState) -> None:
        with self.lock:
            state.healthy = True
            state.successes += 1
            state.last_error = ""
            state.cooldown_until = 0

    def mark_failure(self, state: RPCState, error: str) -> None:
        cooldown = 60 if is_rate_limit_error(error) else 15
        with self.lock:
            state.failures += 1
            state.last_error = error[:300]
            state.cooldown_until = time.time() + cooldown

    def call(self, method: str, params: list[Any]) -> tuple[Any, RPCState]:
        states = self.healthy_states()

        if not states:
            with self.lock:
                states = [s for s in self.states if s.healthy]

        if not states:
            raise RuntimeError("Avtive Ethereum RPC not")

        errors: list[str] = []

        for state in states:
            for attempt in range(1, RPC_RETRIES + 1):
                if stop_event.is_set():
                    raise KeyboardInterrupt

                try:
                    result = self.direct_call(state, method, params)
                    self.mark_success(state)
                    update_stats(last_rpc=state.url)
                    return result, state
                except Exception as exc:
                    error = str(exc)
                    errors.append(f"{state.url}: {error}")
                    self.mark_failure(state, error)

                    if is_rate_limit_error(error):
                        break
                    if attempt < RPC_RETRIES:
                        time.sleep(0.4 * attempt)

        raise RuntimeError(" | ".join(errors[-5:]))

    def initialize(self) -> None:
        def test(state: RPCState) -> RPCState:
            try:
                chain_id = self.direct_call(state, "eth_chainId", [], timeout=10)
                if parse_int(chain_id) != 1:
                    raise RuntimeError("Ethereum not mainnet")

                self.direct_call(state, "eth_blockNumber", [], timeout=10)
                state.healthy = True
                state.successes += 1
            except Exception as exc:
                state.healthy = False
                state.last_error = str(exc)
            return state

        with ThreadPoolExecutor(max_workers=min(16, len(self.states))) as ex:
            list(ex.map(test, self.states))

        update_stats(active_rpc=sum(1 for s in self.states if s.healthy))

        if stats.active_rpc == 0:
            raise RuntimeError("Nobody Ethereum RPC no working")


rpc_pool = RPCPool(HTTP_RPCS)


# ============================================================
# EXPLORER
# ============================================================

class ExplorerClient:
    def __init__(self, urls: list[str]) -> None:
        self.urls = list(dict.fromkeys(urls))
        self.local = threading.local()

    def session(self) -> requests.Session:
        session = getattr(self.local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "Accept": "application/json",
                "User-Agent": "Ethereum-Public-Key-Recovery/5.0",
            })
            self.local.session = session
        return session

    def request_page(
        self,
        base_url: str,
        address: str,
        page: int,
    ) -> list[dict[str, Any]]:
        increment_stat("explorer_requests")

        response = self.session().get(
            base_url,
            params={
                "module": "account",
                "action": "txlist",
                "address": address,
                "page": page,
                "offset": EXPLORER_PAGE_SIZE,
                "sort": "asc",
            },
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code == 429:
            raise RuntimeError("Explorer HTTP 429 rate limit")

        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError("Explorer JSON did not return") from exc

        result = data.get("result")
        if isinstance(result, list):
            return result

        message = f"{data.get('message', '')} {result}".lower()
        if "no transactions" in message:
            return []

        raise RuntimeError(f"Explorer error: {message[:300]}")

    def get_page(
        self,
        address: str,
        page: int,
    ) -> list[dict[str, Any]]:
        errors: list[str] = []

        for base_url in self.urls:
            for attempt in range(1, EXPLORER_RETRIES + 1):
                try:
                    return self.request_page(base_url, address, page)
                except Exception as exc:
                    error = str(exc)
                    errors.append(error)
                    if is_rate_limit_error(error):
                        time.sleep(RATE_LIMIT_WAIT * attempt)
                    elif attempt < EXPLORER_RETRIES:
                        time.sleep(attempt)

        raise RuntimeError("Explorer API not working: " + " | ".join(errors[-4:]))

    def find_outgoing_tx(self, address: str) -> str | None:
        page = 1

        while True:
            if stop_event.is_set():
                raise KeyboardInterrupt

            if MAX_EXPLORER_PAGES and page > MAX_EXPLORER_PAGES:
                return None

            transactions = self.get_page(address, page)

            if not transactions:
                return None

            for tx in transactions:
                sender = str(tx.get("from", "")).lower()
                tx_hash = str(tx.get("hash", "")).lower()

                if sender == address and tx_hash.startswith("0x") and len(tx_hash) == 66:
                    return tx_hash

            if len(transactions) < EXPLORER_PAGE_SIZE:
                return None

            page += 1


explorer = ExplorerClient(EXPLORER_APIS)


# ============================================================
# SIGNING HASH
# ============================================================

def encode_access_list(access_list: Any) -> list[Any]:
    result = []
    for item in access_list or []:
        if isinstance(item, dict):
            address = hex_to_bytes(item.get("address"))
            storage_keys = [
                hex_to_bytes(k)
                for k in item.get("storageKeys", [])
            ]
        else:
            address = hex_to_bytes(item[0])
            storage_keys = [hex_to_bytes(k) for k in item[1]]

        result.append([address, storage_keys])
    return result


def encode_authorization_list(auth_list: Any) -> list[Any]:
    result = []
    for auth in auth_list or []:
        if isinstance(auth, dict):
            result.append([
                int_to_minimal_bytes(parse_int(auth.get("chainId"))),
                hex_to_bytes(auth.get("address")),
                int_to_minimal_bytes(parse_int(auth.get("nonce"))),
                int_to_minimal_bytes(parse_int(auth.get("yParity", auth.get("v", 0)))),
                int_to_minimal_bytes(parse_int(auth.get("r"))),
                int_to_minimal_bytes(parse_int(auth.get("s"))),
            ])
        else:
            result.append(list(auth))
    return result


def tx_type(tx: dict[str, Any]) -> int:
    return parse_int(tx.get("type", 0))


def recovery_id(tx: dict[str, Any], kind: int) -> int:
    if tx.get("yParity") is not None:
        value = parse_int(tx["yParity"])
    else:
        v = parse_int(tx.get("v"))
        if kind == 0:
            if v in (27, 28):
                value = v - 27
            elif v >= 35:
                value = (v - 35) % 2
            else:
                value = v
        else:
            value = v - 27 if v in (27, 28) else v

    if value not in (0, 1):
        raise ValueError(f"Noto‘g‘ri recovery id: {value}")
    return value


def signing_hash(tx: dict[str, Any]) -> bytes:
    kind = tx_type(tx)

    common_input = hex_to_bytes(tx.get("input", tx.get("data", "0x")))
    to = hex_to_bytes(tx.get("to"))

    if kind == 0:
        fields = [
            int_to_minimal_bytes(parse_int(tx.get("nonce"))),
            int_to_minimal_bytes(parse_int(tx.get("gasPrice"))),
            int_to_minimal_bytes(parse_int(tx.get("gas"))),
            to,
            int_to_minimal_bytes(parse_int(tx.get("value"))),
            common_input,
        ]

        v = parse_int(tx.get("v"))
        if v >= 35:
            chain_id = parse_int(tx.get("chainId")) if tx.get("chainId") is not None else (v - 35) // 2
            fields.extend([int_to_minimal_bytes(chain_id), b"", b""])

        return keccak(rlp.encode(fields))

    if kind == 1:
        fields = [
            int_to_minimal_bytes(parse_int(tx.get("chainId"))),
            int_to_minimal_bytes(parse_int(tx.get("nonce"))),
            int_to_minimal_bytes(parse_int(tx.get("gasPrice"))),
            int_to_minimal_bytes(parse_int(tx.get("gas"))),
            to,
            int_to_minimal_bytes(parse_int(tx.get("value"))),
            common_input,
            encode_access_list(tx.get("accessList")),
        ]
        return keccak(b"\x01" + rlp.encode(fields))

    if kind == 2:
        fields = [
            int_to_minimal_bytes(parse_int(tx.get("chainId"))),
            int_to_minimal_bytes(parse_int(tx.get("nonce"))),
            int_to_minimal_bytes(parse_int(tx.get("maxPriorityFeePerGas"))),
            int_to_minimal_bytes(parse_int(tx.get("maxFeePerGas"))),
            int_to_minimal_bytes(parse_int(tx.get("gas"))),
            to,
            int_to_minimal_bytes(parse_int(tx.get("value"))),
            common_input,
            encode_access_list(tx.get("accessList")),
        ]
        return keccak(b"\x02" + rlp.encode(fields))

    if kind == 3:
        fields = [
            int_to_minimal_bytes(parse_int(tx.get("chainId"))),
            int_to_minimal_bytes(parse_int(tx.get("nonce"))),
            int_to_minimal_bytes(parse_int(tx.get("maxPriorityFeePerGas"))),
            int_to_minimal_bytes(parse_int(tx.get("maxFeePerGas"))),
            int_to_minimal_bytes(parse_int(tx.get("gas"))),
            to,
            int_to_minimal_bytes(parse_int(tx.get("value"))),
            common_input,
            encode_access_list(tx.get("accessList")),
            int_to_minimal_bytes(parse_int(tx.get("maxFeePerBlobGas"))),
            [hex_to_bytes(x) for x in tx.get("blobVersionedHashes", [])],
        ]
        return keccak(b"\x03" + rlp.encode(fields))

    if kind == 4:
        fields = [
            int_to_minimal_bytes(parse_int(tx.get("chainId"))),
            int_to_minimal_bytes(parse_int(tx.get("nonce"))),
            int_to_minimal_bytes(parse_int(tx.get("maxPriorityFeePerGas"))),
            int_to_minimal_bytes(parse_int(tx.get("maxFeePerGas"))),
            int_to_minimal_bytes(parse_int(tx.get("gas"))),
            to,
            int_to_minimal_bytes(parse_int(tx.get("value"))),
            common_input,
            encode_access_list(tx.get("accessList")),
            encode_authorization_list(tx.get("authorizationList")),
        ]
        return keccak(b"\x04" + rlp.encode(fields))

    raise ValueError(f"No supports transaction kind: {kind}")


def recover_public_key(tx: dict[str, Any]) -> tuple[str, str]:
    kind = tx_type(tx)
    rid = recovery_id(tx, kind)
    r_value = parse_int(tx.get("r"))
    s_value = parse_int(tx.get("s"))

    if r_value <= 0 or s_value <= 0:
        raise ValueError("Transaction sign no yet")

    signature = keys.Signature(vrs=(rid, r_value, s_value))
    public_key = signature.recover_public_key_from_msg_hash(signing_hash(tx))
    public_bytes = public_key.to_bytes()

    public_hex = "0x04" + public_bytes.hex()
    recovered_address = "0x" + keccak(public_bytes)[-20:].hex()
    return public_hex, recovered_address.lower()


# ============================================================
# PROCESSING
# ============================================================

def get_transaction_count(address: str) -> int:
    result, _ = rpc_pool.call(
        "eth_getTransactionCount",
        [address, "latest"],
    )
    return parse_int(result)


def get_transaction(tx_hash: str) -> dict[str, Any]:
    result, _ = rpc_pool.call(
        "eth_getTransactionByHash",
        [tx_hash],
    )
    if not isinstance(result, dict):
        raise RuntimeError("Transaction RPC orqali topilmadi")
    return result


def process_address(address: str) -> None:
    increment_stat("active_workers")
    update_stats(current_address=address)

    try:
        nonce_count = get_transaction_count(address)

        if nonce_count == 0:
            append_line(NOT_FOUND_FILE, f"{address} | NO_OUTGOING_TRANSACTION")
            with state_lock:
                stats.not_found += 1
                stats.completed += 1
                stats.last_result = f"[red]Not Found[/red] — {address}"
                stats.last_tx = "-"
            return

        tx_hash = explorer.find_outgoing_tx(address)

        if tx_hash is None:
            append_line(
                NOT_FOUND_FILE,
                f"{address} | EXPLORER_OUTGOING_TX_NOT_FOUND | nonce={nonce_count}",
            )
            with state_lock:
                stats.not_found += 1
                stats.completed += 1
                stats.last_result = f"[red]Not Found[/red] — {address}"
                stats.last_tx = "-"
            return

        tx = get_transaction(tx_hash)

        sender = str(tx.get("from", "")).lower()
        if sender != address:
            raise RuntimeError(
                f"Transaction sender mos emas: {sender}"
            )

        public_key, recovered_address = recover_public_key(tx)

        if recovered_address != address:
            raise RuntimeError(
                f"Public key addressga mos emas: {recovered_address}"
            )

        block_number = parse_int(tx.get("blockNumber"))
        transaction_type = tx_type(tx)

        append_line(
            OUTPUT_FILE,
            (
                f"{address} | {public_key} | tx={tx_hash} | "
                f"block={block_number} | type={transaction_type}"
            ),
        )

        with state_lock:
            stats.found += 1
            stats.completed += 1
            stats.last_result = f"[green]PUBLIC KEY TOPILDI[/green] — {address}"
            stats.last_tx = tx_hash

    except Exception as exc:
        append_line(ERROR_FILE, f"{address} | {exc}")
        with state_lock:
            stats.errors += 1
            stats.completed += 1
            stats.last_result = f"[magenta]XATO[/magenta] — {address}"
            stats.last_tx = "-"
    finally:
        increment_stat("active_workers", -1)


# ============================================================
# FILE INPUT / RESUME
# ============================================================

def load_addresses_from_file(path: Path) -> set[str]:
    result: set[str] = set()

    if not path.exists():
        return result

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            address = normalize_address(line.split("|", 1)[0])
            if address:
                result.add(address)

    return result


def load_input_addresses() -> list[str]:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"{INPUT_FILE.resolve()} topilmadi")

    addresses: list[str] = []
    seen: set[str] = set()

    with INPUT_FILE.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue

            address = normalize_address(raw)
            if address is None:
                increment_stat("invalid")
                continue

            if address not in seen:
                seen.add(address)
                addresses.append(address)

    return addresses


def processed_addresses() -> set[str]:
    if not RESUME:
        return set()

    result = set()
    result.update(load_addresses_from_file(OUTPUT_FILE))
    result.update(load_addresses_from_file(NOT_FOUND_FILE))

    if SKIP_PREVIOUS_ERRORS:
        result.update(load_addresses_from_file(ERROR_FILE))

    return result


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    addresses = load_input_addresses()
    processed = processed_addresses()
    pending = [a for a in addresses if a not in processed]

    with state_lock:
        stats.total = len(pending)
        stats.skipped = len(addresses) - len(pending)
        stats.start_time = time.time()

    if not pending:
        console.print("[green]Barcha addresslar oldin tekshirilgan.[/green]")
        return

    with Live(
        build_dashboard(),
        console=console,
        refresh_per_second=STATUS_REFRESH_PER_SECOND,
        screen=True,
        transient=False,
    ) as live:
        try:
            update_stats(last_result="[cyan]RPC endpointlar tekshirilmoqda...[/cyan]")
            live.update(build_dashboard())

            rpc_pool.initialize()

            update_stats(last_result="[cyan]Addresslar tekshirilmoqda...[/cyan]")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_address, address) for address in pending]

                while futures:
                    live.update(build_dashboard())

                    done_now = [f for f in futures if f.done()]
                    for future in done_now:
                        futures.remove(future)
                        try:
                            future.result()
                        except Exception:
                            pass

                    if futures:
                        time.sleep(0.15)

        except KeyboardInterrupt:
            stop_event.set()
            update_stats(last_result="[yellow]Foydalanuvchi to‘xtatdi[/yellow]")
            live.update(build_dashboard())
        finally:
            stop_event.set()
            live.update(build_dashboard())

    console.print()
    console.print(
        Panel.fit(
            f"[bold green]End[/bold green]\n"
            f"Result: [cyan]{OUTPUT_FILE.resolve()}[/cyan]\n"
            f"Not Found: [cyan]{NOT_FOUND_FILE.resolve()}[/cyan]\n"
            f"Errors: [cyan]{ERROR_FILE.resolve()}[/cyan]",
            border_style="green",
        )
    )


if __name__ == "__main__":
    main()
