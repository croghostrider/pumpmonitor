import asyncio
import base64
import contextlib
import json
import os
import struct
import signal
import sys
from datetime import datetime, timezone

import aiohttp
import base58
import websockets
import asyncpg
from dotenv import load_dotenv
from solders.pubkey import Pubkey

# Lade Umgebungsvariablen aus .env
load_dotenv()

# PostgreSQL-Konfiguration
PG_HOST     = os.environ["PG_HOST"]
PG_PORT     = int(os.environ.get("PG_PORT", 32772))
PG_DATABASE = "pumpmonitor"
PG_USER     = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]

# Solana / AMM-Konfiguration
WSS_ENDPOINT         = os.environ["SOLANA_NODE_WSS_ENDPOINT"]
RPC_ENDPOINT         = os.environ["SOLANA_NODE_RPC_ENDPOINT"]
PUMP_AMM_PROGRAM_ID  = Pubkey.from_string("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")
PUMP_PROGRAM_ID      = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
MARKET_ACCOUNT_LENGTH = 8 + 1 + 2 + 32*6 + 8
MARKET_DISCRIMINATOR  = base58.b58encode(b'\xf1\x9am\x04\x11\xb1m\xbc').decode()
QUOTE_MINT_SOL        = base58.b58encode(
    bytes(Pubkey.from_string("So11111111111111111111111111111111111111112"))
).decode()


# Globaler Connection-Pool und Tasks
db_pool: asyncpg.Pool
price_tasks: list[asyncio.Task] = []

async def ensure_database_exists():
    """
    Verbindet sich zur Standard-Datenbank, um die Ziel-DB anzulegen, falls sie nicht existiert.
    """
    # Verbinde zunächst zur Default-Datenbank 'postgres'
    conn = await asyncpg.connect(
        host=PG_HOST, port=PG_PORT,
        database='postgres', user=PG_USER, password=PG_PASSWORD
    )
    # Erstelle Datenbank, wenn sie nicht existiert (Fehler ignorieren)
    try:
        await conn.execute(f"CREATE DATABASE {PG_DATABASE}")
    except asyncpg.DuplicateDatabaseError:
        pass
    finally:
        await conn.close()

async def init_db():
    """Initialisiert den PostgreSQL-Pool und legt Tabellen an."""
    global db_pool
    # Stelle sicher, dass die Datenbank existiert
    await ensure_database_exists()
    # Erstelle Connection-Pool zur Ziel-Datenbank
    db_pool = await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT,
        database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD,
        min_size=1, max_size=10
    )
    async with db_pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS pool_events (
            id SERIAL PRIMARY KEY,
            pool_pubkey TEXT,
            detected_at TIMESTAMPTZ,
            slot BIGINT,
            pool_bump SMALLINT,
            idx SMALLINT,
            creator TEXT,
            base_mint TEXT,
            quote_mint TEXT,
            lp_mint TEXT,
            pool_base_token_account TEXT,
            pool_quote_token_account TEXT,
            lp_supply BIGINT,
            token_age_seconds BIGINT,
            creator_total_tokens BIGINT,
            creator_last_token_creation TIMESTAMPTZ
        );
        """)
        await conn.execute("""
        ALTER TABLE token_creations
            ADD COLUMN IF NOT EXISTS metadata_created_on TEXT,
            ADD COLUMN IF NOT EXISTS metadata_twitter TEXT,
            ADD COLUMN IF NOT EXISTS metadata_telegram TEXT,
            ADD COLUMN IF NOT EXISTS metadata_website TEXT;
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS price_logs (
            id SERIAL PRIMARY KEY,
            pool_pubkey TEXT,
            slot BIGINT,
            base_account TEXT,
            quote_account TEXT,
            base_amount BIGINT,
            quote_amount BIGINT,
            price DOUBLE PRECISION
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS token_creations (
            id SERIAL PRIMARY KEY,
            signature TEXT,
            created_at TIMESTAMPTZ,
            name TEXT,
            symbol TEXT,
            uri TEXT,
            mint TEXT,
            bonding_curve TEXT,
            user_pubkey TEXT
        );
        """)

# --- Parse-Funktionen ---
def parse_token_amount(data: bytes) -> int:
    return struct.unpack("<Q", data[64:72])[0]

def parse_market_account_data(data: bytes) -> dict:
    parsed = {}
    offset = 8
    fields = [
        ("pool_bump", "u8"), ("idx", "u16"), ("creator", "pubkey"),
        ("base_mint", "pubkey"), ("quote_mint", "pubkey"), ("lp_mint", "pubkey"),
        ("pool_base_token_account", "pubkey"), ("pool_quote_token_account", "pubkey"),
        ("lp_supply", "u64"),
    ]
    for name, typ in fields:
        if typ == "u8":
            parsed[name] = data[offset]
            offset += 1
        elif typ == "u16":
            parsed[name] = struct.unpack("<H", data[offset:offset+2])[0]
            offset += 2
        elif typ == "u64":
            parsed[name] = struct.unpack("<Q", data[offset:offset+8])[0]
            offset += 8
        else:
            raw = data[offset:offset+32]
            parsed[name] = base58.b58encode(raw).decode()
            offset += 32
    return parsed


def parse_create_instruction(data: bytes) -> dict | None:
    if len(data) < 8:
        return None
    offset = 8
    parsed = {}
    try:
        for key in ["name", "symbol", "uri"]:
            length = struct.unpack("<I", data[offset:offset+4])[0]
            offset += 4
            parsed[key] = data[offset:offset+length].decode()
            offset += length
        for key in ["mint", "bondingCurve", "user"]:
            parsed[key] = base58.b58encode(data[offset:offset+32]).decode()
            offset += 32
        return parsed
    except Exception:
        return None

# --- Fetch existing markets ---

async def fetch_existing_market_pubkeys() -> set[str]:
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "getProgramAccounts",
        "params": [
            str(PUMP_AMM_PROGRAM_ID),
            {"encoding": "base64", "commitment": "processed", "filters": [
                {"dataSize": MARKET_ACCOUNT_LENGTH},
                {"memcmp": {"offset": 0, "bytes": MARKET_DISCRIMINATOR}},
                {"memcmp": {"offset": 75, "bytes": QUOTE_MINT_SOL}}
            ]}
        ]
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(RPC_ENDPOINT, json=body) as resp:
            result = await resp.json()
            return {acc["pubkey"] for acc in result.get("result", [])}

# --- Preis-Änderungen abonnieren und loggen ---

async def subscribe_price_changes_for_10min(pool_pubkey: str, base_acc: str, quote_acc: str):
    sub_map = {}
    end_time = asyncio.get_event_loop().time() + 600
    base_amt = quote_amt = None
    last_price = None

    async with db_pool.acquire() as conn:
        async with websockets.connect(WSS_ENDPOINT) as ws:
            for idx, acc in [(1, base_acc), (2, quote_acc)]:
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": idx, "method": "accountSubscribe",
                    "params": [acc, {"encoding": "base64", "commitment": "processed"}]
                }))
                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("id") == idx and "result" in msg:
                        sub_map[msg["result"]] = acc
                        break

            while asyncio.get_event_loop().time() < end_time:
                msg = json.loads(await ws.recv())
                if msg.get("method") != "accountNotification":
                    continue
                sub_id = msg["params"]["subscription"]
                account = sub_map.get(sub_id)
                slot = msg["params"]["result"]["context"]["slot"]
                data_b64 = msg["params"]["result"]["value"]["data"][0]
                amt = parse_token_amount(base64.b64decode(data_b64))

                if account == base_acc:
                    base_amt = amt
                else:
                    quote_amt = amt

                if base_amt is not None and quote_amt is not None:
                    price = quote_amt / base_amt if base_amt else 0
                    if price != last_price:
                        await conn.execute(
                            """
                            INSERT INTO price_logs (
                                pool_pubkey, slot, base_account, quote_account,
                                base_amount, quote_amount, price
                            ) VALUES ($1,$2,$3,$4,$5,$6,$7)
                            """,
                            pool_pubkey, slot, base_acc, quote_acc,
                            base_amt, quote_amt, price
                        )
                        last_price = price

# --- Neue Pools überwachen ---

async def listen_new_markets():
    known = await fetch_existing_market_pubkeys()
    async with db_pool.acquire() as conn:
        async with websockets.connect(WSS_ENDPOINT) as ws:
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": 1, "method": "programSubscribe",
                "params": [
                    str(PUMP_AMM_PROGRAM_ID),
                    {"commitment": "processed", "encoding": "base64", "filters": [
                        {"dataSize": MARKET_ACCOUNT_LENGTH},
                        {"memcmp": {"offset": 0, "bytes": MARKET_DISCRIMINATOR}},
                        {"memcmp": {"offset": 75, "bytes": QUOTE_MINT_SOL}}
                    ]}
                ]
            }))
            while True:
                msg = json.loads(await ws.recv())
                if msg.get("method") != "programNotification":
                    continue
                val = msg["params"]["result"]["value"]
                pubkey = val["pubkey"]
                if pubkey in known:
                    continue

                raw_data = base64.b64decode(val["account"]["data"][0])
                parsed = parse_market_account_data(raw_data)
                if Pubkey.from_string(parsed["creator"]).is_on_curve():
                    continue

                task = asyncio.create_task(
                    subscribe_price_changes_for_10min(
                        pubkey,
                        parsed["pool_base_token_account"],
                        parsed["pool_quote_token_account"]
                    )
                )
                price_tasks.append(task)

                now = datetime.now(timezone.utc)
                print(f"new pool found: {pubkey}")
                slot = msg["params"]["result"]["context"]["slot"]

                first_row = await conn.fetchrow(
                    "SELECT created_at FROM token_creations WHERE mint=$1 ORDER BY created_at ASC LIMIT 1",
                    parsed["base_mint"]
                )
                token_age = int((now - first_row["created_at"]).total_seconds()) if first_row else None
                total = await conn.fetchval(
                    "SELECT COUNT(*) FROM token_creations WHERE user=$1",
                    parsed["creator"]
                )
                last = await conn.fetchval(
                    "SELECT MAX(created_at) FROM token_creations WHERE user=$1",
                    parsed["creator"]
                )

                await conn.execute(
                    """
                    INSERT INTO pool_events (
                        pool_pubkey, detected_at, slot,
                        pool_bump, idx, creator,
                        base_mint, quote_mint, lp_mint,
                        pool_base_token_account, pool_quote_token_account,
                        lp_supply, token_age_seconds,
                        creator_total_tokens, creator_last_token_creation
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                    """,
                    pubkey, now, slot,
                    parsed["pool_bump"], parsed["idx"], parsed["creator"],
                    parsed["base_mint"], parsed["quote_mint"], parsed["lp_mint"],
                    parsed["pool_base_token_account"], parsed["pool_quote_token_account"],
                    parsed["lp_supply"], token_age,
                    total, last
                )
                known.add(pubkey)

# --- Neue Token-Erstellungen überwachen ---

async def listen_for_new_tokens():
    while True:
        try:
            async with db_pool.acquire() as conn:
                async with websockets.connect(WSS_ENDPOINT) as ws:
                    await ws.send(json.dumps({
                        "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                        "params": [
                            {"mentions": [str(PUMP_PROGRAM_ID)]},
                            {"commitment": "processed"}
                        ]
                    }))
                    await ws.recv()
                    while True:
                        msg = json.loads(await ws.recv())
                        if msg.get("method") != "logsNotification":
                            continue
                        logs = msg["params"]["result"]["value"].get("logs", [])
                        if any("Instruction: Create" in l for l in logs):
                            for entry in logs:
                                if entry.startswith("Program data:"):
                                    encoded = entry.split(": ", 1)[1]
                                    parsed = parse_create_instruction(base64.b64decode(encoded))
                                    if not parsed:
                                        continue
                                    now = datetime.now(timezone.utc)

                                    # --- HIER: Metadaten von der URI laden ---
                                    created_on_str = None
                                    twitter = None
                                    telegram = None
                                    website = None

                                    try:
                                        async with aiohttp.ClientSession() as session:
                                            resp = await session.get(parsed["uri"])
                                            resp.raise_for_status()
                                            metadata = await resp.json()
                                            # Felder extrahieren
                                            created_on_str = metadata.get("createdOn")
                                            twitter  = metadata.get("twitter")
                                            telegram = metadata.get("telegram")
                                            website  = metadata.get("website")
                                    except Exception as e:
                                        # bei Fehlern: Metadaten leer lassen
                                        print(f"Could not fetch metadata for {parsed['uri']}: {e}")


                                    print(f"new token found: {parsed["mint"]}")
                                    sig = msg["params"]["result"]["value"]["signature"]
                                    await conn.execute(
                                        """
                                        INSERT INTO token_creations (
                                            signature, created_at,
                                            name, symbol, uri,
                                            mint, bonding_curve, user_pubkey,
                                            metadata_created_on,
                                            metadata_twitter,
                                            metadata_telegram,
                                            metadata_website
                                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                                        """,
                                        sig, now,
                                        parsed["name"], parsed["symbol"], parsed["uri"],
                                        parsed["mint"], parsed["bondingCurve"], parsed["user"],
                                        created_on_str, twitter, telegram, website
                                    )
        except Exception as e:
            print(f"Error in token listener: {e}")
            await asyncio.sleep(5)

# --- Main & Graceful Shutdown ---

async def main():
    await init_db()
    market_task = asyncio.create_task(listen_new_markets())
    token_task = asyncio.create_task(listen_for_new_tokens())

    loop = asyncio.get_running_loop()
    def _shutdown():
        for t in [market_task, token_task] + price_tasks:
            t.cancel()

    with contextlib.suppress(NotImplementedError):
        loop.add_signal_handler(signal.SIGINT, _shutdown)
        loop.add_signal_handler(signal.SIGTERM, _shutdown)
    try:
        await asyncio.gather(market_task, token_task)
    except asyncio.CancelledError:
        print("Beende Listener, warte auf Preisaufzeichnungen...")
        await asyncio.gather(*price_tasks, return_exceptions=True)
    finally:
        await db_pool.close()
        print("Script beendet.")

if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
