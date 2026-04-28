"""SAGE OTP — multi-user Telegram dispatcher.

Runs every user's personal Telegram bot in a single async event loop.
Forwards new OTPs the moment they land in Postgres (via LISTEN/NOTIFY)
and exposes /get, /numbers, /release, /balance, /stats, /start, /help.
"""
import asyncio
import json
import logging
import os
import re
from contextlib import suppress
from datetime import datetime
from typing import Dict, Optional

import aiohttp
import asyncpg
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, Message,
    CallbackQuery, BotCommand,
)
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL  = os.environ["SUPABASE_URL"]
SERVICE_KEY   = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
DB_URL        = os.environ["SUPABASE_DB_URL"]
ACCHUB_EMAIL  = os.getenv("ACCHUB_EMAIL", "")
ACCHUB_PASS   = os.getenv("ACCHUB_PASSWORD", "")
BOT_NAME      = os.getenv("BOT_NAME", "SAGE OTP")
BANNER_URL    = os.getenv("BANNER_URL", "")

ACCHUB_HTTP   = "https://sms.acchub.io"
REFRESH_SEC   = 30  # how often to reload user-bot pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sage")
for n in ("aiogram.event", "aiogram.dispatcher"):
    logging.getLogger(n).setLevel(logging.WARNING)

# ──────────────────────────────────────────────────────────────────
# Shared state
# ──────────────────────────────────────────────────────────────────
pool: Optional[asyncpg.Pool] = None
bots: Dict[str, "BotInstance"] = {}  # user_id -> BotInstance
acchub = {"token": None, "expires": 0, "session": None}

# ──────────────────────────────────────────────────────────────────
# AccHub upstream client
# ──────────────────────────────────────────────────────────────────
async def acchub_session() -> aiohttp.ClientSession:
    if acchub["session"] is None or acchub["session"].closed:
        acchub["session"] = aiohttp.ClientSession(headers={"User-Agent": "SAGE-OTP-bot/1.0"})
    return acchub["session"]

async def acchub_login() -> Optional[str]:
    if not ACCHUB_EMAIL or not ACCHUB_PASS:
        return None
    s = await acchub_session()
    try:
        async with s.post(f"{ACCHUB_HTTP}/api/login", json={"email": ACCHUB_EMAIL, "password": ACCHUB_PASS}) as r:
            j = await r.json()
            tok = j.get("token") or j.get("data", {}).get("token")
            if tok:
                acchub["token"] = tok
                log.info("acchub login OK")
                return tok
    except Exception as e:
        log.warning("acchub login failed: %s", e)
    return None

async def acchub_get_countries():
    s = await acchub_session()
    if not acchub["token"]:
        await acchub_login()
    headers = {"Authorization": f"Bearer {acchub['token']}"}
    try:
        async with s.get(f"{ACCHUB_HTTP}/api/countries", headers=headers) as r:
            j = await r.json()
            return j.get("data") or j or []
    except Exception as e:
        log.warning("countries failed: %s", e); return []

async def acchub_get_operators(country_id: int):
    s = await acchub_session()
    headers = {"Authorization": f"Bearer {acchub['token']}"}
    try:
        async with s.get(f"{ACCHUB_HTTP}/api/operators?country_id={country_id}", headers=headers) as r:
            j = await r.json()
            return j.get("data") or j or []
    except Exception as e:
        log.warning("ops failed: %s", e); return []

async def acchub_get_number(country_id: int, operator_id: int):
    s = await acchub_session()
    headers = {"Authorization": f"Bearer {acchub['token']}"}
    try:
        async with s.post(f"{ACCHUB_HTTP}/api/numbers/get", headers=headers,
                          json={"country_id": country_id, "operator_id": operator_id}) as r:
            j = await r.json()
            return j.get("data") or j
    except Exception as e:
        log.warning("get_number failed: %s", e); return None

# ──────────────────────────────────────────────────────────────────
# Per-user bot wrapper
# ──────────────────────────────────────────────────────────────────
FLAGS = {  # truncated; full set lives in DB country_name
    "1":"🇺🇸","7":"🇷🇺","20":"🇪🇬","27":"🇿🇦","30":"🇬🇷","31":"🇳🇱","33":"🇫🇷","34":"🇪🇸",
    "39":"🇮🇹","44":"🇬🇧","49":"🇩🇪","52":"🇲🇽","55":"🇧🇷","61":"🇦🇺","62":"🇮🇩","63":"🇵🇭",
    "65":"🇸🇬","66":"🇹🇭","81":"🇯🇵","86":"🇨🇳","90":"🇹🇷","91":"🇮🇳","92":"🇵🇰","234":"🇳🇬",
    "254":"🇰🇪","255":"🇹🇿","256":"🇺🇬","263":"🇿🇼","380":"🇺🇦","971":"🇦🇪",
}

class BotInstance:
    def __init__(self, profile: dict):
        self.profile = profile
        self.user_id = profile["id"]
        self.token = profile["telegram_bot_token"]
        self.bot = Bot(self.token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher()
        self.dp.include_router(self._router())
        self.task: Optional[asyncio.Task] = None
        self.update_offset = int(profile.get("telegram_update_offset") or 0)

    # ── Buttons attached to every OTP ──
    def buttons(self) -> InlineKeyboardMarkup:
        rows = [[InlineKeyboardButton(text=f"🤖 Open Bot", url=f"https://t.me/{self.profile.get('bot_username','')}" if self.profile.get('bot_username') else "https://t.me/")]]
        if self.profile.get("channel_link"):
            rows[0].append(InlineKeyboardButton(text="📣 Join Channel", url=self.profile["channel_link"]))
        if self.profile.get("otp_group_link"):
            rows.append([InlineKeyboardButton(text="👥 OTP Group", url=self.profile["otp_group_link"])])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    # ── Force-join check ──
    async def assert_joined(self, msg: Message) -> bool:
        async with pool.acquire() as c:
            chans = await c.fetch("SELECT * FROM forced_channels WHERE active=true AND chat_id IS NOT NULL")
        missing = []
        for ch in chans:
            try:
                m = await self.bot.get_chat_member(ch["chat_id"], msg.from_user.id)
                if m.status in ("left", "kicked"):
                    missing.append(ch)
            except Exception:
                missing.append(ch)
        if missing:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"{c['icon'] or '📣'} {c['label']}", url=c["url"])] for c in missing
            ] + [[InlineKeyboardButton(text="✅ I Joined", callback_data="check_join")]])
            await msg.answer("🔒 Please join all required channels first:", reply_markup=kb)
            return False
        return True

    # ── Routes ──
    def _router(self) -> Router:
        r = Router()

        @r.message(CommandStart())
        async def start(msg: Message):
            args = msg.text.split(maxsplit=1)
            arg = args[1].strip() if len(args) > 1 else ""
            # If user is THIS bot's owner using a deep link to capture chat_id
            if arg and arg == (self.profile.get("telegram_link_code") or ""):
                async with pool.acquire() as c:
                    await c.execute(
                        "UPDATE profiles SET telegram_chat_id=$1, telegram_link_code=NULL WHERE id=$2",
                        str(msg.chat.id), self.user_id,
                    )
                self.profile["telegram_chat_id"] = str(msg.chat.id)
                await msg.answer(f"✅ <b>{BOT_NAME}</b> linked!\nEvery OTP on numbers you claim will land here.", reply_markup=self.buttons())
                return
            txt = (
                f"👋 Welcome to <b>{BOT_NAME}</b>\n\n"
                "Tap a command:\n"
                "/get — claim a number\n"
                "/numbers — your active numbers\n"
                "/balance — wallet\n"
                "/stats — your stats\n"
                "/help — full menu"
            )
            await msg.answer(txt, reply_markup=self.buttons())

        @r.message(Command("help"))
        async def help_(msg: Message):
            await msg.answer(
                "<b>Commands</b>\n"
                "/get — pick country & operator\n"
                "/numbers — your active claimed numbers\n"
                "/release [phone] — release a number\n"
                "/balance — your wallet\n"
                "/stats — your stats\n",
                reply_markup=self.buttons(),
            )

        @r.message(Command("balance"))
        async def bal(msg: Message):
            async with pool.acquire() as c:
                row = await c.fetchrow("SELECT wallet_cents, lifetime_earned_cents, total_otps FROM profiles WHERE id=$1", self.user_id)
            await msg.answer(
                f"💰 <b>Wallet</b>: ${row['wallet_cents']/100:.2f}\n"
                f"📈 <b>Lifetime</b>: ${row['lifetime_earned_cents']/100:.2f}\n"
                f"📩 <b>OTPs</b>: {row['total_otps']}",
                reply_markup=self.buttons(),
            )

        @r.message(Command("stats"))
        async def stats(msg: Message):
            async with pool.acquire() as c:
                row = await c.fetchrow("SELECT total_otps, lifetime_earned_cents FROM profiles WHERE id=$1", self.user_id)
                rank = await c.fetchval(
                    "SELECT COUNT(*)+1 FROM profiles WHERE lifetime_earned_cents > $1", row["lifetime_earned_cents"]
                )
            await msg.answer(
                f"📊 <b>Your stats</b>\n📩 OTPs: {row['total_otps']}\n💰 Earned: ${row['lifetime_earned_cents']/100:.2f}\n🏆 Rank: #{rank}",
                reply_markup=self.buttons(),
            )

        @r.message(Command("numbers"))
        async def nums(msg: Message):
            async with pool.acquire() as c:
                rows = await c.fetch(
                    "SELECT phone_number, country_name, operator, claimed_at FROM claimed_numbers "
                    "WHERE user_id=$1 AND released_at IS NULL ORDER BY claimed_at DESC LIMIT 20",
                    self.user_id,
                )
            if not rows:
                await msg.answer("No active numbers. Use /get to claim one.")
                return
            text = "<b>Your active numbers</b>\n" + "\n".join(
                f"📱 <code>{r['phone_number']}</code> — {r['country_name'] or '?'} ({r['operator'] or '?'})" for r in rows
            )
            await msg.answer(text, reply_markup=self.buttons())

        @r.message(Command("release"))
        async def release(msg: Message):
            parts = msg.text.split(maxsplit=1)
            if len(parts) < 2:
                await msg.answer("Usage: <code>/release +1234567890</code>")
                return
            phone = parts[1].strip()
            async with pool.acquire() as c:
                r = await c.execute(
                    "UPDATE claimed_numbers SET released_at=now() WHERE user_id=$1 AND phone_number=$2 AND released_at IS NULL",
                    self.user_id, phone,
                )
            await msg.answer("✅ Released" if r != "UPDATE 0" else "Number not found")

        @r.message(Command("get"))
        async def get_(msg: Message):
            if not await self.assert_joined(msg):
                return
            countries = await acchub_get_countries()
            if not countries:
                await msg.answer("⚠️ Provider unavailable. Try again in a moment.")
                return
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text=f"{FLAGS.get(str(c.get('code','')),'🌍')} {c.get('name','?')}",
                    callback_data=f"c:{c['id']}"
                )] for c in countries[:50]
            ])
            await msg.answer("🌍 <b>Pick a country</b>", reply_markup=kb)

        @r.callback_query(F.data.startswith("c:"))
        async def pick_country(cb: CallbackQuery):
            cid = int(cb.data.split(":")[1])
            ops = await acchub_get_operators(cid)
            if not ops:
                await cb.answer("No operators", show_alert=True); return
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"📡 {o.get('name','op')}", callback_data=f"o:{cid}:{o['id']}")] for o in ops[:30]
            ])
            await cb.message.edit_text("📡 <b>Pick operator</b>", reply_markup=kb)
            await cb.answer()

        @r.callback_query(F.data.startswith("o:"))
        async def pick_op(cb: CallbackQuery):
            _, cid, oid = cb.data.split(":")
            num = await acchub_get_number(int(cid), int(oid))
            phone = (num or {}).get("phone") or (num or {}).get("number")
            if not phone:
                await cb.answer("No numbers available", show_alert=True); return
            async with pool.acquire() as c:
                await c.execute(
                    "INSERT INTO claimed_numbers(user_id, phone_number, country_code, operator) VALUES ($1,$2,$3,$4)",
                    self.user_id, str(phone), str(num.get("country_code","")), str(num.get("operator","")),
                )
            await cb.message.edit_text(
                f"✅ <b>Claimed</b>\n📱 <code>{phone}</code>\n\nWaiting for OTP… you'll get it here instantly.",
                reply_markup=self.buttons(),
            )
            await cb.answer("Number claimed!")

        @r.callback_query(F.data == "check_join")
        async def check_join(cb: CallbackQuery):
            if await self.assert_joined(cb.message):
                await cb.message.edit_text("✅ Verified — send /get to claim a number.")
            await cb.answer()

        return r

    async def start(self):
        await self.bot.delete_webhook(drop_pending_updates=False)
        await self.bot.set_my_commands([
            BotCommand(command="start", description="Start / link"),
            BotCommand(command="get", description="Claim a number"),
            BotCommand(command="numbers", description="Your active numbers"),
            BotCommand(command="balance", description="Wallet"),
            BotCommand(command="stats", description="Your stats"),
            BotCommand(command="help", description="Help"),
        ])
        self.task = asyncio.create_task(self.dp.start_polling(self.bot, handle_signals=False))

    async def stop(self):
        with suppress(Exception):
            await self.dp.stop_polling()
        with suppress(Exception):
            await self.bot.session.close()
        if self.task:
            self.task.cancel()

    # ── Forward an incoming OTP to the user ──
    async def forward_otp(self, otp: dict):
        if not self.profile.get("notify_on_otp", True):
            return
        if not self.profile.get("telegram_chat_id"):
            return
        cc = re.sub(r"\D", "", otp.get("country_code", "") or "")
        flag = FLAGS.get(cc, "🌍")
        text = (
            f"📩 <b>{otp.get('service') or otp.get('provider') or 'OTP'}</b>\n"
            f"{flag} <b>{otp.get('country_name','?')}</b> (+{cc})\n"
            f"📱 <code>{otp.get('phone_number','')}</code>\n"
            f"🔑 <b><code>{otp.get('otp_code','')}</code></b>\n"
        )
        if otp.get("full_message"):
            text += f"\n<i>{str(otp['full_message'])[:300]}</i>"
        kb = self.buttons()
        for chat in [self.profile.get("telegram_chat_id"), self.profile.get("otp_group_id")]:
            if not chat:
                continue
            try:
                await self.bot.send_message(chat, text, reply_markup=kb, disable_web_page_preview=True)
            except Exception as e:
                log.warning("forward to %s failed: %s", chat, e)

# ──────────────────────────────────────────────────────────────────
# Bot pool management
# ──────────────────────────────────────────────────────────────────
async def reload_bots():
    async with pool.acquire() as c:
        rows = await c.fetch(
            "SELECT * FROM profiles WHERE telegram_bot_token IS NOT NULL AND bot_enabled = true"
        )
    seen = set()
    for p in rows:
        seen.add(p["id"])
        existing = bots.get(p["id"])
        if existing and existing.token == p["telegram_bot_token"]:
            existing.profile = dict(p)
            continue
        if existing:
            await existing.stop()
        try:
            inst = BotInstance(dict(p))
            await inst.start()
            bots[p["id"]] = inst
            log.info("✓ started bot for %s (@%s)", p["email"], p.get("bot_username"))
        except Exception as e:
            log.warning("✗ failed start bot for %s: %s", p["email"], e)
    # remove bots that are no longer enabled
    for uid in list(bots.keys()):
        if uid not in seen:
            log.info("⏹ stopping bot for %s", uid)
            await bots[uid].stop()
            del bots[uid]

# ──────────────────────────────────────────────────────────────────
# OTP listener — LISTEN/NOTIFY first, polling fallback
# ──────────────────────────────────────────────────────────────────
async def otp_listener():
    last_id = None
    async with pool.acquire() as c:
        last = await c.fetchrow("SELECT id FROM otps ORDER BY received_at DESC LIMIT 1")
        last_id = last["id"] if last else None
    log.info("📡 OTP listener active (start cursor=%s)", last_id)

    while True:
        try:
            async with pool.acquire() as c:
                rows = await c.fetch(
                    """SELECT o.*, cn.user_id AS owner_id
                       FROM otps o
                       LEFT JOIN claimed_numbers cn
                         ON cn.phone_number = o.phone_number AND cn.released_at IS NULL
                       WHERE o.received_at > now() - interval '2 minutes'
                       ORDER BY o.received_at ASC"""
                )
            for row in rows:
                if last_id and row["id"] <= last_id:
                    continue
                last_id = row["id"]
                owner = row["owner_id"]
                if owner and owner in bots:
                    asyncio.create_task(bots[owner].forward_otp(dict(row)))
        except Exception as e:
            log.warning("listener tick error: %s", e)
        await asyncio.sleep(2)

# ──────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────
async def reload_loop():
    while True:
        try:
            await reload_bots()
        except Exception as e:
            log.warning("reload error: %s", e)
        await asyncio.sleep(REFRESH_SEC)

async def main():
    global pool
    log.info("🚀 %s — multi-user dispatcher booting", BOT_NAME)
    pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)
    await acchub_login()
    await reload_bots()
    log.info("▶ loaded %d user bots", len(bots))
    await asyncio.gather(reload_loop(), otp_listener())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("bye")