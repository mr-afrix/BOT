from __future__ import annotations

import asyncio
import logging
import os
from typing import Dict, Optional

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, Message, ReplyKeyboardMarkup,
)
from dotenv import load_dotenv

load_dotenv()

SITE_API         = os.environ["SITE_API"].rstrip("/")
SERVER_ID        = os.environ["SERVER_ID"]
SERVER_SEC       = os.environ["SERVER_SECRET"]
BOT_NAME         = os.environ.get("BOT_NAME", "SAGE OTP")
POLL_BOTS_EVERY  = int(os.environ.get("POLL_BOTS_EVERY", "10"))
HEARTBEAT_EVERY  = int(os.environ.get("HEARTBEAT_EVERY", "1"))
OTP_POLL_EVERY   = int(os.environ.get("OTP_POLL_EVERY", "1"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sage")

bots:           Dict[str, Bot]  = {}
owner_meta:     Dict[str, dict] = {}
token_to_owner: Dict[str, str]  = {}
last_otp_seen:  Dict[str, str]  = {}

session:  Optional[aiohttp.ClientSession] = None
dp        = Dispatcher()
router    = Router()
dp.include_router(router)

_polling_task: Optional[asyncio.Task] = None
_polling_lock = asyncio.Lock()


# ── API client ────────────────────────────────────────────────────────────────

async def api(action: str, body: dict | None = None) -> dict:
    assert session
    payload = {"server_id": SERVER_ID, "server_secret": SERVER_SEC, **(body or {})}
    headers = {
        "x-server-id": SERVER_ID,
        "x-server-secret": SERVER_SEC,
        "Content-Type": "application/json",
    }
    try:
        async with session.post(
            f"{SITE_API}?action={action}", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=20)
        ) as r:
            data = await r.json(content_type=None)
            if r.status >= 400:
                log.warning("api %s -> %s %s", action, r.status, data)
            return data
    except Exception as e:
        log.error("api %s failed: %s", action, e)
        return {"error": str(e)}


# ── Owner resolution ──────────────────────────────────────────────────────────

def owner_of(bot: Bot) -> Optional[str]:
    return token_to_owner.get(bot.token)

def is_bot_owner(bot: Bot, tg_user_id: int) -> bool:
    oid = owner_of(bot)
    if not oid:
        return False
    meta = owner_meta.get(oid, {})
    stored = meta.get("owner_telegram_id")
    return bool(stored) and str(stored) == str(tg_user_id)


# ── Force-join ────────────────────────────────────────────────────────────────

async def passes_force_join(bot: Bot, user_id: int) -> tuple[bool, list[dict]]:
    oid = owner_of(bot)
    if not oid:
        return True, []
    res   = await api("needs_join", {"owner_id": oid})
    chans = res.get("channels") or []
    missing = []
    for ch in chans:
        cid = ch.get("chat_id")
        if not cid:
            continue
        try:
            m = await bot.get_chat_member(cid, user_id)
            if m.status in ("left", "kicked"):
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return len(missing) == 0, missing

def force_join_kb(channels: list[dict]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"Join {c.get('label', 'channel')}", url=c["url"])]
        for c in channels if c.get("url")
    ]
    rows.append([InlineKeyboardButton(text="Check membership", callback_data="fj_check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── Keyboards ─────────────────────────────────────────────────────────────────

def main_menu_kb(is_owner: bool) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Get Number"), KeyboardButton(text="My Numbers")],
        [KeyboardButton(text="Balance"),    KeyboardButton(text="Help")],
    ]
    if is_owner:
        rows.append([KeyboardButton(text="Owner Stats"), KeyboardButton(text="Broadcast")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def number_kb(phone: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Copy Number", copy_text={"text": phone})],
        [InlineKeyboardButton(text="Get Another", callback_data="get_another"),
         InlineKeyboardButton(text="Release",     callback_data=f"rel:{phone}")],
        [InlineKeyboardButton(text="Back to Menu", callback_data="menu")],
    ])

def otp_kb(phone: str, otp: str, meta: dict) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="Copy Number", copy_text={"text": phone}),
         InlineKeyboardButton(text="Copy Code",   copy_text={"text": otp})],
        [InlineKeyboardButton(text="Get Another", callback_data="get_another"),
         InlineKeyboardButton(text="Release",     callback_data=f"rel:{phone}")],
    ]
    extras = []
    if meta.get("bot_username"):
        extras.append(InlineKeyboardButton(text="Open Bot",     url=f"https://t.me/{meta['bot_username']}"))
    if meta.get("channel_link"):
        extras.append(InlineKeyboardButton(text="Join Channel", url=meta["channel_link"]))
    if extras:
        rows.append(extras)
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── Gate ──────────────────────────────────────────────────────────────────────

async def gate(msg: Message, bot: Bot) -> bool:
    ok, missing = await passes_force_join(bot, msg.from_user.id)
    if not ok:
        await msg.answer(
            "<b>Join the channels below to use this bot:</b>\nThen tap <i>Check membership</i>.",
            reply_markup=force_join_kb(missing),
        )
    return ok


# ── Handlers ──────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: Message, bot: Bot):
    oid = owner_of(bot)
    if not oid:
        return await msg.answer("This bot is not linked yet.")
    await api("register_bot_user", {
        "owner_id":    oid,
        "tg_chat_id":  str(msg.chat.id),
        "tg_user_id":  str(msg.from_user.id),
        "tg_username": msg.from_user.username,
    })
    if not await gate(msg, bot):
        return
    await msg.answer(
        f"<b>Welcome to {BOT_NAME}</b>\n\n"
        "Use the menu below to grab a free number, get OTPs, and check your balance.",
        reply_markup=main_menu_kb(is_bot_owner(bot, msg.from_user.id)),
    )

@router.callback_query(F.data == "fj_check")
async def cb_fj(cq: CallbackQuery, bot: Bot):
    ok, _ = await passes_force_join(bot, cq.from_user.id)
    if ok:
        await cq.message.edit_text("Verified. You can use the bot now.")
        await cq.message.answer("Choose an action:", reply_markup=main_menu_kb(is_bot_owner(bot, cq.from_user.id)))
    else:
        await cq.answer("Still missing some channels.", show_alert=True)

@router.callback_query(F.data == "menu")
async def cb_menu(cq: CallbackQuery, bot: Bot):
    await cq.message.answer("Menu:", reply_markup=main_menu_kb(is_bot_owner(bot, cq.from_user.id)))


# ── Get Number flow ───────────────────────────────────────────────────────────

async def show_countries(source: Message | CallbackQuery, bot: Bot, page: int = 0):
    oid = owner_of(bot)
    res      = await api("countries", {"owner_id": oid})
    countries = (res or {}).get("data") or []
    per   = 8
    chunk = countries[page * per:(page + 1) * per]
    rows  = [
        [InlineKeyboardButton(text=c.get("name", "?"), callback_data=f"c:{c.get('id')}")]
        for c in chunk
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="« Prev", callback_data=f"cp:{page - 1}"))
    if (page + 1) * per < len(countries):
        nav.append(InlineKeyboardButton(text="Next »", callback_data=f"cp:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="Back", callback_data="menu")])
    kb   = InlineKeyboardMarkup(inline_keyboard=rows)
    text = "<b>Pick a country:</b>"
    if isinstance(source, CallbackQuery):
        await source.message.edit_text(text, reply_markup=kb)
    else:
        await source.answer(text, reply_markup=kb)

@router.message(F.text.in_({"Get Number", "/get"}))
async def m_get(msg: Message, bot: Bot):
    if not await gate(msg, bot):
        return
    await show_countries(msg, bot, 0)

@router.callback_query(F.data.startswith("cp:"))
async def cb_cpage(cq: CallbackQuery, bot: Bot):
    await show_countries(cq, bot, int(cq.data.split(":")[1]))

@router.callback_query(F.data == "get_another")
async def cb_again(cq: CallbackQuery, bot: Bot):
    if not await gate(cq.message, bot):
        return
    await show_countries(cq, bot, 0)

@router.callback_query(F.data.startswith("c:"))
async def cb_country(cq: CallbackQuery, bot: Bot):
    country_id = int(cq.data.split(":")[1])
    oid        = owner_of(bot)
    res        = await api("operators", {"owner_id": oid, "country_id": country_id})
    ops        = (res or {}).get("data") or []
    if not ops:
        return await cq.answer("No operators available.", show_alert=True)
    rows = [
        [InlineKeyboardButton(text=op.get("name", "?"), callback_data=f"o:{country_id}:{op.get('id')}")]
        for op in ops[:20]
    ]
    rows.append([InlineKeyboardButton(text="Back", callback_data="get_another")])
    await cq.message.edit_text("<b>Pick an operator:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@router.callback_query(F.data.startswith("o:"))
async def cb_operator(cq: CallbackQuery, bot: Bot):
    _, country_id, operator_id = cq.data.split(":")
    oid = owner_of(bot)
    await cq.message.edit_text("Provisioning number…")
    res = await api("get_number", {
        "owner_id":    oid,
        "tg_user_id":  str(cq.from_user.id),
        "country_id":  int(country_id),
        "operator_id": int(operator_id),
    })
    if res.get("error"):
        return await cq.message.edit_text(f"Could not get a number: <code>{res['error']}</code>")
    phone = res["phone"]
    await cq.message.edit_text(
        f"<b>Your number is ready</b>\n\n"
        f"Country: {res.get('country', '?')}\n"
        f"Operator: {res.get('operator', '?')}\n"
        f"Number: <code>{phone}</code>\n\n"
        f"OTPs will arrive here automatically.",
        reply_markup=number_kb(phone),
    )

@router.callback_query(F.data.startswith("rel:"))
async def cb_release(cq: CallbackQuery, bot: Bot):
    phone = cq.data.split(":", 1)[1]
    await api("release", {
        "owner_id":   owner_of(bot),
        "tg_user_id": str(cq.from_user.id),
        "phone":      phone,
    })
    await cq.answer("Released.")
    await cq.message.edit_reply_markup(reply_markup=None)

@router.message(F.text.in_({"My Numbers", "/numbers"}))
async def m_nums(msg: Message, bot: Bot):
    if not await gate(msg, bot):
        return
    res  = await api("my_numbers", {"owner_id": owner_of(bot), "tg_user_id": str(msg.from_user.id)})
    rows = (res or {}).get("data") or []
    if not rows:
        return await msg.answer("No active numbers.")
    txt = "<b>Active numbers:</b>\n" + "\n".join(
        f"• <code>{r['phone_number']}</code> — {r.get('country_name', '?')} / {r.get('operator', '?')}"
        for r in rows
    )
    await msg.answer(txt)

@router.message(F.text.in_({"Balance", "/balance"}))
async def m_bal(msg: Message, bot: Bot):
    if not await gate(msg, bot):
        return
    res = await api("balance", {"owner_id": owner_of(bot), "tg_user_id": str(msg.from_user.id)})
    if res.get("error"):
        return await msg.answer(f"Error: {res['error']}")
    bal  = res.get("wallet_cents", 0) / 100
    life = res.get("lifetime_cents", 0) / 100
    await msg.answer(
        f"<b>Wallet</b>\n\n"
        f"Balance: <b>${bal:.2f}</b>\n"
        f"Lifetime: ${life:.2f}\n"
        f"OTPs received: {res.get('total_otps', 0)}\n"
        f"Numbers claimed: {res.get('total_numbers', 0)}"
    )

@router.message(F.text.in_({"Help", "/help"}))
async def m_help(msg: Message, bot: Bot):
    await msg.answer(
        "<b>Commands</b>\n\n"
        "/get — pick country & operator, get a free number\n"
        "/numbers — list your active numbers\n"
        "/balance — wallet & stats\n"
        "/release — release a number\n\n"
        "Every OTP that lands on your number is forwarded here instantly."
    )

@router.message(F.text == "Owner Stats")
async def m_ownstats(msg: Message, bot: Bot):
    if not is_bot_owner(bot, msg.from_user.id):
        return
    res = await api("balance", {"owner_id": owner_of(bot)})
    await msg.answer(
        f"<b>Bot stats</b>\n"
        f"Users earned: ${res.get('lifetime_cents', 0) / 100:.2f}\n"
        f"OTPs total: {res.get('total_otps', 0)}"
    )


# ── Pool management ───────────────────────────────────────────────────────────

async def _init_bot(row: dict) -> Bot | None:
    token = row.get("telegram_bot_token")
    if not token:
        return None
    try:
        b = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        await b.set_my_commands([
            BotCommand(command="start",   description="Start"),
            BotCommand(command="get",     description="Get a free number"),
            BotCommand(command="numbers", description="My numbers"),
            BotCommand(command="balance", description="Wallet & stats"),
            BotCommand(command="help",    description="Help"),
        ])
        log.info("registered bot for owner %s", row["id"])
        return b
    except Exception as e:
        log.error("could not init bot %s: %s", row["id"], e)
        return None

async def reload_bots():
    global _polling_task

    res  = await api("active_bots", {})
    rows = res.get("data") or []

    seen_tokens: set[str] = set()
    new_bots_added = False

    for row in rows:
        token = row.get("telegram_bot_token")
        if not token:
            continue
        seen_tokens.add(token)
        owner_meta[row["id"]] = row

        if token not in token_to_owner:
            token_to_owner[token] = row["id"]

        if row["id"] not in bots:
            b = await _init_bot(row)
            if b:
                bots[row["id"]] = b
                new_bots_added = True

    dropped = [oid for oid, b in list(bots.items()) if b.token not in seen_tokens]
    for oid in dropped:
        b = bots.pop(oid, None)
        owner_meta.pop(oid, None)
        token_to_owner.pop(b.token if b else "", None)
        if b:
            await b.session.close()
        log.info("removed bot for owner %s", oid)
        new_bots_added = True

    if new_bots_added and bots:
        async with _polling_lock:
            if _polling_task and not _polling_task.done():
                _polling_task.cancel()
                try:
                    await _polling_task
                except asyncio.CancelledError:
                    pass
            _polling_task = asyncio.create_task(_run_polling())
            log.info("polling restarted with %d bots", len(bots))


async def _run_polling():
    if not bots:
        return
    try:
        await dp.start_polling(*bots.values(), handle_signals=False)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error("polling crashed: %s", e)


# ── OTP delivery ──────────────────────────────────────────────────────────────

async def poll_otps_for(owner_id: str, bot: Bot):
    res  = await api("recent_otps", {"owner_id": owner_id})
    rows = res.get("data") or []
    if not rows:
        return
    rows = list(reversed(rows))
    last = last_otp_seen.get(owner_id, "")
    new  = [r for r in rows if r.get("received_at", "") > last]
    if not new:
        return

    meta     = owner_meta.get(owner_id, {})
    chat_id  = meta.get("telegram_chat_id")
    group_id = meta.get("otp_group_id")
    targets  = [t for t in [chat_id, group_id] if t]
    if not targets:
        last_otp_seen[owner_id] = rows[-1]["received_at"]
        return

    for r in new:
        phone = r["phone_number"] if r["phone_number"].startswith("+") else f"+{r['phone_number']}"
        otp   = r["otp_code"]
        text  = (
            f"<b>New OTP</b>\n\n"
            f"Number: <code>{phone}</code>\n"
            f"Code: <code>{otp}</code>\n"
            f"Service: {r.get('service', '?')}\n"
            f"Country: {r.get('country_name') or r.get('country_code') or '?'}\n\n"
            f"<i>{(r.get('full_message') or '')[:300]}</i>"
        )
        kb = otp_kb(phone, otp, meta)
        for t in targets:
            try:
                await bot.send_message(t, text, reply_markup=kb, disable_web_page_preview=True)
            except Exception as e:
                log.warning("send otp to %s failed: %s", t, e)

    last_otp_seen[owner_id] = rows[-1]["received_at"]


# ── Background loops ──────────────────────────────────────────────────────────

async def heartbeat_loop():
    while True:
        try:
            await api("heartbeat", {"bots_loaded": len(bots)})
        except Exception as e:
            log.warning("heartbeat: %s", e)
        await asyncio.sleep(HEARTBEAT_EVERY)

async def reload_loop():
    while True:
        try:
            await reload_bots()
        except Exception as e:
            log.error("reload: %s", e)
        await asyncio.sleep(POLL_BOTS_EVERY)

async def otp_loop():
    while True:
        try:
            for oid, b in list(bots.items()):
                await poll_otps_for(oid, b)
        except Exception as e:
            log.error("otp loop: %s", e)
        await asyncio.sleep(OTP_POLL_EVERY)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    global session
    session = aiohttp.ClientSession()

    await reload_bots()
    log.info("▶ %s loaded %d user bots", BOT_NAME, len(bots))

    await asyncio.gather(
        heartbeat_loop(),
        reload_loop(),
        otp_loop(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
