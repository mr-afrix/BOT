from __future__ import annotations

import asyncio
import logging
import os
import platform
import signal
import socket
import sys
import time
from typing import Dict, Optional

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import (
    BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, Message, ReplyKeyboardMarkup,
)
from dotenv import load_dotenv

try:
    import psutil
except Exception:
    psutil = None

load_dotenv()

SITE_API        = os.environ["SITE_API"].rstrip("/")
SERVER_ID       = os.environ["SERVER_ID"]
SERVER_SEC      = os.environ["SERVER_SECRET"]
BOT_NAME        = os.environ.get("BOT_NAME", "SAGE OTP")
POLL_BOTS_EVERY = int(os.environ.get("POLL_BOTS_EVERY", "10"))
HEARTBEAT_EVERY = int(os.environ.get("HEARTBEAT_EVERY", "1"))
OTP_POLL_EVERY  = int(os.environ.get("OTP_POLL_EVERY", "1"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("sage")

bots:           Dict[str, Bot]  = {}
owner_meta:     Dict[str, dict] = {}
token_to_owner: Dict[str, str]  = {}
last_otp_seen:  Dict[str, str]  = {}

session: Optional[aiohttp.ClientSession] = None
dp     = Dispatcher()
router = Router()
dp.include_router(router)

_polling_task: Optional[asyncio.Task] = None
_polling_lock = asyncio.Lock()
START_TS = time.time()
_shutdown_event = asyncio.Event()


async def api(action: str, body: dict | None = None, *, retries: int = 3) -> dict:
    assert session
    payload = {"server_id": SERVER_ID, "server_secret": SERVER_SEC, **(body or {})}
    headers = {
        "x-server-id": SERVER_ID,
        "x-server-secret": SERVER_SEC,
        "Content-Type": "application/json",
    }
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            async with session.post(
                f"{SITE_API}?action={action}", json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                data = await r.json(content_type=None)
                if r.status >= 400:
                    log.warning("api %s -> %s %s", action, r.status, data)
                return data
        except asyncio.CancelledError:
            raise
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
    log.error("api %s failed after %d attempts: %s", action, retries, last_err)
    return {"error": str(last_err)}


def owner_of(bot: Bot) -> Optional[str]:
    return token_to_owner.get(bot.token)


def is_bot_owner(bot: Bot, tg_user_id: int) -> bool:
    oid = owner_of(bot)
    if not oid:
        return False
    meta = owner_meta.get(oid, {})
    stored = meta.get("owner_telegram_id")
    return bool(stored) and str(stored) == str(tg_user_id)


async def passes_force_join(tg_user_id: int, owner_id: str) -> tuple[bool, list[dict]]:
    res = await api("verify_main", {"owner_id": owner_id, "tg_user_id": str(tg_user_id)})
    if res.get("error"):
        return True, []
    return bool(res.get("ok")), list(res.get("missing") or [])


def force_join_kb(channels: list[dict]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"Join {c.get('label', 'channel')}", url=c["url"])]
        for c in channels if c.get("url")
    ]
    rows.append([InlineKeyboardButton(text="Verify Membership", callback_data="fj_check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def gate(msg: Message, bot: Bot) -> bool:
    oid = owner_of(bot)
    if not oid:
        return True
    ok, missing = await passes_force_join(msg.from_user.id, oid)
    if not ok:
        await msg.answer(
            "<b>Join the channels below to use this bot.</b>\nThen tap <i>Verify Membership</i>.",
            reply_markup=force_join_kb(missing),
        )
    return ok


def main_menu_kb(is_owner: bool) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Get Number"), KeyboardButton(text="My Numbers")],
        [KeyboardButton(text="Balance"),    KeyboardButton(text="Help")],
    ]
    if is_owner:
        rows.append([KeyboardButton(text="Owner Stats"), KeyboardButton(text="Broadcast")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def number_kb(phone: str, meta: dict, country_id=None, operator_id=None) -> InlineKeyboardMarkup:
    change_data = f"chg:{phone}:{country_id}:{operator_id}" if country_id and operator_id else f"chg:{phone}"
    rows = [
        [InlineKeyboardButton(text="Copy Number", copy_text={"text": phone})],
        [InlineKeyboardButton(text="Change Number", callback_data=change_data),
         InlineKeyboardButton(text="Release",       callback_data=f"rel:{phone}")],
        [InlineKeyboardButton(text="Get Another", callback_data="get_another"),
         InlineKeyboardButton(text="Back to Menu", callback_data="menu")],
    ]
    if meta.get("channel_link"):
        rows.append([InlineKeyboardButton(text="Join Channel", url=meta["channel_link"])])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def with_banner_preview(text: str, meta: dict) -> str:
    banner = str(meta.get("bot_banner_url") or "").strip()
    if not banner.startswith(("http://", "https://")):
        return text
    return f'<a href="{banner}">&#8205;</a>{text}'


async def render_number_message(
    target: Message, phone: str, country: str, operator: str, meta: dict,
    *, changed: bool = False, country_id=None, operator_id=None
):
    text = with_banner_preview((
        f"<b>{'Number changed' if changed else 'Your number is ready'}</b>\n\n"
        f"Country: {country or '?'}\n"
        f"Operator: {operator or '?'}\n"
        f"Number: <code>{phone}</code>\n\n"
        f"OTPs will arrive here automatically."
    ), meta)
    kb = number_kb(phone, meta, country_id, operator_id)
    try:
        return await target.edit_text(text, reply_markup=kb, disable_web_page_preview=False)
    except Exception:
        return await target.edit_caption(caption=text, reply_markup=kb)


def otp_kb(phone: str, otp: str, meta: dict, country_id=None, operator_id=None) -> InlineKeyboardMarkup:
    change_data = f"chg:{phone}:{country_id}:{operator_id}" if country_id and operator_id else f"chg:{phone}"
    rows = [
        [InlineKeyboardButton(text="Copy Number", copy_text={"text": phone}),
         InlineKeyboardButton(text="Copy Code",   copy_text={"text": otp})],
        [InlineKeyboardButton(text="Change Number", callback_data=change_data),
         InlineKeyboardButton(text="Release",       callback_data=f"rel:{phone}")],
        [InlineKeyboardButton(text="Get Another", callback_data="get_another")],
    ]
    extras = []
    if meta.get("bot_username"):
        extras.append(InlineKeyboardButton(text="Open Bot", url=f"https://t.me/{meta['bot_username']}"))
    if meta.get("channel_link"):
        extras.append(InlineKeyboardButton(text="Join Channel", url=meta["channel_link"]))
    if extras:
        rows.append(extras)
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
        "Use the menu to grab a free number, get OTPs, and check your balance.",
        reply_markup=main_menu_kb(is_bot_owner(bot, msg.from_user.id)),
    )


@router.callback_query(F.data == "fj_check")
async def cb_fj(cq: CallbackQuery, bot: Bot):
    oid = owner_of(bot)
    if not oid:
        return await cq.answer("Bot not linked.", show_alert=True)
    ok, _ = await passes_force_join(cq.from_user.id, oid)
    if ok:
        await cq.message.edit_text("Verified. You can use the bot now.")
        await cq.message.answer("Choose an action:", reply_markup=main_menu_kb(is_bot_owner(bot, cq.from_user.id)))
    else:
        await cq.answer("Still missing some channels.", show_alert=True)


@router.callback_query(F.data == "menu")
async def cb_menu(cq: CallbackQuery, bot: Bot):
    await cq.message.answer("Menu:", reply_markup=main_menu_kb(is_bot_owner(bot, cq.from_user.id)))


async def show_countries(source: Message | CallbackQuery, bot: Bot, page: int = 0):
    oid = owner_of(bot)
    res = await api("countries", {"owner_id": oid})
    countries = (res or {}).get("data") or []
    per   = 8
    chunk = countries[page * per:(page + 1) * per]
    rows  = [
        [InlineKeyboardButton(text=c.get("name", "?"), callback_data=f"c:{c.get('id')}")]
        for c in chunk
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="Prev", callback_data=f"cp:{page - 1}"))
    if (page + 1) * per < len(countries):
        nav.append(InlineKeyboardButton(text="Next", callback_data=f"cp:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="Back", callback_data="menu")])
    kb   = InlineKeyboardMarkup(inline_keyboard=rows)
    text = "<b>Pick a country:</b>"
    if isinstance(source, CallbackQuery):
        try:
            await source.message.edit_text(text, reply_markup=kb)
        except Exception:
            await source.message.answer(text, reply_markup=kb)
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
    oid  = owner_of(bot)
    meta = owner_meta.get(oid, {})
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
    await render_number_message(
        cq.message, phone, res.get("country", "?"), res.get("operator", "?"), meta,
        country_id=res.get("country_id") or country_id,
        operator_id=res.get("operator_id") or operator_id,
    )


@router.callback_query(F.data.startswith("chg:"))
async def cb_change(cq: CallbackQuery, bot: Bot):
    parts = cq.data.split(":")
    phone = parts[1]
    country_id  = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
    operator_id = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None
    oid  = owner_of(bot)
    meta = owner_meta.get(oid, {})
    await cq.answer("Changing number…")
    res = await api("change_number", {
        "owner_id":    oid,
        "tg_user_id":  str(cq.from_user.id),
        "phone":       phone,
        "country_id":  country_id,
        "operator_id": operator_id,
    })
    if res.get("error"):
        return await cq.message.edit_text(f"Could not change: <code>{res['error']}</code>")
    new = res["phone"]
    await render_number_message(
        cq.message, new, res.get("country", "?"), res.get("operator", "?"), meta, changed=True,
        country_id=res.get("country_id") or country_id,
        operator_id=res.get("operator_id") or operator_id,
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
    try:
        await cq.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


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
        f"Balance: <b>${bal:.4f}</b>\n"
        f"Lifetime: ${life:.4f}\n"
        f"OTPs received: {res.get('total_otps', 0)}\n"
        f"Numbers claimed: {res.get('total_numbers', 0)}"
    )


@router.message(F.text.in_({"Help", "/help"}))
async def m_help(msg: Message, _: Bot):
    await msg.answer(
        "<b>Commands</b>\n\n"
        "/get — pick country &amp; operator, get a free number\n"
        "/numbers — list your active numbers\n"
        "/balance — wallet &amp; stats\n"
        "/help — show this list\n\n"
        "Every OTP that lands on your number is forwarded here instantly."
    )


@router.message(F.text == "Owner Stats")
async def m_ownstats(msg: Message, bot: Bot):
    if not is_bot_owner(bot, msg.from_user.id):
        return
    res = await api("balance", {"owner_id": owner_of(bot)})
    await msg.answer(
        f"<b>Bot stats</b>\n"
        f"Lifetime earned: ${res.get('lifetime_cents', 0) / 100:.4f}\n"
        f"OTPs total: {res.get('total_otps', 0)}\n"
        f"Numbers claimed: {res.get('total_numbers', 0)}"
    )


async def _close_bot(b: Bot) -> None:
    try:
        await b.session.close()
    except Exception:
        pass


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


async def _stop_polling() -> None:
    global _polling_task
    if _polling_task and not _polling_task.done():
        _polling_task.cancel()
        try:
            await _polling_task
        except (asyncio.CancelledError, Exception):
            pass
    _polling_task = None


async def reload_bots() -> None:
    global _polling_task
    res  = await api("active_bots", {})
    rows = res.get("data") or []
    seen_tokens: set[str] = set()
    pool_changed = False

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
                pool_changed = True

    dropped = [oid for oid, b in list(bots.items()) if b.token not in seen_tokens]
    for oid in dropped:
        b = bots.pop(oid, None)
        owner_meta.pop(oid, None)
        if b:
            token_to_owner.pop(b.token, None)
            await _close_bot(b)
        log.info("removed bot for owner %s", oid)
        pool_changed = True

    if not pool_changed:
        return

    async with _polling_lock:
        await _stop_polling()
        if bots:
            _polling_task = asyncio.create_task(_run_polling())
            log.info("polling restarted with %d bots", len(bots))


async def _run_polling() -> None:
    if not bots:
        return
    try:
        await dp.start_polling(*bots.values(), handle_signals=False)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error("polling crashed: %s", e)


async def poll_otps_for(owner_id: str, bot: Bot) -> None:
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
        text  = with_banner_preview((
            f"<b>New OTP</b>\n\n"
            f"Number:  <code>{phone}</code>\n"
            f"Code:    <code>{otp}</code>\n"
            f"Service: {r.get('service', '?')}\n"
            f"Country: {r.get('country_name') or r.get('country_code') or '?'}\n\n"
            f"<i>{(r.get('full_message') or '')[:300]}</i>"
        ), meta)
        kb = otp_kb(phone, otp, meta, r.get("country_id"), r.get("operator_id"))
        for t in targets:
            try:
                await bot.send_message(t, text, reply_markup=kb, disable_web_page_preview=False)
            except Exception as e:
                log.warning("send to %s failed: %s", t, e)
    last_otp_seen[owner_id] = rows[-1]["received_at"]


def _gather_metrics() -> dict:
    m: dict = {
        "uptime_s":       int(time.time() - START_TS),
        "hostname":       socket.gethostname()[:80],
        "platform":       f"{platform.system()} {platform.release()}"[:80],
        "python_version": sys.version.split()[0],
    }
    if psutil:
        try:
            vm = psutil.virtual_memory()
            m["ram_mb"]       = int(vm.used / (1024 * 1024))
            m["ram_total_mb"] = int(vm.total / (1024 * 1024))
            m["cpu_pct"]      = round(psutil.cpu_percent(interval=None), 1)
            m["disk_pct"]     = round(psutil.disk_usage("/").percent, 1)
        except Exception:
            pass
    return m


async def heartbeat_loop() -> None:
    while not _shutdown_event.is_set():
        try:
            await api("heartbeat", {
                "bots_loaded": len(bots),
                "metrics": _gather_metrics(),
            })
        except Exception as e:
            log.warning("heartbeat: %s", e)
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=HEARTBEAT_EVERY)
        except asyncio.TimeoutError:
            pass


async def reload_loop() -> None:
    while not _shutdown_event.is_set():
        try:
            await reload_bots()
        except Exception as e:
            log.error("reload: %s", e)
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=POLL_BOTS_EVERY)
        except asyncio.TimeoutError:
            pass


async def otp_loop() -> None:
    while not _shutdown_event.is_set():
        try:
            for oid, b in list(bots.items()):
                await poll_otps_for(oid, b)
        except Exception as e:
            log.error("otp loop: %s", e)
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=OTP_POLL_EVERY)
        except asyncio.TimeoutError:
            pass


async def shutdown() -> None:
    if _shutdown_event.is_set():
        return
    log.info("shutting down…")
    _shutdown_event.set()
    async with _polling_lock:
        await _stop_polling()
    for b in list(bots.values()):
        await _close_bot(b)
    bots.clear()
    if session and not session.closed:
        await session.close()
    log.info("shutdown complete")


async def main() -> None:
    global session

    connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
    session = aiohttp.ClientSession(connector=connector)

    if psutil:
        psutil.cpu_percent(interval=None)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))
        except NotImplementedError:
            pass

    await reload_bots()
    log.info("▶ %s loaded %d user bots on server %s", BOT_NAME, len(bots), SERVER_ID)

    try:
        await asyncio.gather(
            heartbeat_loop(),
            reload_loop(),
            otp_loop(),
        )
    except asyncio.CancelledError:
        pass
    finally:
        await shutdown()


if __name__ == "__main__":
    asyncio.run(main())
