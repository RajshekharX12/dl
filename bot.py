#!/usr/bin/env python3
# bot.py
import asyncio
import os
import re
import time
import uuid
from contextlib import suppress
from typing import Optional, Dict, List, Tuple
from urllib.parse import urlparse, urljoin
import mimetypes
import math

from dotenv import load_dotenv
load_dotenv()

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, FSInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# robust HTTP session for uploads (fixes "Cannot write to closing transport")
from aiogram.client.session.aiohttp import AiohttpSession
from aiohttp import ClientTimeout, TCPConnector

from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

import yt_dlp
import requests
from http.cookiejar import MozillaCookieJar

# ===================== Config =====================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("Set BOT_TOKEN in .env")

DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "downloads")
MAX_FILE_MB = int(os.environ.get("MAX_FILE_MB", "1900"))
DEFAULT_MODE = os.environ.get("DEFAULT_UPLOAD_MODE", "video").strip().lower()  # video|document
COOKIES_DIR = os.path.join(os.getcwd(), "cookies")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(COOKIES_DIR, exist_ok=True)

URL_RE = re.compile(r"(https?://[^\s]+)", re.IGNORECASE)

# ===================== Aiogram =====================
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)
g_bot: Optional[Bot] = None

JOBS: Dict[str, dict] = {}

# ===================== Helpers =====================
def esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def fmt_bytes(n: Optional[float]) -> str:
    if n is None:
        return "?"
    for u in ("B","KB","MB","GB","TB"):
        if n < 1024: return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"

def fmt_eta(sec: Optional[float]) -> str:
    if sec is None: return "?"
    sec = max(0, int(sec))
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")

def looks_video(path: str) -> bool:
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    return ext in {"mp4","mkv","webm","mov","m4v"}

def file_too_large(path: str) -> bool:
    try:
        return os.path.getsize(path) > MAX_FILE_MB * 1024 * 1024
    except FileNotFoundError:
        return False

def bar(pct: float, width: int = 20) -> str:
    pct = max(0.0, min(100.0, pct))
    fill = int(width * pct / 100.0)
    return "[" + "#" * fill + "–" * (width - fill) + "]"

def schedule_edit(loop: asyncio.AbstractEventLoop, message: Message, text: str, reply_markup=None):
    async def _edit():
        with suppress(Exception):
            await message.edit_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    asyncio.run_coroutine_threadsafe(_edit(), loop)

def host_title(url: str) -> str:
    host = urlparse(url).netloc or "video"
    return f"{host} video"

def common_headers(url: str) -> dict:
    return {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0 Safari/537.36"),
        "Referer": url,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "DNT": "1",
        "Connection": "keep-alive",
    }

async def http_get_text_async(url: str) -> str:
    def _get():
        r = requests.get(url, headers=common_headers(url), timeout=25, allow_redirects=True)
        r.raise_for_status()
        return r.text
    return await asyncio.to_thread(_get)

def find_direct_media(html: str, base_url: str) -> Tuple[List[str], List[str]]:
    m3u8 = set()
    mp4 = set()
    for m in re.findall(r'<source[^>]+src=["\']([^"\']+)["\']', html, re.I):
        u = urljoin(base_url, m)
        if ".m3u8" in u: m3u8.add(u)
        if u.lower().endswith(".mp4"): mp4.add(u)
    for m in re.findall(r'(?:src|file|hls|url)\s*[:=]\s*["\'](http[^"\']+)["\']', html, re.I):
        u = urljoin(base_url, m)
        if ".m3u8" in u: m3u8.add(u)
        if ".mp4" in u: mp4.add(u)
    for m in re.findall(r'https?://[^\s"\']+\.m3u8[^\s"\']*', html, re.I):
        m3u8.add(m)
    for m in re.findall(r'https?://[^\s"\']+\.mp4[^\s"\']*', html, re.I):
        mp4.add(m)
    return list(m3u8), list(mp4)

def m3u8_heights(m3u8_text: str) -> List[int]:
    hs = set()
    for line in m3u8_text.splitlines():
        m = re.search(r"RESOLUTION=\s*\d+x(\d+)", line)
        if m:
            hs.add(int(m.group(1)))
    return sorted(hs, reverse=True)

def is_cf_block(err_text: str) -> bool:
    t = (err_text or "").lower()
    return ("cloudflare" in t and ("403" in t or "challenge" in t)) or ("403" in t and "forbidden" in t)

# ---------- Cookies helpers ----------
def clean_domain(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    if s.startswith("http"):
        s = urlparse(s).netloc
    s = s.split("/")[0].lower().lstrip(".")
    return s or None

def cookie_path_for_domain(domain: str) -> str:
    return os.path.join(COOKIES_DIR, f"{domain}.txt")

def list_cookie_domains() -> List[str]:
    items = []
    for name in os.listdir(COOKIES_DIR):
        if name.endswith(".txt"):
            items.append(name[:-4])
    items.sort()
    return items

def find_cookie_for_url(url: str) -> Optional[str]:
    host = urlparse(url).netloc.lower()
    parts = host.split(".")
    candidates = [host] + [".".join(parts[i:]) for i in range(len(parts)-1)]
    for d in candidates:
        p = cookie_path_for_domain(d)
        if os.path.isfile(p):
            return p
    fallback = os.path.join(os.getcwd(), "cookies.txt")
    if os.path.isfile(fallback):
        return fallback
    return None

def cookiejar_for_url(url: str) -> Optional[MozillaCookieJar]:
    path = find_cookie_for_url(url)
    if not path:
        return None
    try:
        jar = MozillaCookieJar()
        jar.load(path, ignore_discard=True, ignore_expires=True)
        return jar
    except Exception:
        return None

# ---------- Accept raw Cookie: header or curl -H Cookie ----------
import time as _time
_COOKIE_HDR_RE = re.compile(r"(?:^|\b)Cookie:\s*([^\"'\r\n]+)", re.I)
_CURL_COOKIE_RE = re.compile(r"-H\s*[\"']?Cookie:\s*([^\"']+)[\"']?", re.I)

def cookie_text_to_netscape(txt: str, domain: str) -> Optional[str]:
    src = (txt or "").strip()
    m = _CURL_COOKIE_RE.search(src) or _COOKIE_HDR_RE.search(src)
    if m:
        src = m.group(1)
    if "=" not in src and ";" not in src:
        return None
    pairs = []
    for part in src.strip().strip(";").split(";"):
        if "=" not in part:
            continue
        name, val = part.split("=", 1)
        name = name.strip()
        val = val.strip()
        if not name:
            continue
        pairs.append((name, val))
    if not pairs:
        return None
    exp = int(_time.time()) + 180 * 24 * 3600  # 180 days
    dom = "." + domain.lstrip(".")
    lines = ["# Netscape HTTP Cookie File"]
    for name, val in pairs:
        lines.append(f"{dom}\tTRUE\t/\tFALSE\t{exp}\t{name}\t{val}")
    return "\n".join(lines) + "\n"

# ===================== Keyboards (inline) =====================
def main_menu_kb() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="➕ Cookies", callback_data="menu:cookies"),
        InlineKeyboardButton(text="⚙️ Settings", callback_data="menu:settings"),
    )
    kb.row(
        InlineKeyboardButton(text="📊 Status", callback_data="menu:status"),
        InlineKeyboardButton(text="🧹 Purge", callback_data="menu:purge"),
    )
    kb.row(InlineKeyboardButton(text="ℹ️ About", callback_data="menu:about"))
    return kb

def cookies_menu_kb() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="📜 List", callback_data="cookies:list"),
        InlineKeyboardButton(text="➕ Add", callback_data="cookies:add"),
    )
    kb.row(
        InlineKeyboardButton(text="🗑️ Delete", callback_data="cookies:del"),
        InlineKeyboardButton(text="🔥 Clear ALL", callback_data="cookies:clear"),
    )
    kb.row(InlineKeyboardButton(text="⬅️ Back", callback_data="menu:root"))
    return kb

def settings_menu_kb(current_mode: str, max_mb: int) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text=f"Upload: {current_mode}", callback_data="settings:mode"),
        InlineKeyboardButton(text=f"Max: {max_mb} MB", callback_data="settings:max"),
    )
    kb.row(InlineKeyboardButton(text="⬅️ Back", callback_data="menu:root"))
    return kb

def setmax_choices_kb() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    for v in (512, 1024, 1536, 1800, 1900):
        kb.add(InlineKeyboardButton(text=f"{v} MB", callback_data=f"settings:max:set:{v}"))
    kb.adjust(3)
    kb.row(InlineKeyboardButton(text="✍️ Custom…", callback_data="settings:max:custom"))
    kb.row(InlineKeyboardButton(text="⬅️ Back", callback_data="menu:settings"))
    return kb

def kb_format_choices(job_id: str, heights: List[int], include_best: bool = True):
    kb = InlineKeyboardBuilder()
    if include_best:
        kb.add(InlineKeyboardButton(text="Best", callback_data=f"get:{job_id}:best"))
    for h in sorted(set(heights), reverse=True):
        if h:
            kb.add(InlineKeyboardButton(text=f"{h}p", callback_data=f"get:{job_id}:h{h}"))
    if heights:
        kb.adjust(3)
    kb.row(InlineKeyboardButton(text="❌ Cancel", callback_data=f"cancel:{job_id}"))
    return kb.as_markup()

def confirm_kb(prefix: str) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Confirm", callback_data=f"{prefix}:confirm"),
        InlineKeyboardButton(text="✖️ Cancel", callback_data=f"{prefix}:cancel"),
    )
    return kb

# ===================== States =====================
class CookieAddStates(StatesGroup):
    waiting_domain = State()
    waiting_body = State()

class CookieDelStates(StatesGroup):
    waiting_domain = State()

class SetMaxState(StatesGroup):
    waiting_custom = State()

# ===================== Commands -> Inline Menus =====================
@router.message(Command("start"))
async def start_cmd(m: Message):
    txt = (
        "Send a video/page URL to download.\n"
        "I’ll show qualities (Best/1080p/720p/…), then download with live <b>% / speed / ETA</b> and send it.\n\n"
        "Use the inline menu below for cookies, settings, status and cleanup."
    )
    await m.reply(txt, parse_mode="HTML", reply_markup=main_menu_kb().as_markup())

@router.message(Command("menu"))
async def menu_cmd(m: Message):
    await m.reply("Main menu:", reply_markup=main_menu_kb().as_markup())

@router.message(Command("help"))
async def help_cmd(m: Message):
    await start_cmd(m)

# ===================== Inline Menu Callbacks =====================
@router.callback_query(F.data == "menu:root")
async def menu_root(cq: CallbackQuery):
    await cq.answer()
    with suppress(Exception):
        await cq.message.edit_text("Main menu:", reply_markup=main_menu_kb().as_markup())

@router.callback_query(F.data == "menu:cookies")
async def menu_cookies(cq: CallbackQuery):
    await cq.answer()
    with suppress(Exception):
        await cq.message.edit_text("Cookies manager:", reply_markup=cookies_menu_kb().as_markup())

@router.callback_query(F.data == "menu:settings")
async def menu_settings(cq: CallbackQuery):
    await cq.answer()
    with suppress(Exception):
        await cq.message.edit_text(
            "Settings:", reply_markup=settings_menu_kb(DEFAULT_MODE, MAX_FILE_MB).as_markup()
        )

@router.callback_query(F.data == "menu:status")
async def menu_status(cq: CallbackQuery):
    await cq.answer()
    if not JOBS:
        text = "No active jobs."
    else:
        lines = [f"• <code>{jid}</code> — {esc(j.get('title','?'))}" for jid, j in JOBS.items()]
        text = "<b>Active jobs</b>:\n" + "\n".join(lines)
    with suppress(Exception):
        await cq.message.edit_text(text, parse_mode="HTML", reply_markup=main_menu_kb().as_markup())

@router.callback_query(F.data == "menu:purge")
async def menu_purge(cq: CallbackQuery):
    await cq.answer()
    with suppress(Exception):
        await cq.message.edit_text("Delete all files in downloads?", reply_markup=confirm_kb("purge").as_markup())

@router.callback_query(F.data == "menu:about")
async def menu_about(cq: CallbackQuery):
    await cq.answer()
    text = (
        "This bot uses <code>yt-dlp</code> (supports many sites, incl. adult ones).\n"
        "For login/18+ sites, add cookies via the Cookies menu.\n"
        "No bypass for DRM/paywalls/private Telegram channels.\n"
        "Tip: send the direct video page URL."
    )
    with suppress(Exception):
        await cq.message.edit_text(text, parse_mode="HTML", reply_markup=main_menu_kb().as_markup())

# ===================== Purge confirm =====================
@router.callback_query(F.data.in_({"purge:confirm", "purge:cancel"}))
async def purge_confirm(cq: CallbackQuery):
    act = cq.data.split(":")[1]
    await cq.answer()
    if act == "cancel":
        with suppress(Exception):
            await cq.message.edit_text("Purge cancelled.", reply_markup=main_menu_kb().as_markup())
        return
    removed = 0
    for name in os.listdir(DOWNLOAD_DIR):
        p = os.path.join(DOWNLOAD_DIR, name)
        if os.path.isfile(p):
            with suppress(Exception):
                os.remove(p)
                removed += 1
    with suppress(Exception):
        await cq.message.edit_text(f"🧹 Deleted {removed} files from <code>{esc(DOWNLOAD_DIR)}</code>.", parse_mode="HTML", reply_markup=main_menu_kb().as_markup())

# ===================== Settings: mode & max =====================
@router.callback_query(F.data == "settings:mode")
async def settings_mode(cq: CallbackQuery):
    global DEFAULT_MODE
    DEFAULT_MODE = "document" if DEFAULT_MODE == "video" else "video"
    await cq.answer(f"Upload mode: {DEFAULT_MODE}", show_alert=False)
    with suppress(Exception):
        await cq.message.edit_text("Settings:", reply_markup=settings_menu_kb(DEFAULT_MODE, MAX_FILE_MB).as_markup())

@router.callback_query(F.data == "settings:max")
async def settings_max(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    await cq.answer()
    with suppress(Exception):
        await cq.message.edit_text("Select max upload size:", reply_markup=setmax_choices_kb().as_markup())

@router.callback_query(F.data.startswith("settings:max:set:"))
async def settings_max_set(cq: CallbackQuery):
    global MAX_FILE_MB
    try:
        MAX_FILE_MB = int(cq.data.rsplit(":", 1)[1])
    except Exception:
        await cq.answer("Invalid value.", show_alert=True)
        return
    await cq.answer(f"Max set to {MAX_FILE_MB} MB")
    with suppress(Exception):
        await cq.message.edit_text("Settings:", reply_markup=settings_menu_kb(DEFAULT_MODE, MAX_FILE_MB).as_markup())

@router.callback_query(F.data == "settings:max:custom")
async def settings_max_custom(cq: CallbackQuery, state: FSMContext):
    await state.set_state(SetMaxState.waiting_custom)
    await cq.answer()
    with suppress(Exception):
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="✖️ Cancel", callback_data="state:cancel"))
        await cq.message.edit_text("Send a number in MB (e.g., <code>1900</code>):", parse_mode="HTML", reply_markup=kb.as_markup())

@router.message(SetMaxState.waiting_custom)
async def settings_max_custom_value(m: Message, state: FSMContext):
    global MAX_FILE_MB
    val = (m.text or "").strip()
    if not val.isdigit():
        return await m.reply("Enter a positive integer (MB). Or tap Cancel.", reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text="Cancel", callback_data="state:cancel")).as_markup())
    MAX_FILE_MB = max(1, int(val))
    await state.clear()
    await m.reply(f"✅ Max upload size set to <b>{MAX_FILE_MB} MB</b>.", parse_mode="HTML", reply_markup=main_menu_kb().as_markup())

@router.callback_query(F.data == "state:cancel")
async def state_cancel(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    await cq.answer("Cancelled")
    with suppress(Exception):
        await cq.message.edit_text("Cancelled.", reply_markup=main_menu_kb().as_markup())

# ===================== Cookies Inline Flow =====================
@router.callback_query(F.data == "cookies:list")
async def cookies_list(cq: CallbackQuery):
    await cq.answer()
    items = list_cookie_domains()
    if not items:
        text = "No cookies saved.\nUse ➕ Add to upload Netscape cookies (.txt) or paste cookie text / Cookie: header."
    else:
        lines = [f"• <code>{esc(d)}</code>" for d in items]
        text = "<b>Saved cookie domains</b>:\n" + "\n".join(lines)
    with suppress(Exception):
        await cq.message.edit_text(text, parse_mode="HTML", reply_markup=cookies_menu_kb().as_markup())

@router.callback_query(F.data == "cookies:add")
async def cookies_add(cq: CallbackQuery, state: FSMContext):
    await state.set_state(CookieAddStates.waiting_domain)
    await cq.answer()
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="✖️ Cancel", callback_data="state:cancel"))
    with suppress(Exception):
        await cq.message.edit_text("Send the <b>site</b> (domain or URL) for cookies:", parse_mode="HTML", reply_markup=kb.as_markup())

@router.message(CookieAddStates.waiting_domain)
async def add_wait_domain(m: Message, state: FSMContext):
    dom = clean_domain(m.text or "")
    if not dom:
        return await m.reply("Invalid site. Send a domain (e.g. <code>example.com</code>) or a full URL.", parse_mode="HTML",
                             reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text="Cancel", callback_data="state:cancel")).as_markup())
    await state.update_data(domain=dom)
    await state.set_state(CookieAddStates.waiting_body)
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="✖️ Cancel", callback_data="state:cancel"))
    await m.reply(
        f"Upload a <b>.txt</b> (Netscape) or paste cookie text / <code>Cookie:</code> header for <code>{esc(dom)}</code>.",
        parse_mode="HTML",
        reply_markup=kb.as_markup()
    )

@router.message(CookieAddStates.waiting_body, F.document)
async def add_cookie_file(m: Message, state: FSMContext):
    data = await state.get_data()
    dom = data.get("domain")
    if not dom:
        await state.clear()
        return await m.reply("Session lost. Start again via Cookies ➕ Add.", reply_markup=main_menu_kb().as_markup())
    path = cookie_path_for_domain(dom)
    try:
        await g_bot.download(m.document, destination=path)
        await state.clear()
        return await m.reply(f"✅ Saved cookies to <code>{esc(path)}</code>", parse_mode="HTML", reply_markup=cookies_menu_kb().as_markup())
    except Exception as e:
        await state.clear()
        return await m.reply(f"❌ Failed to save cookies: <code>{esc(str(e))}</code>", parse_mode="HTML", reply_markup=cookies_menu_kb().as_markup())

@router.message(CookieAddStates.waiting_body)
async def add_cookie_text(m: Message, state: FSMContext):
    data = await state.get_data()
    dom = data.get("domain")
    if not dom:
        await state.clear()
        return await m.reply("Session lost. Start again via Cookies ➕ Add.", reply_markup=main_menu_kb().as_markup())
    path = cookie_path_for_domain(dom)
    try:
        txt = (m.text or "").strip()
        if not txt:
            return await m.reply("Empty text. Upload a .txt or paste cookie lines.",
                                 reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text="Cancel", callback_data="state:cancel")).as_markup())
        ns = cookie_text_to_netscape(txt, dom)
        content = ns if ns is not None else txt
        with open(path, "w", encoding="utf-8") as f:
            f.write(content if content.endswith("\n") else content + "\n")
        await state.clear()
        return await m.reply(f"✅ Saved cookies to <code>{esc(path)}</code>", parse_mode="HTML", reply_markup=cookies_menu_kb().as_markup())
    except Exception as e:
        await state.clear()
        return await m.reply(f"❌ Failed to save cookies: <code>{esc(str(e))}</code>", parse_mode="HTML", reply_markup=cookies_menu_kb().as_markup())

@router.callback_query(F.data == "cookies:del")
async def cookies_del(cq: CallbackQuery):
    await cq.answer()
    items = list_cookie_domains()
    if not items:
        with suppress(Exception):
            await cq.message.edit_text("No cookies to delete.", reply_markup=cookies_menu_kb().as_markup())
        return
    kb = InlineKeyboardBuilder()
    for d in items[:90]:
        kb.add(InlineKeyboardButton(text=d, callback_data=f"cookies:del:{d}"))
    kb.adjust(2)
    kb.row(InlineKeyboardButton(text="⬅️ Back", callback_data="menu:cookies"))
    with suppress(Exception):
        await cq.message.edit_text("Select domain to delete:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("cookies:del:"))
async def cookies_del_domain(cq: CallbackQuery):
    dom = cq.data.split(":", 2)[2]
    path = cookie_path_for_domain(dom)
    if not os.path.isfile(path):
        await cq.answer("Not found.")
        return
    with suppress(Exception):
        os.remove(path)
    await cq.answer(f"Deleted {dom}")
    with suppress(Exception):
        await cq.message.edit_text(f"🗑️ Deleted cookies for <code>{esc(dom)}</code>.", parse_mode="HTML", reply_markup=cookies_menu_kb().as_markup())

@router.callback_query(F.data == "cookies:clear")
async def cookies_clear(cq: CallbackQuery):
    await cq.answer()
    with suppress(Exception):
        await cq.message.edit_text("Delete ALL cookie files?", reply_markup=confirm_kb("cookies:clearall").as_markup())

@router.callback_query(F.data.in_({"cookies:clearall:confirm", "cookies:clearall:cancel"}))
async def cookies_clear_confirm(cq: CallbackQuery):
    await cq.answer()
    if cq.data.endswith(":cancel"):
        with suppress(Exception):
            await cq.message.edit_text("Cancelled.", reply_markup=cookies_menu_kb().as_markup())
        return
    deleted = 0
    for name in os.listdir(COOKIES_DIR):
        if name.endswith(".txt"):
            with suppress(Exception):
                os.remove(os.path.join(COOKIES_DIR, name))
                deleted += 1
    with suppress(Exception):
        await cq.message.edit_text(f"✅ Deleted {deleted} cookie file(s).", reply_markup=cookies_menu_kb().as_markup())

# ===================== yt-dlp helpers =====================
def token_to_format(token: str) -> str:
    if token == "best":
        return "bv*+ba/b"
    if token.startswith("h") and token[1:].isdigit():
        h = int(token[1:])
        return f"bv*[height<={h}]+ba/b[height<={h}]"
    return "bv*+ba/b"

def build_ytdlp_opts(url: str, fmt: str, outtmpl: str, hook):
    opts = {
        "format": fmt,
        "outtmpl": outtmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "concurrent_fragment_downloads": 12,  # faster HLS
        "progress_hooks": [hook],
        "merge_output_format": "mp4",
        "retries": 7,
        "fragment_retries": 20,
        "socket_timeout": 25,
        "http_headers": common_headers(url),
        "nocheckcertificate": True,
        "geo_bypass": True,
        "extractor_retries": 4,
        "http_chunk_size": 10 * 1024 * 1024,  # 10MB chunks often speed up
        "throttled_rate": None,
    }
    cookiefile = find_cookie_for_url(url)
    if cookiefile:
        opts["cookiefile"] = cookiefile
    return opts

async def probe_info(url: str) -> Tuple[Optional[dict], bool, Optional[str]]:
    base = {
        "skip_download": True, "quiet": True, "no_warnings": True, "noplaylist": True,
        "http_headers": common_headers(url), "nocheckcertificate": True, "geo_bypass": True,
        "extractor_retries": 2,
    }
    cookiefile = find_cookie_for_url(url)
    if cookiefile:
        base["cookiefile"] = cookiefile
    try:
        return await asyncio.to_thread(lambda: yt_dlp.YoutubeDL(base).extract_info(url, download=False)), False, None
    except Exception:
        try:
            base2 = dict(base); base2["force_generic_extractor"] = True
            return await asyncio.to_thread(lambda: yt_dlp.YoutubeDL(base2).extract_info(url, download=False)), True, None
        except Exception as e2:
            return None, False, f"{type(e2).__name__}: {e2}"

# ===================== URL handler =====================
@router.message(F.text.regexp(URL_RE))
async def on_url(m: Message):
    url = URL_RE.search(m.text).group(1)
    msg = await m.reply("🔎 Checking…", reply_markup=main_menu_kb().as_markup())

    job_id = uuid.uuid4().hex[:8]
    job = {"url": url, "title": host_title(url), "msg": msg, "cancelled": False,
           "dl_url": None, "dl_is_direct": False, "direct_mp4": None}
    JOBS[job_id] = job

    info, used_generic, err = await probe_info(url)
    if info:
        heights: List[int] = []
        for f in (info.get("formats") or []):
            if f.get("vcodec") in (None, "none"):
                continue
            h = f.get("height")
            if isinstance(h, int) and h > 0:
                heights.append(h)
            else:
                res = f.get("resolution") or f.get("format_note") or ""
                mh = re.search(r"(\d+)\s*p", res or "")
                if mh:
                    heights.append(int(mh.group(1)))
        job["title"] = info.get("title") or job["title"]
        suffix = " (generic)" if used_generic else ""
        await msg.edit_text(
            f"🎬 <b>{esc(job['title'])}</b>{esc(suffix)}\nChoose a quality:",
            reply_markup=kb_format_choices(job_id, heights or []),
            parse_mode="HTML"
        )
        return

    # Fallback probe to sniff direct media (will also be used as Strategy 3/4 later)
    note = ""
    if err and is_cf_block(err):
        note = "\n<i>Site uses anti-bot protection. If you have access, add cookies, or paste a direct .mp4/.m3u8.</i>"
    html = None
    with suppress(Exception):
        html = await http_get_text_async(url)
    if html:
        m3u8s, mp4s = find_direct_media(html, url)
        if m3u8s:
            mtxt = ""
            with suppress(Exception):
                mtxt = await http_get_text_async(m3u8s[0])
            heights = m3u8_heights(mtxt or "") or [1080, 720, 480, 360]
            job["dl_url"] = m3u8s[0]
            job["dl_is_direct"] = True
            await msg.edit_text(
                f"🎬 <b>{esc(job['title'])}</b>\n(Direct HLS found){note}\nChoose a quality:",
                reply_markup=kb_format_choices(job_id, heights),
                parse_mode="HTML"
            )
            return
        if mp4s:
            job["dl_url"] = mp4s[0]
            job["direct_mp4"] = mp4s[0]
            job["dl_is_direct"] = True
            await msg.edit_text(
                f"🎬 <b>{esc(job['title'])}</b>\n(Direct MP4 found){note}\nChoose:",
                reply_markup=kb_format_choices(job_id, [], include_best=True),
                parse_mode="HTML"
            )
            return

    await msg.edit_text(
        f"🎬 <b>{esc(job['title'])}</b>\nCouldn’t list qualities.{note}\nPick one to try:",
        reply_markup=kb_format_choices(job_id, [1080, 720, 480, 360]),
        parse_mode="HTML"
    )

# ===================== 4-Strategy Download =====================
async def sniff_direct_once(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (m3u8_url, mp4_url) or (None,None)."""
    with suppress(Exception):
        html = await http_get_text_async(url)
        if html:
            m3u8s, mp4s = find_direct_media(html, url)
            return (m3u8s[0] if m3u8s else None, mp4s[0] if mp4s else None)
    return (None, None)

async def direct_http_download(mp4_url: str, outtmpl: str, title: str, msg: Message, job_id: str) -> Optional[str]:
    """Strategy 4: stream direct MP4 via requests with cookiejar + progress."""
    # derive filename
    parsed = urlparse(mp4_url)
    name = os.path.basename(parsed.path) or "video.mp4"
    if not os.path.splitext(name)[1]:
        name += ".mp4"
    safe_title = re.sub(r"[\\/:*?\"<>|]+", "_", title)[:150]
    filename = f"{safe_title} [direct].{os.path.splitext(name)[1].lstrip('.')}"
    path = os.path.join(DOWNLOAD_DIR, filename)

    # session with cookies
    jar = cookiejar_for_url(mp4_url)
    sess = requests.Session()
    if jar: sess.cookies = jar
    sess.headers.update(common_headers(mp4_url))
    # try to probe size
    total = None
    try:
        h = sess.head(mp4_url, timeout=25, allow_redirects=True)
        if 'content-length' in h.headers:
            total = int(h.headers['content-length'])
    except Exception:
        pass

    # stream
    r = sess.get(mp4_url, stream=True, timeout=25, allow_redirects=True)
    r.raise_for_status()

    loop = asyncio.get_running_loop()
    downloaded = 0
    chunk = 2 * 1024 * 1024  # 2MB
    last = 0.0
    with open(path, "wb") as f:
        for data in r.iter_content(chunk_size=chunk):
            if JOBS.get(job_id, {}).get("cancelled"):
                r.close()
                with suppress(Exception): os.remove(path)
                raise yt_dlp.utils.DownloadError("Cancelled by user")
            if not data:
                continue
            f.write(data)
            downloaded += len(data)
            now = time.time()
            if now - last > 1.0:
                last = now
                pct = (downloaded / total * 100.0) if total else 0.0
                spd = None  # not trivial without timestamps per-chunk; keep UI consistent
                text = (
                    f"⏬ <b>{esc(title)}</b>\n"
                    f"{bar(pct if total else 0.0)}  {(pct if total else 0.0):.1f}%\n"
                    f"{fmt_bytes(downloaded)} / {fmt_bytes(total)}"
                )
                schedule_edit(loop, msg, text, reply_markup=None)
    return path

async def send_with_retry(chat_id: int, path: str, caption: str, as_video: bool) -> None:
    """Retry upload once if transport hiccups."""
    for attempt in range(2):
        try:
            if as_video and looks_video(path):
                try:
                    await g_bot.send_video(chat_id, FSInputFile(path), caption=caption)
                except Exception:
                    await g_bot.send_document(chat_id, FSInputFile(path), caption=caption)
            else:
                await g_bot.send_document(chat_id, FSInputFile(path), caption=caption)
            return
        except Exception as e:
            # classic aiohttp noise when connection closes mid-upload
            if "closing transport" in str(e).lower() or "Timeout" in str(e):
                if attempt == 0:
                    await asyncio.sleep(2.0)
                    continue
            raise

@router.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    if job_id in JOBS:
        JOBS[job_id]["cancelled"] = True
    with suppress(Exception):
        await cq.message.edit_text("🛑 Cancelled.", reply_markup=main_menu_kb().as_markup())
    JOBS.pop(job_id, None)
    await cq.answer("Cancelled")

@router.callback_query(F.data.startswith("get:"))
async def cb_get(cq: CallbackQuery):
    _, job_id, token = cq.data.split(":")
    job = JOBS.get(job_id)
    if not job:
        return await cq.answer("Job missing.", show_alert=True)

    src_url = job.get("url")
    title = job["title"]
    msg = job["msg"]
    await cq.answer("Downloading…")
    with suppress(Exception):
        await msg.edit_text(
            f"⏬ <b>{esc(title)}</b>\nPreparing…",
            reply_markup=kb_format_choices(job_id, [], include_best=False),
            parse_mode="HTML"
        )

    loop = asyncio.get_running_loop()
    started = {"flag": False, "ts": 0.0, "file": None}

    # common progress hook for yt-dlp strategies
    def hook(d):
        if JOBS.get(job_id, {}).get("cancelled"):
            raise yt_dlp.utils.DownloadError("Cancelled by user")
        st = d.get("status")
        if st == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            done = d.get("downloaded_bytes") or 0
            pct = (done / total * 100) if total else 0.0
            spd = d.get("speed")
            eta = d.get("eta")
            now = time.time()
            if done and not started["flag"]:
                started["flag"] = True
            if now - started["ts"] > 1.0:
                started["ts"] = now
                text = (
                    f"⏬ <b>{esc(title)}</b>\n"
                    f"{bar(pct)}  {pct:.1f}%\n"
                    f"{fmt_bytes(done)} / {fmt_bytes(total)} • {fmt_bytes(spd)}/s • ETA {fmt_eta(eta)}"
                )
                schedule_edit(loop, msg, text, reply_markup=kb_format_choices(job_id, [], include_best=False))
        elif st == "finished":
            started["file"] = d.get("filename")
            schedule_edit(loop, msg, "✅ Download complete. Finalizing…", reply_markup=kb_format_choices(job_id, [], include_best=False))

    fmt_sel = token_to_format(token)
    outtmpl = os.path.join(DOWNLOAD_DIR, "%(title).200B [%(id)s].%(ext)s")

    def ytdlp_run(url: str, force_generic: bool = False):
        opts = build_ytdlp_opts(url, fmt_sel, outtmpl, hook)
        if force_generic:
            opts["force_generic_extractor"] = True
        with yt_dlp.YoutubeDL(opts) as y:
            info = y.extract_info(url, download=True)
            return started["file"] or y.prepare_filename(info)

    # --------- 4 STRATEGIES (auto) ----------
    final_path = None
    try:
        # Strategy 1: yt-dlp native extractor
        try:
            final_path = await asyncio.to_thread(ytdlp_run, src_url, False)
        except Exception:
            # Strategy 2: yt-dlp generic extractor
            try:
                final_path = await asyncio.to_thread(ytdlp_run, src_url, True)
            except Exception:
                # Strategy 3: sniff direct m3u8/mp4, prefer m3u8 via yt-dlp
                m3u8_url, mp4_url = None, None
                if job.get("dl_url"):
                    # we already sniffed earlier
                    if job.get("dl_url", "").endswith(".m3u8"):
                        m3u8_url = job["dl_url"]
                    elif job.get("direct_mp4"):
                        mp4_url = job["direct_mp4"]
                    else:
                        # ensure both checked
                        m3u8_url, mp4_url = await sniff_direct_once(src_url)
                else:
                    m3u8_url, mp4_url = await sniff_direct_once(src_url)

                if m3u8_url:
                    try:
                        final_path = await asyncio.to_thread(ytdlp_run, m3u8_url, False)
                    except Exception:
                        pass

                # Strategy 4: direct MP4 stream via requests (only if still no file and mp4 exists)
                if not final_path and (mp4_url or job.get("direct_mp4")):
                    mp4 = mp4_url or job.get("direct_mp4")
                    final_path = await direct_http_download(mp4, outtmpl, title, msg, job_id)

    except yt_dlp.utils.DownloadError as e:
        s = str(e)
        tip = ""
        if is_cf_block(s):
            tip = ("\n<i>Site is using anti-bot protection. Add cookies or provide a direct .mp4/.m3u8.</i>")
        elif any(w in s.lower() for w in ("sign in", "login", "account", "age")):
            tip = "\n<i>Login/consent required. Add cookies via Cookies ➕ Add.</i>"
        with suppress(Exception):
            await msg.edit_text(f"❌ Download error:\n<code>{esc(s)}</code>{tip}", parse_mode="HTML", reply_markup=main_menu_kb().as_markup())
        JOBS.pop(job_id, None)
        return
    except Exception as e:
        with suppress(Exception):
            await msg.edit_text(f"❌ Error:\n<code>{esc(type(e).__name__ + ': ' + str(e))}</code>", parse_mode="HTML", reply_markup=main_menu_kb().as_markup())
        JOBS.pop(job_id, None)
        return

    if not final_path or not os.path.exists(final_path):
        with suppress(Exception):
            await msg.edit_text("❌ File not found after download.", reply_markup=main_menu_kb().as_markup())
        JOBS.pop(job_id, None)
        return

    # size gate
    if file_too_large(final_path):
        with suppress(Exception):
            await msg.edit_text(
                f"✅ Downloaded <code>{esc(os.path.basename(final_path))}</code>\n"
                f"Size: {fmt_bytes(os.path.getsize(final_path))}\n"
                f"⚠️ Too large for Telegram (&gt;{MAX_FILE_MB} MB). Pick a lower quality.",
                parse_mode="HTML",
                reply_markup=kb_format_choices(job_id, [1080, 720, 480, 360])
            )
        with suppress(Exception):
            os.remove(final_path)
        return  # keep job so user can choose new quality

    # Upload (robust)
    with suppress(Exception):
        await msg.edit_text("⬆️ Uploading…", reply_markup=None)

    try:
        await send_with_retry(msg.chat.id, final_path, "✅ Done.", (DEFAULT_MODE == "video"))
    except Exception as e:
        with suppress(Exception):
            await msg.edit_text(f"❌ Upload failed: <code>{esc(str(e))}</code>", parse_mode="HTML", reply_markup=main_menu_kb().as_markup())
        with suppress(Exception):
            os.remove(final_path)
        JOBS.pop(job_id, None)
        return

    with suppress(Exception):
        await msg.delete()
    with suppress(Exception):
        os.remove(final_path)
    JOBS.pop(job_id, None)

# ===================== Runner =====================
async def check_ffmpeg():
    from shutil import which
    if which("ffmpeg") is None:
        print("WARNING: ffmpeg not found in PATH. Install ffmpeg for HLS/merge.")

async def main():
    global g_bot
    await check_ffmpeg()

    # robust session to prevent "Cannot write to closing transport"
    timeout = ClientTimeout(total=None, connect=60, sock_connect=60, sock_read=None)
    connector = TCPConnector(limit=64, force_close=False, enable_cleanup_closed=True)
    session = AiohttpSession(timeout=timeout, connector=connector)

    g_bot = Bot(
        BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    await g_bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(g_bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
