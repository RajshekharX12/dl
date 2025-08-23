#!/usr/bin/env python3
import asyncio
import os
import re
import time
import uuid
from contextlib import suppress
from typing import Optional, Dict, List, Tuple
from urllib.parse import urlparse, urljoin

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

from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

import yt_dlp
import requests

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
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

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
        r = requests.get(url, headers=common_headers(url), timeout=20, allow_redirects=True)
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
    t = err_text.lower()
    return ("cloudflare" in t and ("403" in t or "challenge" in t)) or ("403" in t and "forbidden" in t)

# ===================== Cookies manager =====================
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

def list_cookie_domains() -> List[Tuple[str, int]]:
    items = []
    for name in os.listdir(COOKIES_DIR):
        if name.endswith(".txt"):
            path = os.path.join(COOKIES_DIR, name)
            try:
                ts = int(os.path.getmtime(path))
            except Exception:
                ts = 0
            items.append((name[:-4], ts))
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

# FSM states for /add and /del
class CookieAddStates(StatesGroup):
    waiting_domain = State()
    waiting_body = State()

class CookieDelStates(StatesGroup):
    waiting_domain = State()

# ===================== Keyboards =====================
def kb_format_choices(job_id: str, heights: List[int], include_best: bool = True):
    kb = InlineKeyboardBuilder()
    if include_best:
        kb.add(InlineKeyboardButton(text="Best", callback_data=f"get:{job_id}:best"))
    for h in sorted(set(heights), reverse=True):
        if h:
            kb.add(InlineKeyboardButton(text=f"{h}p", callback_data=f"get:{job_id}:h{h}"))
    kb.adjust(3)
    kb.row(InlineKeyboardButton(text="❌ Cancel", callback_data=f"cancel:{job_id}"))
    return kb.as_markup()

# ===================== Commands =====================
@router.message(Command("start"))
async def start_cmd(m: Message):
    await m.reply(
        "Send a video/page URL.\n"
        "I’ll show actual qualities (Best, 1080p, 720p, …), then download with live <b>% / speed / ETA</b> and send it.\n\n"
        "<b>Cookies manager</b>\n"
        "• /cookies — list cookies + help\n"
        "• /add — add/replace cookies for a site (Netscape .txt or paste)\n"
        "• /del — delete cookies for a site\n"
        "• /clearcookies — delete <i>all</i> cookies\n\n"
        "<b>Other</b>\n"
        "• /mode — upload as video or document\n"
        "• /setmax — set max Telegram upload size (MB)\n"
        "• /status — show active jobs\n"
        "• /purge — delete old files in downloads\n"
        "• /about — info & tips",
        parse_mode="HTML"
    )

@router.message(Command("help"))
async def help_cmd(m: Message):
    await start_cmd(m)

@router.message(Command("about"))
async def about_cmd(m: Message):
    await m.reply(
        "This bot uses <code>yt-dlp</code> to fetch formats from many sites (including adult sites supported by yt-dlp).\n"
        "For sites with age gates/login, add cookies via <b>/add</b>.\n"
        "I won’t bypass DRM/paywalls or private Telegram channels.\n"
        "Tip: Send the <i>video page</i> URL (not just a channel listing).\n",
        parse_mode="HTML"
    )

@router.message(Command("mode"))
async def mode_cmd(m: Message):
    global DEFAULT_MODE
    arg = (m.text or "").split(maxsplit=1)
    if len(arg) == 2 and arg[1].strip().lower() in {"video","document"}:
        DEFAULT_MODE = arg[1].strip().lower()
        return await m.reply(f"✅ Upload mode set to <b>{DEFAULT_MODE}</b>.", parse_mode="HTML")
    await m.reply(f"Current upload mode: <b>{DEFAULT_MODE}</b>\nUse <code>/mode video</code> or <code>/mode document</code>.", parse_mode="HTML")

@router.message(Command("setmax"))
async def setmax_cmd(m: Message):
    global MAX_FILE_MB
    parts = (m.text or "").split()
    if len(parts) == 2 and parts[1].isdigit():
        MAX_FILE_MB = max(1, int(parts[1]))
        return await m.reply(f"✅ Max upload size set to <b>{MAX_FILE_MB} MB</b>.", parse_mode="HTML")
    await m.reply(f"Usage: <code>/setmax 1900</code> (current: <b>{MAX_FILE_MB} MB</b>)", parse_mode="HTML")

@router.message(Command("status"))
async def status_cmd(m: Message):
    if not JOBS:
        return await m.reply("No active jobs.")
    lines = []
    for jid, j in JOBS.items():
        lines.append(f"• <code>{jid}</code> — {esc(j.get('title','?'))}")
    await m.reply("<b>Active jobs</b>:\n" + "\n".join(lines), parse_mode="HTML")

@router.message(Command("purge"))
async def purge_cmd(m: Message):
    removed = 0
    for name in os.listdir(DOWNLOAD_DIR):
        p = os.path.join(DOWNLOAD_DIR, name)
        if os.path.isfile(p):
            with suppress(Exception):
                os.remove(p)
                removed += 1
    await m.reply(f"🧹 Deleted {removed} files from <code>{esc(DOWNLOAD_DIR)}</code>.", parse_mode="HTML")

@router.message(Command("cookies"))
async def cookies_cmd(m: Message):
    items = list_cookie_domains()
    if not items:
        text = (
            "<b>Cookies</b>\n"
            "No site cookies saved.\n\n"
            "Use /add to store cookies for a domain (send a .txt file or paste Netscape cookies).\n"
            "Bot auto-uses matching cookies per URL."
        )
    else:
        lines = [f"• <code>{esc(dom)}</code>" for dom, _ in items]
        text = (
            "<b>Cookies</b>\nSaved domains:\n" + "\n".join(lines) +
            "\n\nUse /add to add/replace, /del to remove one, /clearcookies to wipe all."
        )
    await m.reply(text, parse_mode="HTML")

@router.message(Command("add"))
async def add_cmd(m: Message, state: FSMContext):
    await state.set_state(CookieAddStates.waiting_domain)
    await m.reply("Send the <b>site</b> (domain or URL) you want to save cookies for.", parse_mode="HTML")

@router.message(CookieAddStates.waiting_domain)
async def add_wait_domain(m: Message, state: FSMContext):
    dom = clean_domain(m.text or "")
    if not dom:
        return await m.reply("Invalid site. Send a domain (e.g. <code>example.com</code>) or a full URL.", parse_mode="HTML")
    await state.update_data(domain=dom)
    await state.set_state(CookieAddStates.waiting_body)
    await m.reply(
        f"Okay. Send cookies for <code>{esc(dom)}</code> now:\n"
        "• Either upload a <b>.txt</b> file (Netscape format),\n"
        "• Or paste the cookie text here.",
        parse_mode="HTML"
    )

@router.message(CookieAddStates.waiting_body, F.document)
async def add_cookie_file(m: Message, state: FSMContext):
    data = await state.get_data()
    dom = data.get("domain")
    if not dom:
        await state.clear()
        return await m.reply("Session lost. Run /add again.")
    path = cookie_path_for_domain(dom)
    try:
        await g_bot.download(m.document, destination=path)
        await state.clear()
        return await m.reply(f"✅ Saved cookies to <code>{esc(path)}</code>", parse_mode="HTML")
    except Exception as e:
        await state.clear()
        return await m.reply(f"❌ Failed to save cookies: <code>{esc(str(e))}</code>", parse_mode="HTML")

@router.message(CookieAddStates.waiting_body)
async def add_cookie_text(m: Message, state: FSMContext):
    data = await state.get_data()
    dom = data.get("domain")
    if not dom:
        await state.clear()
        return await m.reply("Session lost. Run /add again.")
    path = cookie_path_for_domain(dom)
    try:
        txt = (m.text or "").strip()
        if not txt:
            return await m.reply("Empty text. Send a .txt file or paste cookie lines.")
        with open(path, "w", encoding="utf-8") as f:
            f.write(txt + ("\n" if not txt.endswith("\n") else ""))
        await state.clear()
        return await m.reply(f"✅ Saved cookies to <code>{esc(path)}</code>", parse_mode="HTML")
    except Exception as e:
        await state.clear()
        return await m.reply(f"❌ Failed to save cookies: <code>{esc(str(e))}</code>", parse_mode="HTML")

@router.message(Command("del"))
async def del_cmd(m: Message, state: FSMContext):
    await state.set_state(CookieDelStates.waiting_domain)
    await m.reply("Send the <b>site</b> (domain or URL) whose cookies you want to delete.", parse_mode="HTML")

@router.message(CookieDelStates.waiting_domain)
async def del_wait_domain(m: Message, state: FSMContext):
    dom = clean_domain(m.text or "")
    await state.clear()
    if not dom:
        return await m.reply("Invalid site. Send a domain (e.g. <code>example.com</code>) or a full URL.", parse_mode="HTML")
    path = cookie_path_for_domain(dom)
    if not os.path.isfile(path):
        return await m.reply(f"No cookies for <code>{esc(dom)}</code>.", parse_mode="HTML")
    try:
        os.remove(path)
        return await m.reply(f"🗑️ Deleted cookies for <code>{esc(dom)}</code>.", parse_mode="HTML")
    except Exception as e:
        return await m.reply(f"❌ Failed to delete: <code>{esc(str(e))}</code>", parse_mode="HTML")

@router.message(Command("clearcookies"))
async def clearcookies_cmd(m: Message):
    await m.reply(
        "This will delete <b>ALL</b> saved cookies. Reply with <code>YES</code> to confirm.",
        parse_mode="HTML"
    )

@router.message(F.text == "YES")
async def clearcookies_yes(m: Message):
    deleted = 0
    for name in os.listdir(COOKIES_DIR):
        if name.endswith(".txt"):
            with suppress(Exception):
                os.remove(os.path.join(COOKIES_DIR, name))
                deleted += 1
    await m.reply(f"✅ Deleted {deleted} cookie file(s).")

# ===================== Probe & download =====================
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
        "concurrent_fragment_downloads": 4,
        "progress_hooks": [hook],
        "merge_output_format": "mp4",
        "retries": 5,
        "fragment_retries": 10,
        "socket_timeout": 15,
        "http_headers": common_headers(url),
        "nocheckcertificate": True,
        "geo_bypass": True,
        "extractor_retries": 3,
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
    msg = await m.reply("🔎 Checking…")

    job_id = uuid.uuid4().hex[:8]
    job = {"url": url, "title": host_title(url), "msg": msg, "cancelled": False,
           "dl_url": None, "dl_is_direct": False}
    JOBS[job_id] = job

    # yt-dlp probe
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

    # direct-media fallback
    note = ""
    if err and is_cf_block(err):
        note = "\n<i>Site uses anti-bot protection. If you have access, export session cookies to <code>cookies.txt</code> via /add, or paste a direct .mp4/.m3u8.</i>"
    elif err:
        note = f"\n<i>Extractor failed ({esc(err)}). We can try a generic attempt.</i>"

    html = None
    with suppress(Exception):
        html = await http_get_text_async(url)
    if html:
        m3u8s, mp4s = find_direct_media(html, url)
        if m3u8s:
            m3u8_url = m3u8s[0]
            mtxt = ""
            with suppress(Exception):
                mtxt = await http_get_text_async(m3u8_url)
            heights = m3u8_heights(mtxt or "") or [1080, 720, 480, 360]
            job["dl_url"] = m3u8_url
            job["dl_is_direct"] = True
            await msg.edit_text(
                f"🎬 <b>{esc(job['title'])}</b>\n(Direct HLS found){note}\nChoose a quality:",
                reply_markup=kb_format_choices(job_id, heights),
                parse_mode="HTML"
            )
            return
        if mp4s:
            job["dl_url"] = mp4s[0]
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

# ===================== Callbacks =====================
@router.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    if job_id in JOBS:
        JOBS[job_id]["cancelled"] = True
    with suppress(Exception):
        await cq.message.edit_text("🛑 Cancelled.")
    JOBS.pop(job_id, None)
    await cq.answer("Cancelled")

@router.callback_query(F.data.startswith("get:"))
async def cb_get(cq: CallbackQuery):
    _, job_id, token = cq.data.split(":")
    job = JOBS.get(job_id)
    if not job:
        return await cq.answer("Job missing.", show_alert=True)

    src_url = job.get("dl_url") or job["url"]
    title = job["title"]
    msg = job["msg"]

    await cq.answer("Downloading…")
    with suppress(Exception):
        await msg.edit_text(f"⏬ <b>{esc(title)}</b>\nPreparing…",
                            reply_markup=kb_format_choices(job_id, [], include_best=False),
                            parse_mode="HTML")

    loop = asyncio.get_running_loop()
    started = {"flag": False, "ts": 0.0, "file": None}

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

    def run_dl(force_generic: bool = False):
        opts = build_ytdlp_opts(src_url, fmt_sel, outtmpl, hook)
        if force_generic:
            opts["force_generic_extractor"] = True
        with yt_dlp.YoutubeDL(opts) as y:
            info = y.extract_info(src_url, download=True)
            return started["file"] or y.prepare_filename(info)

    try:
        # normal → generic fallback
        try:
            final_path = await asyncio.to_thread(run_dl, False)
        except Exception:
            final_path = await asyncio.to_thread(run_dl, True)
    except yt_dlp.utils.DownloadError as e:
        s = str(e)
        tip = ""
        if is_cf_block(s):
            tip = ("\n<i>Site is using anti-bot protection. I can’t bypass that. "
                   "If you have access, add cookies via /add or paste a direct .mp4/.m3u8.</i>")
        elif any(w in s.lower() for w in ("sign in", "login", "account", "age")):
            tip = "\n<i>Login/consent required. Add cookies via /add.</i>"
        with suppress(Exception):
            await msg.edit_text(f"❌ Download error:\n<code>{esc(s)}</code>{tip}", parse_mode="HTML", reply_markup=None)
        JOBS.pop(job_id, None)
        return
    except Exception as e:
        with suppress(Exception):
            await msg.edit_text(f"❌ Error:\n<code>{esc(type(e).__name__ + ': ' + str(e))}</code>", parse_mode="HTML", reply_markup=None)
        JOBS.pop(job_id, None)
        return

    if not final_path or not os.path.exists(final_path):
        with suppress(Exception):
            await msg.edit_text("❌ File not found after download.", reply_markup=None)
        JOBS.pop(job_id, None)
        return

    if file_too_large(final_path):
        with suppress(Exception):
            await msg.edit_text(
                f"✅ Downloaded <code>{esc(os.path.basename(final_path))}</code>\n"
                f"Size: {fmt_bytes(os.path.getsize(final_path))}\n"
                f"⚠️ Too large for Telegram (&gt;{MAX_FILE_MB} MB). Choose a lower quality.",
                parse_mode="HTML",
                reply_markup=None
            )
        with suppress(Exception):
            os.remove(final_path)
        JOBS.pop(job_id, None)
        return

    with suppress(Exception):
        await msg.edit_text("⬆️ Uploading…", reply_markup=None)

    caption = "✅ Done."
    try:
        if DEFAULT_MODE == "video" and looks_video(final_path):
            try:
                await g_bot.send_video(msg.chat.id, FSInputFile(final_path), caption=caption)
            except Exception:
                await g_bot.send_document(msg.chat.id, FSInputFile(final_path), caption=caption)
        else:
            await g_bot.send_document(msg.chat.id, FSInputFile(final_path), caption=caption)
    except Exception as e:
        with suppress(Exception):
            await msg.edit_text(f"❌ Upload failed: <code>{esc(str(e))}</code>", parse_mode=ParseMode.HTML)
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
    # yt-dlp can download without ffmpeg, but merging/HLS often needs it
    from shutil import which
    if which("ffmpeg") is None:
        print("WARNING: ffmpeg not found in PATH. Install ffmpeg for best results (HLS/merge).")

async def main():
    global g_bot
    await check_ffmpeg()
    g_bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await g_bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(g_bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
