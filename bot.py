import asyncio
import os
import re
import time
import uuid
from contextlib import suppress
from typing import Optional, Dict, List, Tuple

from dotenv import load_dotenv
load_dotenv()

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

import yt_dlp

# ===================== Config (minimal) =====================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("Set BOT_TOKEN in .env")

DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "downloads")
MAX_FILE_MB = int(os.environ.get("MAX_FILE_MB", "1900"))
DEFAULT_MODE = os.environ.get("DEFAULT_UPLOAD_MODE", "video").strip().lower()  # video|document
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

URL_RE = re.compile(r"(https?://[^\s]+)", re.IGNORECASE)

# ===================== Aiogram wiring =====================
dp = Dispatcher()
router = Router()
dp.include_router(router)
g_bot: Optional[Bot] = None

# job_id -> info
JOBS: Dict[str, dict] = {}

# ===================== Helpers =====================
def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def fmt_bytes(n: Optional[float]) -> str:
    if n is None:
        return "?"
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"

def fmt_eta(sec: Optional[float]) -> str:
    if sec is None:
        return "?"
    sec = max(0, int(sec))
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")

def looks_video(path: str) -> bool:
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    return ext in {"mp4", "mkv", "webm", "mov", "m4v"}

def file_too_large(path: str) -> bool:
    try:
        return os.path.getsize(path) > MAX_FILE_MB * 1024 * 1024
    except FileNotFoundError:
        return False

def detect_cookiefile() -> Optional[str]:
    for c in ("cookies.txt", "cookies/cookies.txt", "youtube-cookies.txt"):
        if os.path.isfile(c):
            return os.path.abspath(c)
    return None

def bar(pct: float, width: int = 18) -> str:
    pct = max(0.0, min(100.0, pct))
    fill = int(width * pct / 100.0)
    return "[" + "#" * fill + "–" * (width - fill) + "]"

# Safely edit a message from yt-dlp hook threads
def schedule_edit(loop: asyncio.AbstractEventLoop, message: Message, text: str, reply_markup=None):
    async def _edit():
        with suppress(Exception):
            await message.edit_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    asyncio.run_coroutine_threadsafe(_edit(), loop)

def parse_height(fmt: dict) -> Optional[int]:
    """Get a numeric height from a yt-dlp format dict."""
    h = fmt.get("height")
    if isinstance(h, int) and h > 0:
        return h
    # Try "resolution" like "1280x720"
    res = fmt.get("resolution") or ""
    m = re.search(r"x(\d+)", res)
    if m:
        return int(m.group(1))
    # Try format_note like "720p"
    note = fmt.get("format_note") or ""
    m = re.search(r"(\d+)\s*p", note)
    if m:
        return int(m.group(1))
    return None

def extract_info_with_fallback(url: str) -> Tuple[dict, bool]:
    """
    Try normal extractor first. On extractor errors, retry with force_generic_extractor.
    Returns (info, used_generic).
    """
    base = {"skip_download": True, "quiet": True, "no_warnings": True, "noplaylist": True}
    try:
        with yt_dlp.YoutubeDL(base) as y:
            info = y.extract_info(url, download=False)
            return info, False
    except Exception:
        # Retry using generic extractor
        base["force_generic_extractor"] = True
        with yt_dlp.YoutubeDL(base) as y:
            info = y.extract_info(url, download=False)
            return info, True

# ===================== Keyboards =====================
def kb_format_choices(job_id: str, heights: List[int]):
    kb = InlineKeyboardBuilder()
    kb.add(InlineKeyboardButton(text="Best", callback_data=f"get:{job_id}:best"))
    for h in sorted(set(heights), reverse=True):
        if h:
            kb.add(InlineKeyboardButton(text=f"{h}p", callback_data=f"get:{job_id}:h{h}"))
    kb.adjust(3)
    return kb.as_markup()

def kb_cancel(job_id: str):
    kb = InlineKeyboardBuilder()
    kb.add(InlineKeyboardButton(text="❌ Cancel", callback_data=f"cancel:{job_id}"))
    return kb.as_markup()

# ===================== Commands =====================
@router.message(Command("start"))
async def start_cmd(m: Message):
    await m.reply(
        "Send a video/page URL.\n"
        "I’ll probe formats → show actual qualities (Best, 1080p, 720p, …) → download with live <b>% / speed / ETA</b> and send it.\n"
        "<i>Tip: For age/consent-gated or anti-bot sites, put a Netscape <code>cookies.txt</code> next to the bot.</i>",
        parse_mode="HTML"
    )

@router.message(Command("help"))
async def help_cmd(m: Message):
    await start_cmd(m)

# ===================== URL handler =====================
@router.message(F.text.regexp(URL_RE))
async def on_url(m: Message):
    url = URL_RE.search(m.text).group(1)
    msg = await m.reply("🔎 Checking…")

    try:
        info, used_generic = extract_info_with_fallback(url)
    except yt_dlp.utils.DownloadError as e:
        tip = ""
        s = str(e)
        if "Sign in to confirm you're not a bot" in s or "account" in s.lower():
            tip = "\nTip: add <code>cookies.txt</code>."
        return await msg.edit_text(f"❌ Not downloadable / needs login.\n<code>{esc(s)}</code>{tip}", parse_mode="HTML")
    except Exception as e:
        # Covers site-specific extractor crashes like KeyError('videoModel')
        return await msg.edit_text(f"❌ Not downloadable.\n<code>{esc(type(e).__name__ + ': ' + str(e))}</code>", parse_mode="HTML")

    title = info.get("title") or "video"
    fmts = info.get("formats") or []
    heights = []
    for f in fmts:
        if f.get("vcodec") in (None, "none"):
            continue
        h = parse_height(f)
        if h:
            heights.append(h)

    job_id = uuid.uuid4().hex[:8]
    JOBS[job_id] = {
        "url": url,
        "title": title,
        "msg": msg,
        "mode": DEFAULT_MODE,
        "cancelled": False
    }

    note = " (generic)" if used_generic else ""
    await msg.edit_text(
        f"🎬 <b>{esc(title)}</b>{esc(note)}\nChoose a quality:",
        reply_markup=kb_format_choices(job_id, heights or []),
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

def token_to_format(token: str) -> dict:
    if token == "best":
        return {"format": "bv*+ba/b"}  # video+audio
    if token.startswith("h") and token[1:].isdigit():
        h = int(token[1:])
        # choose at or below the selected height
        return {"format": f"bv*[height<={h}]+ba/b[height<={h}]"}
    return {"format": "bv*+ba/b"}

@router.callback_query(F.data.startswith("get:"))
async def cb_get(cq: CallbackQuery):
    # data: get:<job_id>:<token>
    _, job_id, token = cq.data.split(":")
    job = JOBS.get(job_id)
    if not job:
        return await cq.answer("Job missing.", show_alert=True)

    url = job["url"]
    msg = job["msg"]
    title = job["title"]
    await cq.answer("Downloading…")
    await msg.edit_text(f"⏬ <b>{esc(title)}</b>\nPreparing…", reply_markup=kb_cancel(job_id), parse_mode="HTML")

    loop = asyncio.get_running_loop()
    started = {"flag": False, "ts": 0, "file": None}

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
                schedule_edit(loop, msg, text, reply_markup=kb_cancel(job_id))
        elif st == "finished":
            started["file"] = d.get("filename")
            schedule_edit(loop, msg, "✅ Download complete. Finalizing…", reply_markup=kb_cancel(job_id))

    choice = token_to_format(token)
    outtmpl = os.path.join(DOWNLOAD_DIR, "%(title).200B [%(id)s].%(ext)s")

    # Build common options
    def build_opts(force_generic: bool = False):
        opts = {
            "format": choice["format"],
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
            "http_headers": {
                "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/124.0 Safari/537.36"),
                "Referer": url
            }
        }
        if force_generic:
            opts["force_generic_extractor"] = True
        cookiefile = detect_cookiefile()
        if cookiefile:
            opts["cookiefile"] = cookiefile
        return opts

    def run_dl_with_fallback():
        # try native extractor
        try:
            with yt_dlp.YoutubeDL(build_opts(False)) as y:
                info = y.extract_info(url, download=True)
                return started["file"] or y.prepare_filename(info)
        except Exception:
            # fallback: generic extractor (fixes KeyError('videoModel')-type issues)
            with yt_dlp.YoutubeDL(build_opts(True)) as y:
                info = y.extract_info(url, download=True)
                return started["file"] or y.prepare_filename(info)

    try:
        final_path = await loop.run_in_executor(None, run_dl_with_fallback)
    except yt_dlp.utils.DownloadError as e:
        tip = ""
        s = str(e)
        if "Sign in to confirm you're not a bot" in s or "account" in s.lower():
            tip = "\nTip: add a valid <code>cookies.txt</code>."
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
        JOBS.pop(job_id, None)
        return

    # Upload
    with suppress(Exception):
        await msg.edit_text("⬆️ Uploading…", reply_markup=None)

    caption = "✅ Done."
    try:
        if DEFAULT_MODE == "video" and looks_video(final_path):
            # Try sending as video; if Telegram rejects codec/container, fall back to document
            try:
                await g_bot.send_video(msg.chat.id, FSInputFile(final_path), caption=caption)
            except Exception:
                await g_bot.send_document(msg.chat.id, FSInputFile(final_path), caption=caption)
        else:
            await g_bot.send_document(msg.chat.id, FSInputFile(final_path), caption=caption)
    except Exception as e:
        with suppress(Exception):
            await msg.edit_text(f"❌ Upload failed: <code>{esc(str(e))}</code>", parse_mode="HTML")
        JOBS.pop(job_id, None)
        return

    with suppress(Exception):
        await msg.delete()
    JOBS.pop(job_id, None)

# ===================== Runner =====================
async def main():
    global g_bot
    g_bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    # Ensure no webhook conflicts with long polling
    await g_bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(g_bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
