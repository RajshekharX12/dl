import asyncio
import logging
import os
import re
import shlex
import shutil
import subprocess
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Dict, Optional, List

from dotenv import load_dotenv
load_dotenv()

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

import yt_dlp

# ===================== Config & helpers =====================

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN missing in .env")

OWNER_ID = int(os.environ.get("OWNER_ID", "0") or 0)
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "downloads")
MAX_FILE_MB = int(os.environ.get("MAX_FILE_MB", "1900"))     # <=1900 MB recommended
DEFAULT_UPLOAD_MODE = os.environ.get("DEFAULT_UPLOAD_MODE", "video").strip().lower()

# Limits
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", "2"))  # parallel downloads
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

URL_RE = re.compile(r"(https?://[^\s]+)", re.IGNORECASE)

def fmt_bytes(n: Optional[float]) -> str:
    if not n and n != 0:
        return "?"
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024.0
    return f"{n:.1f} PB"

def fmt_eta(seconds: Optional[float]) -> str:
    if seconds is None:
        return "?"
    if seconds < 0:
        seconds = 0
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def safe_filename(name: str) -> str:
    bad = '<>:"/\\|?*'
    for ch in bad:
        name = name.replace(ch, "_")
    return name[:200]

def file_too_large(path: str, max_mb: int) -> bool:
    try:
        return os.path.getsize(path) > max_mb * 1024 * 1024
    except FileNotFoundError:
        return False

def looks_like_video_ext(ext: str) -> bool:
    return (ext or "").lower() in {"mp4", "mkv", "webm", "mov", "m4v"}

def detect_cookiefile() -> Optional[str]:
    for cand in ("cookies.txt", "cookies/cookies.txt", "youtube-cookies.txt"):
        if os.path.isfile(cand):
            return os.path.abspath(cand)
    return None

def ffprobe_duration(path: str) -> Optional[float]:
    try:
        out = subprocess.check_output(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=nk=1:nw=1", path],
            stderr=subprocess.STDOUT
        ).decode().strip()
        return float(out)
    except Exception:
        return None

def compute_bitrate_for_target(duration_sec: float, target_mb: int) -> int:
    """
    Very rough: target total size -> bits per second for video.
    Reserve ~128 kbps for audio, rest for video.
    """
    target_bytes = target_mb * 1024 * 1024
    if duration_sec <= 0:
        # fallback ~2 Mbps
        return int(2_000_000)
    audio_bps = 128_000
    total_bps = int((target_bytes * 8) / duration_sec)
    video_bps = max(300_000, total_bps - audio_bps)
    return video_bps

# ===================== State & models =====================

dp = Dispatcher()
router = Router()
dp.include_router(router)
g_bot: Optional[Bot] = None

@dataclass
class Job:
    job_id: str
    url: str
    chat_id: int
    msg_id: int
    upload_mode: str = DEFAULT_UPLOAD_MODE
    cancel_flag: bool = False
    last_edit: float = 0.0
    outfile: Optional[str] = None
    last_url_title: Optional[str] = None
    last_error: Optional[str] = None
    last_formats: List[int] = field(default_factory=list)  # available heights

ACTIVE: Dict[str, Job] = {}
SEM = asyncio.Semaphore(MAX_CONCURRENT)

# ===================== Keyboards =====================

def kb_progress(job: Job, show_compress: bool = False):
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="❌ Cancel", callback_data=f"cancel:{job.job_id}"),
        InlineKeyboardButton(text=("⬆️ As Video" if job.upload_mode == "video" else "📄 As Document"),
                             callback_data=f"mode:{job.job_id}:{job.upload_mode}")
    )
    kb.row(
        InlineKeyboardButton(text="🔁 Retry", callback_data=f"retry:{job.job_id}"),
        InlineKeyboardButton(text="🎛️ Formats", callback_data=f"formats:{job.job_id}")
    )
    kb.row(
        InlineKeyboardButton(text="🎵 Audio", callback_data=f"fmt:{job.job_id}:aud"),
        InlineKeyboardButton(text="🎵 MP3", callback_data=f"fmt:{job.job_id}:mp3")
    )
    if show_compress:
        kb.row(InlineKeyboardButton(text="🗜️ Compress to Fit", callback_data=f"compress:{job.job_id}"))
    kb.row(InlineKeyboardButton(text="🧹 Delete file", callback_data=f"rm:{job.job_id}"))
    return kb.as_markup()

def kb_formats(job_id: str, heights: List[int]):
    kb = InlineKeyboardBuilder()
    heights = sorted(set([h for h in heights if h]), reverse=True)[:10]
    if not heights:
        kb.row(InlineKeyboardButton(text="Best", callback_data=f"fmt:{job_id}:best"))
    else:
        kb.add(InlineKeyboardButton(text="Best", callback_data=f"fmt:{job_id}:best"))
        for h in heights:
            kb.add(InlineKeyboardButton(text=f"{h}p", callback_data=f"fmt:{job_id}:h{h}"))
        kb.adjust(3)
    kb.row(InlineKeyboardButton(text="⬅️ Back", callback_data=f"back:{job_id}"))
    return kb.as_markup()

# ===================== Message edit helper =====================

async def edit(job: Job, text: str, *, throttle: float = 1.1, show_compress: bool = False):
    now = time.time()
    if now - job.last_edit < throttle:
        return
    job.last_edit = now
    with suppress(Exception):
        await g_bot.edit_message_text(
            text, job.chat_id, job.msg_id,
            reply_markup=kb_progress(job, show_compress=show_compress)
        )

# ===================== Bot commands =====================

@router.message(Command("start"))
async def start_cmd(m: Message):
    await m.reply(
        "Send a video/page URL and I’ll:\n"
        "• start downloading **immediately** (no extra taps)\n"
        "• show live progress with **% / speed / ETA** in one message\n"
        "• upload as Video/Document (toggle)\n"
        "Buttons: Retry • Formats • Audio • MP3 • Compress-to-fit • Delete file\n\n"
        "Admin: /status, /mode video|document, /setmax <MB>, /cleanup <days>\n"
        "Use legally. DRM/paywalls/logins are not bypassed.",
        parse_mode="HTML"
    )

@router.message(Command("help"))
async def help_cmd(m: Message):
    await start_cmd(m)

@router.message(Command("mode"))
async def mode_cmd(m: Message):
    if OWNER_ID and m.from_user and m.from_user.id != OWNER_ID:
        return await m.reply("Only owner can change default mode.")
    parts = m.text.strip().split()
    if len(parts) != 2 or parts[1] not in ("video", "document"):
        return await m.reply("Usage: /mode video|document")
    global DEFAULT_UPLOAD_MODE
    DEFAULT_UPLOAD_MODE = parts[1]
    await m.reply(f"Default upload mode set to: {DEFAULT_UPLOAD_MODE}")

@router.message(Command("setmax"))
async def setmax_cmd(m: Message):
    if OWNER_ID and m.from_user and m.from_user.id != OWNER_ID:
        return await m.reply("Only owner can change max size.")
    parts = m.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await m.reply("Usage: /setmax <MB>")
    global MAX_FILE_MB
    MAX_FILE_MB = int(parts[1])
    await m.reply(f"Max upload size set to {MAX_FILE_MB} MB")

@router.message(Command("status"))
async def status_cmd(m: Message):
    q = len([1 for j in ACTIVE.values() if j.outfile is None])
    await m.reply(
        f"Active jobs: {len(ACTIVE)} (downloading: {q})\n"
        f"Concurrent limit: {MAX_CONCURRENT}\n"
        f"Default mode: {DEFAULT_UPLOAD_MODE}\n"
        f"Max upload MB: {MAX_FILE_MB}"
    )

@router.message(Command("cleanup"))
async def cleanup_cmd(m: Message):
    if OWNER_ID and m.from_user and m.from_user.id != OWNER_ID:
        return await m.reply("Only owner can cleanup.")
    parts = m.text.strip().split()
    days = int(parts[1]) if len(parts) == 2 and parts[1].isdigit() else 3
    cutoff = time.time() - days*86400
    n = 0
    for root, _, files in os.walk(DOWNLOAD_DIR):
        for f in files:
            p = os.path.join(root, f)
            try:
                if os.path.getmtime(p) < cutoff:
                    os.remove(p); n += 1
            except FileNotFoundError:
                pass
    await m.reply(f"Cleaned {n} old files (>{days} days).")

# ===================== URL handling =====================

@router.message(F.text.regexp(URL_RE))
async def handle_url(m: Message):
    url = URL_RE.search(m.text).group(1).strip()
    # Seed one message we will keep editing
    msg = await m.reply("⏳ Starting…")
    job_id = uuid.uuid4().hex[:8]
    job = Job(job_id=job_id, url=url, chat_id=msg.chat.id, msg_id=msg.message_id, upload_mode=DEFAULT_UPLOAD_MODE)
    ACTIVE[job_id] = job
    # Fire & forget
    asyncio.create_task(run_download(job))

# ===================== Callbacks =====================

@router.callback_query(F.data.startswith("mode:"))
async def cb_mode(cq: CallbackQuery):
    _, job_id, mode = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("No active job.", show_alert=True)
    if mode not in ("video", "document"):
        return await cq.answer("Invalid mode.", show_alert=True)
    job.upload_mode = "document" if mode == "video" else "video"
    await cq.answer(f"Upload mode: {job.upload_mode}")
    with suppress(Exception):
        await g_bot.edit_message_reply_markup(job.chat_id, job.msg_id, reply_markup=kb_progress(job))

@router.callback_query(F.data.startswith("cancel:")))
async def cb_cancel(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("Already gone.")
    job.cancel_flag = True
    with suppress(Exception):
        await cq.message.edit_text("🛑 Cancelled.")
    ACTIVE.pop(job_id, None)
    await cq.answer("Cancelled.")

@router.callback_query(F.data.startswith("retry:")))
async def cb_retry(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("Nothing to retry.", show_alert=True)
    # Reset and re-run
    job.cancel_flag = False
    job.outfile = None
    await cq.answer("Retrying…")
    asyncio.create_task(run_download(job))

@router.callback_query(F.data.startswith("formats:")))
async def cb_formats(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("No active job.", show_alert=True)
    heights = job.last_formats or []
    with suppress(Exception):
        await g_bot.edit_message_reply_markup(job.chat_id, job.msg_id, reply_markup=kb_formats(job_id, heights))
    await cq.answer()

@router.callback_query(F.data.startswith("back:")))
async def cb_back(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer()
    with suppress(Exception):
        await g_bot.edit_message_reply_markup(job.chat_id, job.msg_id, reply_markup=kb_progress(job))
    await cq.answer()

@router.callback_query(F.data.startswith("fmt:")))
async def cb_fmt(cq: CallbackQuery):
    _, job_id, token = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("Job missing.", show_alert=True)
    # Start a new run with the chosen token
    job.cancel_flag = False
    job.outfile = None
    await cq.answer(f"Downloading: {token}")
    asyncio.create_task(run_download(job, token_override=token))

@router.callback_query(F.data.startswith("compress:")))
async def cb_compress(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job or not job.outfile or not os.path.exists(job.outfile):
        return await cq.answer("No file to compress.", show_alert=True)
    await cq.answer("Compressing…")
    asyncio.create_task(transcode_and_upload(job))

@router.callback_query(F.data.startswith("rm:")))
async def cb_rm(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job or not job.outfile:
        return await cq.answer("Nothing to delete.", show_alert=True)
    try:
        if os.path.exists(job.outfile):
            os.remove(job.outfile)
        await cq.answer("Deleted.")
        await edit(job, "🧹 File deleted.", throttle=0)
    except Exception as e:
        await cq.answer("Delete failed.", show_alert=True)
        await edit(job, f"❌ Delete failed: <code>{e}</code>", throttle=0)

# ===================== Download / Upload =====================

def token_to_format(token: Optional[str]) -> dict:
    if not token or token == "best":
        return {"format": "bv*+ba/b"}
    if token == "aud":
        return {"format": "bestaudio/best"}
    if token == "mp3":
        return {"format": "bestaudio/best", "extract_mp3": True}
    if token.startswith("h") and token[1:].isdigit():
        h = int(token[1:])
        return {"format": f"bv*[height<={h}]+ba/b[height<={h}]"}
    return {"format": "bv*+ba/b"}

async def run_download(job: Job, token_override: Optional[str] = None):
    async with SEM:
        # Initial message
        await edit(job, "⏬ Preparing…", throttle=0)

        # pre-probe to collect formats & title
        info = None
        try:
            with yt_dlp.YoutubeDL({"skip_download": True, "quiet": True, "no_warnings": True}) as ydl:
                info = ydl.extract_info(job.url, download=False)
                job.last_url_title = info.get("title") or "video"
                job.last_formats = sorted({f.get("height") for f in (info.get("formats") or []) if f.get("height") and f.get("vcodec") not in (None, "none")}, reverse=True)
                await edit(job, f"🎬 <b>{job.last_url_title}</b>\nStarting…", throttle=0)
        except Exception:
            pass  # non-fatal

        loop = asyncio.get_running_loop()
        progress_state = {"last_ts": 0}

        def hook(d):
            if job.cancel_flag:
                raise yt_dlp.utils.DownloadError("Cancelled by user")
            status = d.get("status")
            if status == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate")
                done = d.get("downloaded_bytes") or 0
                speed = d.get("speed")
                eta = d.get("eta")
                pct = (done / total * 100) if total else 0.0
                line = (
                    f"⏬ <b>Downloading…</b>\n"
                    f"{pct:.1f}%  ({fmt_bytes(done)} / {fmt_bytes(total)})\n"
                    f"Speed: {fmt_bytes(speed)}/s   ETA: {fmt_eta(eta)}"
                )
                now = time.time()
                if now - progress_state["last_ts"] > 1.2:
                    progress_state["last_ts"] = now
                    asyncio.run_coroutine_threadsafe(edit(job, line, throttle=0), loop)
            elif status == "finished":
                fn = d.get("filename")
                if fn:
                    job.outfile = fn
                    asyncio.run_coroutine_threadsafe(edit(job, "✅ Download complete. Finalizing…", throttle=0), loop)

        outtmpl = os.path.join(DOWNLOAD_DIR, "%(title).200B [%(id)s].%(ext)s")
        choice = token_to_format(token_override)
        ydl_opts = {
            "format": choice["format"],
            "outtmpl": outtmpl,
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "concurrent_fragment_downloads": 4,
            "progress_hooks": [hook],
            "merge_output_format": "mp4",
            "restrictfilenames": False,
        }
        if choice.get("extract_mp3"):
            ydl_opts["postprocessors"] = [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }]

        cookiefile = detect_cookiefile()
        if cookiefile:
            ydl_opts["cookiefile"] = cookiefile

        try:
            def _go():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info2 = ydl.extract_info(job.url, download=True)
                    if info2 and not job.last_url_title:
                        job.last_url_title = info2.get("title")
                    return job.outfile or ydl.prepare_filename(info2)

            final_path = await loop.run_in_executor(None, _go)

            if not final_path or not os.path.exists(final_path):
                raise RuntimeError("File not found after download.")

            # Upload or offer compression
            too_big = file_too_large(final_path, MAX_FILE_MB)
            if too_big:
                size = os.path.getsize(final_path)
                await edit(
                    job,
                    f"✅ Downloaded: <code>{os.path.basename(final_path)}</code>\n"
                    f"Size: {fmt_bytes(size)}\n\n"
                    f"⚠️ Too large for Telegram (>{MAX_FILE_MB} MB).\n"
                    f"Tap <b>Compress to Fit</b> to auto-transcode & upload, or fetch from server:\n"
                    f"<code>{final_path}</code>",
                    throttle=0,
                    show_compress=True
                )
                return

            caption = "✅ Done."
            ext = os.path.splitext(final_path)[1][1:].lower()
            with suppress(Exception):
                await g_bot.edit_message_text("⬆️ Uploading…", job.chat_id, job.msg_id, reply_markup=kb_progress(job))
            if job.upload_mode == "video" and looks_like_video_ext(ext):
                with open(final_path, "rb") as f:
                    await g_bot.send_video(job.chat_id, f, caption=caption)
            else:
                with open(final_path, "rb") as f:
                    await g_bot.send_document(job.chat_id, f, caption=caption)

            with suppress(Exception):
                await g_bot.delete_message(job.chat_id, job.msg_id)

        except yt_dlp.utils.DownloadError as e:
            job.last_error = str(e)
            tip = ""
            if "Sign in to confirm you're not a bot" in str(e) or "account" in str(e).lower():
                tip = "\nTip: place a valid <code>cookies.txt</code> beside the bot."
            await edit(job, f"❌ Download error.\n<code>{e}</code>{tip}", throttle=0)
        except Exception as e:
            await edit(job, f"❌ Error: <code>{type(e).__name__}: {e}</code>", throttle=0)

async def transcode_and_upload(job: Job):
    src = job.outfile
    if not src or not os.path.exists(src):
        return await edit(job, "❌ Missing source file.", throttle=0)
    dur = ffprobe_duration(src) or 0
    target_bps = compute_bitrate_for_target(dur, MAX_FILE_MB - 5)  # keep margin
    dst = os.path.join(DOWNLOAD_DIR, f"{os.path.splitext(os.path.basename(src))[0]}.tgfit.mp4")

    await edit(job, "🗜️ Compressing to fit… this can take a while.", throttle=0)
    cmd = [
        "ffmpeg", "-y", "-i", src,
        "-c:v", "libx264", "-preset", "veryfast", "-b:v", str(target_bps),
        "-maxrate", str(int(target_bps*1.2)), "-bufsize", str(int(target_bps*2)),
        "-c:a", "aac", "-b:a", "128k",
        dst
    ]
    try:
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    except Exception as e:
        return await edit(job, f"❌ Compress failed: <code>{e}</code>", throttle=0)

    if file_too_large(dst, MAX_FILE_MB):
        return await edit(job, "❌ Still too large after compress. Try lower format.", throttle=0)

    await edit(job, "⬆️ Uploading compressed file…", throttle=0)
    with open(dst, "rb") as f:
        if job.upload_mode == "video":
            await g_bot.send_video(job.chat_id, f, caption="✅ Done (compressed).")
        else:
            await g_bot.send_document(job.chat_id, f, caption="✅ Done (compressed).")

# ===================== Runner =====================

async def main():
    global g_bot
    g_bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(g_bot)

if __name__ == "__main__":
    asyncio.run(main())
