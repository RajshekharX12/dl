import asyncio
import logging
import os
import re
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple

from dotenv import load_dotenv
load_dotenv()

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

import yt_dlp

from utils import fmt_bytes, fmt_eta, safe_filename, file_too_large, looks_like_video_ext

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
OWNER_ID = int(os.environ.get("OWNER_ID", "0") or 0)
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "downloads")
MAX_FILE_MB = int(os.environ.get("MAX_FILE_MB", "1900"))
DEFAULT_UPLOAD_MODE = os.environ.get("DEFAULT_UPLOAD_MODE", "video").strip().lower()
ALLOWED_DOMAINS = [d.strip().lower() for d in os.environ.get("ALLOWED_DOMAINS", "").split(",") if d.strip()]

assert BOT_TOKEN, "BOT_TOKEN is required in .env"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

URL_RE = re.compile(r"(https?://[^\s]+)", re.IGNORECASE)

dp = Dispatcher()
router = Router()
dp.include_router(router)

@dataclass
class Job:
    job_id: str
    url: str
    fmt: str
    chat_id: int
    msg_id: int
    upload_mode: str
    cancel_flag: bool = False
    last_edit: float = 0.0
    outfile: Optional[str] = None
    title: Optional[str] = None

ACTIVE: Dict[str, Job] = {}  # job_id -> Job
SELECTIONS: Dict[str, Dict[str, str]] = {}  # job_id -> token -> yt-dlp format string

def is_allowed_url(url: str) -> bool:
    if not ALLOWED_DOMAINS:
        return True
    try:
        host = re.sub(r"^https?://", "", url).split("/")[0].lower()
        return any(host.endswith(d) for d in ALLOWED_DOMAINS)
    except Exception:
        return False

async def send_or_edit(bot: Bot, job: Job, text: str, kb=None, throttle=1.5):
    now = time.time()
    if now - job.last_edit < throttle:
        return
    job.last_edit = now
    with suppress(Exception):
        await bot.edit_message_text(text, job.chat_id, job.msg_id, reply_markup=kb, disable_web_page_preview=True)

def formats_from_info(info: dict) -> List[dict]:
    fmts = info.get("formats", []) or []
    # keep only reasonable entries
    filtered = []
    for f in fmts:
        if f.get("tbr") is None and f.get("filesize") is None and f.get("filesize_approx") is None:
            # still keep but it may be HLS/unknown size
            pass
        filtered.append(f)
    return filtered

def pick_buttons(info: dict) -> Tuple[List[Tuple[str, str]], str]:
    """
    Return [(label, token)] and default format string.
    token is short id we'll map to a full yt-dlp format later.
    """
    fmts = formats_from_info(info)
    heights = sorted({f.get("height") for f in fmts if f.get("vcodec") not in (None, "none") and f.get("height")}, reverse=True)
    options = []
    added_heights = set()
    for h in heights:
        if h in added_heights: 
            continue
        # prefer mp4 if possible
        best_for_h = [f for f in fmts if f.get("height")==h and f.get("vcodec") not in (None,"none")]
        mp4 = next((f for f in best_for_h if (f.get("ext")=="mp4")), None)
        chosen = mp4 or (best_for_h[0] if best_for_h else None)
        if not chosen:
            continue
        approx = chosen.get("filesize") or chosen.get("filesize_approx")
        label = f"{h}p {chosen.get('ext','?')} ~{fmt_bytes(approx)}" if approx else f"{h}p {chosen.get('ext','?')}"
        token = f"h{h}"
        options.append((label, token))
        added_heights.add(h)
        if len(options) >= 6:
            break

    # Always include Best + Audio-only + MP3
    options = [("Best", "best")] + options + [("Audio-only (best)", "aud"), ("Audio MP3", "mp3"), ("More formats…", "more")]
    default = "bv*+ba/b"
    return options, default

def token_to_format(token: str) -> dict:
    # Return dictionary specifying yt-dlp options for the chosen token
    if token == "best":
        return {"format": "bv*+ba/b"}
    if token == "aud":
        return {"format": "bestaudio/best"}
    if token == "mp3":
        # use postprocessor
        return {"format": "bestaudio/best", "extract_mp3": True}
    if token.startswith("h") and token[1:].isdigit():
        h = int(token[1:])
        return {"format": f"bv*[height<={h}]+ba/b[height<={h}]"}
    if token.startswith("fmtid:"):
        # direct format_id
        fid = token.split(":",1)[1]
        return {"format": f"{fid}"}
    return {"format": "bv*+ba/b"}

def build_quality_kb(job_id: str, pairs: List[Tuple[str,str]]):
    kb = InlineKeyboardBuilder()
    row = []
    for label, token in pairs:
        row.append(("✅ "+label if token=="best" else label, f"sel:{job_id}:{token}"))
        if len(row) == 2:
            kb.row(*[kb.button(text=t, callback_data=d) for t,d in row])
            row = []
    if row:
        kb.row(*[kb.button(text=t, callback_data=d) for t,d in row])
    # upload mode toggle
    kb.row(kb.button(text="⬆️ Upload as Video", callback_data=f"mode:{job_id}:video"),
           kb.button(text="📄 Upload as Document", callback_data=f"mode:{job_id}:document"))
    kb.row(kb.button(text="❌ Cancel", callback_data=f"cancel:{job_id}"))
    return kb.as_markup()

def build_cancel_kb(job_id: str, mode: str):
    kb = InlineKeyboardBuilder()
    kb.row(kb.button(text="❌ Cancel", callback_data=f"cancel:{job_id}"))
    kb.row(kb.button(text=("⬆️ As Video" if mode=="video" else "📄 As Document"), callback_data=f"mode:{job_id}:{mode}"))
    return kb.as_markup()

@router.message(Command("start"))
async def start(m: Message):
    await m.reply(
        "Send me a video/page URL and I’ll:\n"
        "• probe formats & show buttons (1080p/720p/…/audio/MP3)\n"
        "• download with live progress + ETA\n"
        "• upload to Telegram as video/document (toggle)\n\n"
        "Legal note: Only download content you own or have permission to. No DRM/paywalls/logins.",
        disable_web_page_preview=True
    )

@router.message(Command("help"))
async def help_cmd(m: Message):
    await start(m)

@router.message(F.text.regexp(URL_RE))
async def handle_url(m: Message):
    url = URL_RE.search(m.text).group(1).strip()

    if not is_allowed_url(url):
        return await m.reply("This domain is not allowed. Ask the admin to whitelist it via ALLOWED_DOMAINS in .env.")

    probe_msg = await m.reply("🔎 Probing formats…")

    # Extract info without downloading
    try:
        ydl_opts = {
            "skip_download": True,
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        return await probe_msg.edit_text(f"❌ Failed to read this URL.\n<code>{e}</code>")

    title = info.get("title") or "video"
    pairs, default_fmt = pick_buttons(info)

    job_id = uuid.uuid4().hex[:8]
    msg_text = f"🎬 <b>{title}</b>\nChoose a quality or format:"
    kb = build_quality_kb(job_id, pairs)
    sent = await probe_msg.edit_text(msg_text, reply_markup=kb)
    # Store selection mapping (not strictly needed here, but kept for extensibility)
    SELECTIONS[job_id] = {token: token for _, token in pairs}

    # Pre-create Job (no format chosen yet)
    ACTIVE[job_id] = Job(job_id=job_id, url=url, fmt=default_fmt, chat_id=sent.chat.id, msg_id=sent.message_id, upload_mode=DEFAULT_UPLOAD_MODE)

@router.callback_query(F.data.startswith("mode:"))
async def set_mode(cq: CallbackQuery, bot: Bot):
    _, job_id, mode = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("Job not found.", show_alert=True)
    if mode not in ("video", "document"):
        return await cq.answer("Invalid mode.", show_alert=True)
    job.upload_mode = mode
    await cq.answer(f"Upload mode set to {mode}.")
    # just update buttons (rebuild quality keyboard with current mode highlighted)
    await send_or_edit(bot, job, "Upload mode updated. Continue with your selection.", build_cancel_kb(job_id, mode), throttle=0)

@router.callback_query(F.data.startswith("cancel:"))
async def cancel_cb(cq: CallbackQuery, bot: Bot):
    _, job_id = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("Already gone.")
    job.cancel_flag = True
    await cq.message.edit_text("🛑 Cancelled.")
    ACTIVE.pop(job_id, None)
    await cq.answer("Cancelled.")

@router.callback_query(F.data.startswith("sel:"))
async def selected_format(cq: CallbackQuery, bot: Bot):
    _, job_id, token = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("Job expired.", show_alert=True)

    # translate token to yt-dlp format string + flags
    choice = token_to_format(token)
    fmt = choice["format"]
    extract_mp3 = choice.get("extract_mp3", False)

    job.fmt = fmt
    await cq.answer("Starting…")

    # Kick off download task
    asyncio.create_task(run_download(bot, job, extract_mp3=extract_mp3))

async def run_download(bot: Bot, job: Job, extract_mp3: bool = False):
    text = "⏬ Preparing download…"
    await send_or_edit(bot, job, text, build_cancel_kb(job.job_id, job.upload_mode), throttle=0)

    loop = asyncio.get_running_loop()
    progress_state = {"last_text": "", "last_update": 0}

    def hook(d):
        if job.cancel_flag:
            raise yt_dlp.utils.DownloadError("Cancelled by user")

        status = d.get("status")
        # throttle updating via thread-safe call
        if status == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            downloaded = d.get("downloaded_bytes") or 0
            speed = d.get("speed")
            eta = d.get("eta")
            pct = 0.0
            if total:
                pct = downloaded / total * 100
            line = (
                f"⏬ <b>Downloading…</b>\n"
                f"{pct:.1f}%  ({fmt_bytes(downloaded)} / {fmt_bytes(total)})\n"
                f"Speed: {fmt_bytes(speed)}/s\n"
                f"ETA: {fmt_eta(eta)}"
            )
            now = time.time()
            if now - progress_state["last_update"] > 1.5:
                progress_state["last_update"] = now
                asyncio.run_coroutine_threadsafe(send_or_edit(bot, job, line, build_cancel_kb(job.job_id, job.upload_mode), throttle=0), loop)

        elif status == "finished":
            fn = d.get("filename")
            if fn:
                job.outfile = fn
                asyncio.run_coroutine_threadsafe(send_or_edit(bot, job, "✅ Download complete. Finalizing…", build_cancel_kb(job.job_id, job.upload_mode), throttle=0), loop)

    # Prepare yt-dlp options
    outtmpl = os.path.join(DOWNLOAD_DIR, "%(title).200B [%(id)s].%(ext)s")
    ydl_opts = {
        "format": job.fmt,
        "outtmpl": outtmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "concurrent_fragment_downloads": 4,
        "progress_hooks": [hook],
        "merge_output_format": "mp4",   # prefer mp4 on merge
        "restrictfilenames": False,
    }

    if extract_mp3:
        ydl_opts["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }]

    try:
        # Run the blocking download in a thread
        def _go():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(job.url, download=True)
                # if output was postprocessed (mp3), we need final path
                if extract_mp3:
                    # yt-dlp puts filepath in 'requested_downloads'[0]['_filename'] before pp,
                    # after pp, easiest is to compute expected path from info
                    base = info.get("title") or "audio"
                    base = safe_filename(base)
                    # mp3 path will be outtmpl with .mp3
                    return os.path.join(DOWNLOAD_DIR, f"{base} [{info.get('id')}].mp3")
                # normal: final path determined by hook 'finished'
                return job.outfile or ydl.prepare_filename(info)

        final_path = await loop.run_in_executor(None, _go)

        if not final_path or not os.path.exists(final_path):
            raise RuntimeError("File not found after download.")

        # Upload decision
        if file_too_large(final_path, MAX_FILE_MB):
            size = os.path.getsize(final_path)
            msg = (
                f"✅ Downloaded: <code>{os.path.basename(final_path)}</code>\n"
                f"Size: {fmt_bytes(size)}\n\n"
                f"⚠️ Too large to upload to Telegram (>{MAX_FILE_MB} MB).\n"
                f"Use your server access to retrieve the file:\n<code>{final_path}</code>"
            )
            await send_or_edit(bot, job, msg, None, throttle=0)
        else:
            # Choose send method
            ext = os.path.splitext(final_path)[1][1:].lower()
            caption = "✅ Done."
            if job.upload_mode == "video" and looks_like_video_ext(ext):
                await bot.edit_message_text("⬆️ Uploading as video…", job.chat_id, job.msg_id)
                with open(final_path, "rb") as f:
                    await bot.send_video(job.chat_id, f, caption=caption)
            else:
                await bot.edit_message_text("⬆️ Uploading as document…", job.chat_id, job.msg_id)
                with open(final_path, "rb") as f:
                    await bot.send_document(job.chat_id, f, caption=caption)

            # tidy UI
            with suppress(Exception):
                await bot.delete_message(job.chat_id, job.msg_id)

    except yt_dlp.utils.DownloadError as e:
        if "Cancelled" in str(e):
            await send_or_edit(bot, job, "🛑 Cancelled.", None, throttle=0)
        else:
            await send_or_edit(bot, job, f"❌ Download error:\n<code>{e}</code>", None, throttle=0)
    except Exception as e:
        await send_or_edit(bot, job, f"❌ Error:\n<code>{type(e).__name__}: {e}</code>", None, throttle=0)
    finally:
        ACTIVE.pop(job.job_id, None)

async def main():
    bot = Bot(BOT_TOKEN, parse_mode="HTML")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
