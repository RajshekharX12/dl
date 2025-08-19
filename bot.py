import asyncio
import logging
import os
import re
import time
import uuid
import json
import subprocess
from datetime import datetime
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Dict, Optional, List

from dotenv import load_dotenv
load_dotenv()

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

import yt_dlp

# ---------- Optional S3 ----------
try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
except Exception:
    boto3 = None
    BotoCoreError = ClientError = NoCredentialsError = Exception
# ---------------------------------

# ===================== Config =====================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN missing in .env")

OWNER_ID = int(os.environ.get("OWNER_ID", "0") or 0)
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "downloads")
MAX_FILE_MB = int(os.environ.get("MAX_FILE_MB", "1900"))  # stay safely below Telegram bot cap
DEFAULT_UPLOAD_MODE = os.environ.get("DEFAULT_UPLOAD_MODE", "video").strip().lower()

# Quotas / limits
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", "2"))                 # global concurrent downloads
PER_USER_CONCURRENT = int(os.environ.get("PER_USER_CONCURRENT", "2"))       # per-user concurrent downloads
QUOTA_DAILY_JOBS = int(os.environ.get("QUOTA_DAILY_JOBS", "50"))            # per-user jobs/day
QUOTA_DAILY_MB = int(os.environ.get("QUOTA_DAILY_MB", "8000"))              # per-user MB/day (~8GB)

# Allow/ban
ALLOWLIST_IDS = [int(x) for x in os.environ.get("ALLOWLIST_IDS", "").split(",") if x.strip().isdigit()]
BANLIST_IDS = [int(x) for x in os.environ.get("BANLIST_IDS", "").split(",") if x.strip().isdigit()]

# S3 (optional)
S3_ENABLE = os.environ.get("S3_ENABLE", "0").lower() in {"1", "true", "yes"}
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "uploads")
S3_REGION = os.environ.get("S3_REGION", "")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
STATE_DIR = "state"
os.makedirs(STATE_DIR, exist_ok=True)
QUOTA_FILE = os.path.join(STATE_DIR, "quota.json")
ALLOW_FILE = os.path.join(STATE_DIR, "allow.json")

URL_RE = re.compile(r"(https?://[^\s]+)", re.IGNORECASE)

# ===================== Helpers =====================
def fmt_bytes(n: Optional[float]) -> str:
    if n is None:
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
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"

def looks_like_video_ext(ext: str) -> bool:
    return (ext or "").lower() in {"mp4", "mkv", "webm", "mov", "m4v"}

def safe_filename(name: str) -> str:
    for ch in '<>:"/\\|?*':
        name = name.replace(ch, "_")
    return name[:200]

def file_too_large(path: str, max_mb: int) -> bool:
    try:
        return os.path.getsize(path) > max_mb * 1024 * 1024
    except FileNotFoundError:
        return False

def today_key() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

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
    if duration_sec <= 0:
        return 2_000_000
    target_bytes = target_mb * 1024 * 1024
    total_bps = int((target_bytes * 8) / duration_sec)
    return max(300_000, total_bps - 128_000)  # reserve ~128k for audio

def html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# ===================== State =====================
dp = Dispatcher()
router = Router()
dp.include_router(router)
g_bot: Optional[Bot] = None

@dataclass
class Job:
    job_id: str
    user_id: int
    url: str
    chat_id: int
    msg_id: int
    upload_mode: str = field(default_factory=lambda: DEFAULT_UPLOAD_MODE)
    cancel_flag: bool = False
    last_edit: float = 0.0
    outfile: Optional[str] = None
    title: Optional[str] = None
    available_heights: List[int] = field(default_factory=list)

ACTIVE: Dict[str, Job] = {}
SEM = asyncio.Semaphore(MAX_CONCURRENT)
INFLIGHT_BY_USER: Dict[int, int] = {}

QUOTAS = load_json(QUOTA_FILE, {})
ALLOWSTATE = load_json(ALLOW_FILE, {
    "allow": ALLOWLIST_IDS,
    "ban": BANLIST_IDS
})

def is_allowed_user(uid: int) -> bool:
    if uid in ALLOWSTATE.get("ban", []):
        return False
    allow = ALLOWSTATE.get("allow", [])
    return (not allow) or (uid in allow) or (uid == OWNER_ID)

def add_quota(uid: int, size_bytes: int):
    d = today_key()
    day = QUOTAS.setdefault(d, {})
    u = day.setdefault(str(uid), {"jobs": 0, "mb": 0})
    u["jobs"] += 1
    u["mb"] += int(size_bytes / (1024*1024))
    save_json(QUOTA_FILE, QUOTAS)

def exceeds_quota(uid: int) -> Optional[str]:
    d = today_key()
    day = QUOTAS.get(d, {})
    u = day.get(str(uid), {"jobs": 0, "mb": 0})
    if u["jobs"] >= QUOTA_DAILY_JOBS:
        return f"Daily job limit reached ({QUOTA_DAILY_JOBS})."
    if u["mb"] >= QUOTA_DAILY_MB:
        return f"Daily data limit reached ({QUOTA_DAILY_MB} MB)."
    return None

# ===================== Keyboards =====================
def kb_progress(job: Job, show_compress: bool = False):
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="❌ Cancel", callback_data=f"cancel:{job.job_id}"),
        InlineKeyboardButton(text="🔁 Retry", callback_data=f"retry:{job.job_id}")
    )
    kb.row(
        InlineKeyboardButton(text="🎛️ Formats", callback_data=f"formats:{job.job_id}"),
        InlineKeyboardButton(text="🎵 Audio", callback_data=f"fmt:{job.job_id}:aud"),
        InlineKeyboardButton(text="🎵 MP3", callback_data=f"fmt:{job.job_id}:mp3")
    )
    vid_text = "✅ As Video" if job.upload_mode == "video" else "As Video"
    doc_text = "✅ As Document" if job.upload_mode == "document" else "As Document"
    kb.row(
        InlineKeyboardButton(text=vid_text, callback_data=f"setmode:{job.job_id}:video"),
        InlineKeyboardButton(text=doc_text, callback_data=f"setmode:{job.job_id}:document")
    )
    if show_compress:
        kb.row(InlineKeyboardButton(text="🗜️ Compress to Fit", callback_data=f"compress:{job.job_id}"))
    kb.row(InlineKeyboardButton(text="🧹 Delete file", callback_data=f"rm:{job.job_id}"))
    return kb.as_markup()

def kb_formats(job_id: str, heights: List[int]):
    kb = InlineKeyboardBuilder()
    heights = sorted(set([h for h in heights if h]), reverse=True)[:10]
    kb.add(InlineKeyboardButton(text="Best", callback_data=f"fmt:{job_id}:best"))
    for h in heights:
        kb.add(InlineKeyboardButton(text=f"{h}p", callback_data=f"fmt:{job_id}:h{h}"))
    kb.adjust(3)
    kb.row(InlineKeyboardButton(text="⬅️ Back", callback_data=f"back:{job_id}"))
    return kb.as_markup()

# ===================== Edit helper =====================
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

# ===================== Commands =====================
@router.message(Command("start"))
async def start_cmd(m: Message):
    await m.reply(
        "Drop a video/page URL – I start <b>immediately</b>.\n"
        "Live progress: <b>% / speed / ETA</b> in one message.\n"
        "Buttons: Cancel • Retry • Formats • Audio • MP3 • Upload mode • Compress • Delete.\n\n"
        "Admin: /status, /mode video|document, /setmax &lt;MB&gt;, /cleanup &lt;days&gt;, /allow &lt;id&gt;, /deny &lt;id&gt;, /allowlist\n"
        "Use legally. No DRM/paywalls/logins are bypassed."
    )

@router.message(Command("help"))
async def help_cmd(m: Message):
    await start_cmd(m)

@router.message(Command("mode"))
async def mode_cmd(m: Message):
    if OWNER_ID and (not m.from_user or m.from_user.id != OWNER_ID):
        return await m.reply("Only owner can change default mode.")
    parts = m.text.split()
    if len(parts) != 2 or parts[1] not in ("video", "document"):
        return await m.reply("Usage: /mode video|document")
    global DEFAULT_UPLOAD_MODE
    DEFAULT_UPLOAD_MODE = parts[1]
    await m.reply(f"Default upload mode set to: {html_escape(DEFAULT_UPLOAD_MODE)}")

@router.message(Command("setmax"))
async def setmax_cmd(m: Message):
    if OWNER_ID and (not m.from_user or m.from_user.id != OWNER_ID):
        return await m.reply("Only owner can change max size.")
    parts = m.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await m.reply("Usage: /setmax &lt;MB&gt;")
    global MAX_FILE_MB
    MAX_FILE_MB = int(parts[1])
    await m.reply(f"Max upload size set to {MAX_FILE_MB} MB")

@router.message(Command("status"))
async def status_cmd(m: Message):
    act = len(ACTIVE)
    per = INFLIGHT_BY_USER.get(m.from_user.id if m.from_user else 0, 0)
    q = (
        f"Active jobs: {act} • Yours: {per}\n"
        f"Concurrent: {MAX_CONCURRENT} (per-user {PER_USER_CONCURRENT})\n"
        f"Default mode: {html_escape(DEFAULT_UPLOAD_MODE)}\n"
        f"Max upload: {MAX_FILE_MB} MB\n"
        f"Daily quota: {QUOTA_DAILY_JOBS} jobs / {QUOTA_DAILY_MB} MB"
    )
    await m.reply(q)

@router.message(Command("cleanup"))
async def cleanup_cmd(m: Message):
    if OWNER_ID and (not m.from_user or m.from_user.id != OWNER_ID):
        return await m.reply("Only owner can cleanup.")
    parts = m.text.split()
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
    await m.reply(f"Deleted {n} files older than {days} days.")

@router.message(Command("allow"))
async def allow_cmd(m: Message):
    if OWNER_ID and (not m.from_user or m.from_user.id != OWNER_ID):
        return await m.reply("Owner only.")
    parts = m.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await m.reply("Usage: /allow &lt;user_id&gt;")
    uid = int(parts[1])
    allow = set(ALLOWSTATE.get("allow", []))
    allow.add(uid)
    ALLOWSTATE["allow"] = sorted(list(allow))
    save_json(ALLOW_FILE, ALLOWSTATE)
    await m.reply(f"Allowed: {uid}")

@router.message(Command("deny"))
async def deny_cmd(m: Message):
    if OWNER_ID and (not m.from_user or m.from_user.id != OWNER_ID):
        return await m.reply("Owner only.")
    parts = m.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await m.reply("Usage: /deny &lt;user_id&gt;")
    uid = int(parts[1])
    allow = set(ALLOWSTATE.get("allow", []))
    ban = set(ALLOWSTATE.get("ban", []))
    allow.discard(uid)
    ban.add(uid)
    ALLOWSTATE["allow"] = sorted(list(allow))
    ALLOWSTATE["ban"] = sorted(list(ban))
    save_json(ALLOW_FILE, ALLOWSTATE)
    await m.reply(f"Denied: {uid}")

@router.message(Command("allowlist"))
async def allowlist_cmd(m: Message):
    allow = ALLOWSTATE.get("allow", [])
    ban = ALLOWSTATE.get("ban", [])
    await m.reply(f"Allow: {allow or 'everyone'}\nBan: {ban or '—'}")

# ===================== URL handling =====================
@router.message(F.text.regexp(URL_RE))
async def handle_url(m: Message):
    uid = m.from_user.id if m.from_user else 0

    if not is_allowed_user(uid):
        return await m.reply("You are not allowed to use this bot.")

    # quota
    qreason = exceeds_quota(uid)
    if qreason:
        return await m.reply(f"Quota exceeded: {html_escape(qreason)}")

    # per-user concurrency
    if INFLIGHT_BY_USER.get(uid, 0) >= PER_USER_CONCURRENT:
        return await m.reply(f"Too many active downloads. Limit: {PER_USER_CONCURRENT}. Try again later.")

    url = URL_RE.search(m.text).group(1).strip()

    # Pre-check (tell user if downloadable before starting)
    checking = await m.reply("🔎 Checking URL…")
    try:
        with yt_dlp.YoutubeDL({"skip_download": True, "quiet": True, "no_warnings": True}) as ydl:
            info = ydl.extract_info(url, download=False)
        title = info.get("title") or "video"
        extractor = info.get("extractor") or "unknown"
        await checking.edit_text(f"✅ Looks downloadable via <code>{html_escape(extractor)}</code>\n<b>{html_escape(title)}</b>\nStarting…")
        available_heights = sorted({f.get("height") for f in (info.get("formats") or [])
                                    if f.get("height") and f.get("vcodec") not in (None, "none")}, reverse=True)
    except yt_dlp.utils.DownloadError as e:
        tip = ""
        s = str(e)
        if "Sign in to confirm you're not a bot" in s or "account" in s.lower():
            tip = "\nTip: place a valid <code>cookies.txt</code> next to the bot."
        return await checking.edit_text(f"❌ Not downloadable / needs login / DRM.\n<code>{html_escape(s)}</code>{tip}")
    except Exception as e:
        return await checking.edit_text(f"❌ Failed to read the URL.\n<code>{html_escape(str(e))}</code>")

    # Seed message we will keep editing
    prog = await m.reply("⏳ Starting… 0%")

    job_id = uuid.uuid4().hex[:8]
    job = Job(job_id=job_id, user_id=uid, url=url, chat_id=prog.chat.id, msg_id=prog.message_id)
    job.title = title
    job.available_heights = available_heights
    ACTIVE[job_id] = job
    INFLIGHT_BY_USER[uid] = INFLIGHT_BY_USER.get(uid, 0) + 1

    asyncio.create_task(run_download(job))

# ===================== Callback handlers =====================
@router.callback_query(F.data.startswith("setmode:"))
async def cb_setmode(cq: CallbackQuery):
    _, job_id, mode = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("No active job.", show_alert=True)
    if mode not in ("video", "document"):
        return await cq.answer("Invalid mode.", show_alert=True)
    job.upload_mode = mode
    await cq.answer(f"Upload mode: {mode}")
    with suppress(Exception):
        await g_bot.edit_message_reply_markup(job.chat_id, job.msg_id, reply_markup=kb_progress(job))

@router.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("Already gone.")
    job.cancel_flag = True
    with suppress(Exception):
        await cq.message.edit_text("🛑 Cancelled.")
    ACTIVE.pop(job_id, None)
    INFLIGHT_BY_USER[job.user_id] = max(0, INFLIGHT_BY_USER.get(job.user_id, 1) - 1)
    await cq.answer("Cancelled.")

@router.callback_query(F.data.startswith("retry:"))
async def cb_retry(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("Nothing to retry.", show_alert=True)
    job.cancel_flag = False
    job.outfile = None
    await cq.answer("Retrying…")
    asyncio.create_task(run_download(job))

@router.callback_query(F.data.startswith("formats:"))
async def cb_formats(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("No active job.", show_alert=True)
    with suppress(Exception):
        await g_bot.edit_message_reply_markup(job.chat_id, job.msg_id, reply_markup=kb_formats(job.job_id, job.available_heights))
    await cq.answer()

@router.callback_query(F.data.startswith("back:"))
async def cb_back(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer()
    with suppress(Exception):
        await g_bot.edit_message_reply_markup(job.chat_id, job.msg_id, reply_markup=kb_progress(job))
    await cq.answer()

@router.callback_query(F.data.startswith("fmt:"))
async def cb_fmt(cq: CallbackQuery):
    _, job_id, token = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job:
        return await cq.answer("Job missing.", show_alert=True)
    job.cancel_flag = False
    job.outfile = None
    await cq.answer(f"Downloading: {token}")
    asyncio.create_task(run_download(job, token_override=token))

@router.callback_query(F.data.startswith("compress:"))
async def cb_compress(cq: CallbackQuery):
    _, job_id = cq.data.split(":")
    job = ACTIVE.get(job_id)
    if not job or not job.outfile or not os.path.exists(job.outfile):
        return await cq.answer("No file to compress.", show_alert=True)
    await cq.answer("Compressing…")
    asyncio.create_task(transcode_and_upload(job))

@router.callback_query(F.data.startswith("rm:"))
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
        await edit(job, f"❌ Delete failed: <code>{html_escape(str(e))}</code>", throttle=0)

# ===================== Download core =====================
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
        try:
            await edit(job, "⏬ Preparing…", throttle=0)

            loop = asyncio.get_running_loop()
            progress_state = {"ts": 0}

            def hook(d):
                if job.cancel_flag:
                    raise yt_dlp.utils.DownloadError("Cancelled by user")
                if d.get("status") == "downloading":
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
                    if now - progress_state["ts"] > 1.2:
                        progress_state["ts"] = now
                        asyncio.run_coroutine_threadsafe(edit(job, line, throttle=0), loop)
                elif d.get("status") == "finished":
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

            def _go():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info2 = ydl.extract_info(job.url, download=True)
                    if info2 and not job.title:
                        job.title = info2.get("title")
                    return job.outfile or ydl.prepare_filename(info2)

            final_path = await loop.run_in_executor(None, _go)
            if not final_path or not os.path.exists(final_path):
                raise RuntimeError("File not found after download.")

            size_bytes = os.path.getsize(final_path)
            add_quota(job.user_id, size_bytes)

            if file_too_large(final_path, MAX_FILE_MB):
                # Try S3 if enabled
                if S3_ENABLE and boto3 and S3_BUCKET:
                    await edit(job, "📤 Uploading to S3 (file too large for Telegram)…", throttle=0)
                    url = await s3_upload_with_progress(job, final_path)
                    if url:
                        await edit(job, f"✅ Uploaded to S3:\n{html_escape(url)}", throttle=0)
                        return
                    else:
                        await edit(job, "❌ S3 upload failed. Tap <b>Compress to Fit</b> or pull from server path.", throttle=0, show_compress=True)
                else:
                    await edit(job,
                        f"✅ Downloaded: <code>{html_escape(os.path.basename(final_path))}</code>\n"
                        f"Size: {fmt_bytes(size_bytes)}\n\n"
                        f"⚠️ Too large for Telegram (&gt;{MAX_FILE_MB} MB).\n"
                        f"Tap <b>Compress to Fit</b> to transcode &amp; upload, or fetch from server:\n"
                        f"<code>{html_escape(final_path)}</code>",
                        throttle=0, show_compress=True
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
            s = str(e)
            tip = ""
            if "Sign in to confirm you're not a bot" in s or "account" in s.lower():
                tip = "\nTip: put a valid <code>cookies.txt</code> next to the bot."
            await edit(job, f"❌ Download error.\n<code>{html_escape(s)}</code>{tip}", throttle=0)
        except Exception as e:
            await edit(job, f"❌ Error: <code>{html_escape(type(e).__name__ + ': ' + str(e))}</code>", throttle=0)
        finally:
            if not job.outfile or not os.path.exists(job.outfile):
                ACTIVE.pop(job.job_id, None)
            INFLIGHT_BY_USER[job.user_id] = max(0, INFLIGHT_BY_USER.get(job.user_id, 1) - 1)

# ===================== Compress-to-fit & S3 =====================
async def transcode_and_upload(job: Job):
    src = job.outfile
    if not src or not os.path.exists(src):
        return await edit(job, "❌ Missing source file.", throttle=0)

    # bitrate target
    dur = ffprobe_duration(src) or 0
    target_bps = compute_bitrate_for_target(dur, MAX_FILE_MB - 5)
    dst = os.path.join(DOWNLOAD_DIR, f"{os.path.splitext(os.path.basename(src))[0]}.tgfit.mp4")

    await edit(job, "🗜️ Compressing to fit…", throttle=0)
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
        return await edit(job, f"❌ Compress failed: <code>{html_escape(str(e))}</code>", throttle=0)

    if file_too_large(dst, MAX_FILE_MB):
        return await edit(job, "❌ Still too large after compress. Use Formats to pick a lower height.", throttle=0)

    await edit(job, "⬆️ Uploading compressed file…", throttle=0)
    with open(dst, "rb") as f:
        if job.upload_mode == "video":
            await g_bot.send_video(job.chat_id, f, caption="✅ Done (compressed).")
        else:
            await g_bot.send_document(job.chat_id, f, caption="✅ Done (compressed).")

async def s3_upload_with_progress(job: Job, file_path: str) -> Optional[str]:
    if not (S3_ENABLE and boto3 and S3_BUCKET):
        return None
    try:
        s3 = boto3.client("s3", region_name=S3_REGION or None)
        key = f"{S3_PREFIX.strip('/')}/{os.path.basename(file_path)}"
        total = os.path.getsize(file_path)
        state = {"sent": 0, "ts": 0}

        def cb(bytes_amount):
            state["sent"] += bytes_amount
            now = time.time()
            if now - state["ts"] > 1.2:
                state["ts"] = now
                pct = state["sent"] / total * 100 if total else 0
                asyncio.run_coroutine_threadsafe(
                    edit(job, f"📤 S3 upload… {pct:.1f}%  ({fmt_bytes(state['sent'])}/{fmt_bytes(total)})", throttle=0),
                    asyncio.get_event_loop()
                )

        s3.upload_file(file_path, S3_BUCKET, key, Callback=cb)
        url = s3.generate_presigned_url(
            "get_object", Params={"Bucket": S3_BUCKET, "Key": key}, ExpiresIn=7*24*3600
        )
        return url
    except (BotoCoreError, ClientError, NoCredentialsError) as e:
        await edit(job, f"❌ S3 error: <code>{html_escape(str(e))}</code>", throttle=0)
        return None
    except Exception as e:
        await edit(job, f"❌ S3 error: <code>{html_escape(str(e))}</code>", throttle=0)
        return None

# ===================== Runner =====================
async def main():
    global g_bot
    g_bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(g_bot)

if __name__ == "__main__":
    asyncio.run(main())
