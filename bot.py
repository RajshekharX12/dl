#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Izumi-style downloader bot (Aiogram v3)
Fixes:
  - "Job missing" -> jobs persisted in SQLite; auto re-create when missing.
  - "File not found after download" -> robust filepath discovery + ffmpeg/headers/cookies + generic fallback.

Features added (5+):
  1) Cookie store per domain (Paste Cookie header once; reused automatically).
  2) "Recheck now" (fresh job, no stale ID).
  3) "Force generic" extractor fallback.
  4) /status (active jobs, disk usage) and /clean (purge old files).
  5) "Show command" (sanitized yt-dlp command used), plus "Show log".
  6) Auto-detect DRM-ish failures -> clean user message.

Requirements:
  - Python 3.10+
  - aiogram 3.14.0
  - yt-dlp >= 2024.08.06
  - ffmpeg installed on system (apt install ffmpeg)
"""

import asyncio
import contextlib
import dataclasses
import datetime as dt
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# --- Config -----------------------------------------------------------------

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN env var is required")
    sys.exit(1)

ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/root/dl/out")).expanduser()
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = Path(os.getenv("DB_PATH", "bot.db")).expanduser()

# --- Database ---------------------------------------------------------------

def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db() -> None:
    con = db()
    with con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            jid TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            url TEXT NOT NULL,
            fmt TEXT,
            force_generic INTEGER DEFAULT 0,
            status TEXT NOT NULL,
            filepath TEXT,
            log TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )""")
        con.execute("""
        CREATE TABLE IF NOT EXISTS cookies (
            user_id INTEGER NOT NULL,
            domain  TEXT NOT NULL,
            cookie  TEXT NOT NULL,
            PRIMARY KEY (user_id, domain)
        )""")
    con.close()

# --- Utilities --------------------------------------------------------------

URL_RE = re.compile(r"(https?://[^\s]+)", re.IGNORECASE)

def extract_url(text: str) -> Optional[str]:
    if not text:
        return None
    m = URL_RE.search(text)
    return m.group(1) if m else None

def domain_from_url(url: str) -> str:
    # simple & safe
    m = re.match(r"https?://([^/]+)", url)
    return (m.group(1) if m else "").lower()

def now_ts() -> int:
    return int(time.time())

def human_bytes(n: int) -> str:
    step = 1024.0
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < step:
            return f"{n:.0f} {unit}"
        n /= step
    return f"{n:.1f} PB"

def disk_usage_str(path: Path) -> str:
    total, used, free = shutil.disk_usage(path)
    return f"Used {human_bytes(used)} / Total {human_bytes(total)} (Free {human_bytes(free)})"

def sanitized_cookie_preview(cookie: str) -> str:
    # Hide everything except cookie keys
    keys = [kv.split("=")[0].strip() for kv in cookie.split(";") if "=" in kv]
    return "; ".join(f"{k}=***" for k in keys[:10])

def ffmpeg_present() -> bool:
    return shutil.which("ffmpeg") is not None

# --- Job model --------------------------------------------------------------

@dataclasses.dataclass
class Job:
    jid: str
    user_id: int
    url: str
    fmt: Optional[str] = None
    force_generic: bool = False
    status: str = "pending"  # pending / running / done / failed / canceled
    filepath: Optional[str] = None
    log: str = ""
    created_at: int = dataclasses.field(default_factory=now_ts)
    updated_at: int = dataclasses.field(default_factory=now_ts)

    @staticmethod
    def from_row(row: sqlite3.Row) -> "Job":
        return Job(
            jid=row["jid"],
            user_id=row["user_id"],
            url=row["url"],
            fmt=row["fmt"],
            force_generic=bool(row["force_generic"]),
            status=row["status"],
            filepath=row["filepath"],
            log=row["log"] or "",
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

def job_create(user_id: int, url: str, fmt: Optional[str] = None, force_generic: bool = False) -> Job:
    jid = str(uuid.uuid4())
    j = Job(jid=jid, user_id=user_id, url=url, fmt=fmt, force_generic=force_generic)
    con = db()
    with con:
        con.execute("""
            INSERT INTO jobs (jid, user_id, url, fmt, force_generic, status, filepath, log, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (j.jid, j.user_id, j.url, j.fmt, int(j.force_generic), j.status, j.filepath, j.log, j.created_at, j.updated_at))
    con.close()
    return j

def job_get(jid: str) -> Optional[Job]:
    con = db()
    try:
        row = con.execute("SELECT * FROM jobs WHERE jid=?", (jid,)).fetchone()
        return Job.from_row(row) if row else None
    finally:
        con.close()

def job_update(j: Job) -> None:
    j.updated_at = now_ts()
    con = db()
    with con:
        con.execute("""
            UPDATE jobs SET fmt=?, force_generic=?, status=?, filepath=?, log=?, updated_at=?
            WHERE jid=?
        """, (j.fmt, int(j.force_generic), j.status, j.filepath, j.log, j.updated_at, j.jid))
    con.close()

def cookie_get(user_id: int, domain: str) -> Optional[str]:
    con = db()
    try:
        row = con.execute("SELECT cookie FROM cookies WHERE user_id=? AND domain=?", (user_id, domain)).fetchone()
        return row["cookie"] if row else None
    finally:
        con.close()

def cookie_set(user_id: int, domain: str, cookie: str) -> None:
    con = db()
    with con:
        con.execute("""
            INSERT INTO cookies (user_id, domain, cookie)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, domain) DO UPDATE SET cookie=excluded.cookie
        """, (user_id, domain, cookie.strip()))
    con.close()

# --- YT-DLP wrapper ---------------------------------------------------------

# We import lazily so the bot can still /start even if yt_dlp missing.
def _import_yt_dlp():
    try:
        import yt_dlp  # type: ignore
        return yt_dlp
    except Exception as e:
        return None

class BufferLogger(logging.Logger):
    def __init__(self, name="ydl", level=logging.INFO):
        super().__init__(name, level=level)
        self.buf: List[str] = []

    def info(self, msg, *args, **kwargs):
        s = str(msg)
        self.buf.append(s)
        super().info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        s = f"WARNING: {msg}"
        self.buf.append(s)
        super().warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        s = f"ERROR: {msg}"
        self.buf.append(s)
        super().error(msg, *args, **kwargs)

def build_format_selector(choice: Optional[str]) -> str:
    # defaults tuned for mp4 merges
    if not choice or choice == "best":
        return "bv*+ba/b"  # best video+audio, fallback to best
    if choice == "1080p":
        return "bv*[height<=1080]+ba/b[height<=1080]"
    if choice == "720p":
        return "bv*[height<=720]+ba/b[height<=720]"
    # as a safe fallback
    return "bv*+ba/b"

async def run_download(
    j: Job,
    user_cookie: Optional[str],
) -> Tuple[Optional[Path], str, str]:
    """
    Returns: (filepath, sanitized_command_text, short_result_message)
    """
    yt_dlp = _import_yt_dlp()
    log = BufferLogger()
    if yt_dlp is None:
        msg = "yt-dlp is not installed. Run: pip install -U yt-dlp"
        j.status = "failed"
        j.log = msg
        job_update(j)
        return None, "", msg

    fmt = build_format_selector(j.fmt or "best")
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if user_cookie:
        headers["Cookie"] = user_cookie

    if not ffmpeg_present():
        log.warning("ffmpeg not found on system; merging may fail. Install with: apt install ffmpeg")

    outtmpl = str(DOWNLOAD_DIR / "%(title).200B [%(id)s].%(ext)s")

    ydl_opts = {
        "outtmpl": outtmpl,
        "format": fmt,
        "merge_output_format": "mp4",  # ensure final .mp4 when merging
        "restrictfilenames": False,
        "noprogress": True,
        "quiet": True,
        "no_warnings": True,
        "concurrent_fragment_downloads": 8,
        "http_headers": headers,
        "logger": log,
    }
    if j.force_generic:
        ydl_opts["force_generic_extractor"] = True

    # Try: extractor -> if fails and not forced generic, auto retry with generic once
    tried_generic = False
    last_exc_text = ""

    for attempt in (1, 2):
        try:
            def _do():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    return ydl.extract_info(j.url, download=True)
            info = await asyncio.to_thread(_do)
            # Derive final file path robustly
            # 1) new 'requested_downloads' API
            filepath = None
            rds = info.get("requested_downloads") if isinstance(info, dict) else None
            if isinstance(rds, list) and rds:
                # pick the largest file or the last merged file
                candidates = []
                for it in rds:
                    fp = it.get("filepath")
                    if fp:
                        p = Path(fp)
                        if p.exists():
                            candidates.append((p.stat().st_size, p))
                if candidates:
                    filepath = sorted(candidates, key=lambda x: x[0], reverse=True)[0][1]
            # 2) try prepare_filename
            if not filepath:
                def _prep():
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        return Path(ydl.prepare_filename(info))
                p = await asyncio.to_thread(_prep)
                # Sometimes the merged file is .mp4 not the ext predicted
                if p.exists():
                    filepath = p
                else:
                    # Look up by ID in DOWNLOAD_DIR
                    vid = info.get("id", "")
                    best = None
                    for f in DOWNLOAD_DIR.glob(f"*{vid}*"):
                        if f.is_file():
                            size = f.stat().st_size
                            if not best or size > best[0]:
                                best = (size, f)
                    if best:
                        filepath = best[1]

            if not filepath or not filepath.exists():
                raise FileNotFoundError("Downloaded file could not be located after merge.")

            # success
            cmd_text = f"yt-dlp -o '{outtmpl}' -f \"{fmt}\" {'--force-generic-extractor' if j.force_generic else ''}"
            if user_cookie:
                cmd_text += "  # + Cookie header (hidden)"
            return filepath, cmd_text, "ok"

        except Exception as e:
            last_exc_text = str(e)
            log.error(last_exc_text)
            if ("This video is DRM protected" in last_exc_text
                or "Unsupported DRM" in last_exc_text
                or "encrypted" in last_exc_text.lower()):
                return None, "", "drm"
            if j.force_generic or tried_generic:
                break
            # one retry with generic extractor
            tried_generic = True
            ydl_opts["force_generic_extractor"] = True

    # failed
    cmd_text = f"yt-dlp -o '{outtmpl}' -f \"{fmt}\" {'--force-generic-extractor' if (j.force_generic or tried_generic) else ''}"
    if user_cookie:
        cmd_text += "  # + Cookie header (hidden)"
    return None, cmd_text, "fail"

# --- Bot UI -----------------------------------------------------------------

router = Router()

def kb_main(jid: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    # quality row
    b.button(text="Best", callback_data=f"act=fmt_best|jid={jid}")
    b.button(text="1080p", callback_data=f"act=fmt_1080|jid={jid}")
    b.button(text="720p", callback_data=f"act=fmt_720|jid={jid}")
    b.adjust(3)
    # tools
    b.button(text="📋 Paste Cookie header", callback_data=f"act=cookie|jid={jid}")
    b.adjust(1)
    b.button(text="🔁 Recheck now", callback_data=f"act=recheck|jid={jid}")
    b.button(text="🧪 Force generic", callback_data=f"act=generic|jid={jid}")
    b.adjust(2)
    b.button(text="📄 Show log", callback_data=f"act=log|jid={jid}")
    b.button(text="🔧 Show command", callback_data=f"act=cmd|jid={jid}")
    b.adjust(2)
    b.button(text="✖️ Cancel", callback_data=f"act=cancel|jid={jid}")
    b.adjust(1)
    return b.as_markup()

def parse_cb(data: str) -> Dict[str, str]:
    # "act=fmt_720|jid=UUID"
    out: Dict[str, str] = {}
    for part in data.split("|"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k] = v
    return out

async def send_controls(msg: Message, url: str, j: Job) -> None:
    caption = (
        f"URL: {url}\n"
        f"Job: <code>{j.jid}</code>\n"
        f"Status: {j.status}\n\n"
        f"Pick a quality or paste cookies / force generic if needed."
    )
    await msg.answer(caption, reply_markup=kb_main(j.jid))

# --- Handlers ---------------------------------------------------------------

@router.message(CommandStart())
async def on_start(m: Message):
    text = (
        "Send me a video/page URL. I’ll fetch it with yt-dlp.\n\n"
        "<b>Tips</b>\n"
        "• If you get <i>File not found after download</i>, paste Cookie header or try Force generic.\n"
        "• Use <code>/status</code> for active jobs & disk usage. <code>/clean</code> to delete old files.\n"
        "• Cookies are saved per domain for you.\n"
    )
    await m.answer(text)

@router.message(Command("status"))
async def on_status(m: Message):
    con = db()
    rows = con.execute("SELECT status, COUNT(*) c FROM jobs GROUP BY status").fetchall()
    con.close()
    parts = [f"{r['status']}: {r['c']}" for r in rows] or ["no jobs"]
    await m.answer(
        "Jobs → " + ", ".join(parts) + f"\nDownloads dir: <code>{DOWNLOAD_DIR}</code>\n{disk_usage_str(DOWNLOAD_DIR)}"
    )

@router.message(Command("clean"))
async def on_clean(m: Message):
    # delete files older than 3 days to free space
    cutoff = time.time() - 3 * 24 * 3600
    removed = 0
    for p in DOWNLOAD_DIR.glob("*"):
        with contextlib.suppress(Exception):
            if p.is_file() and p.stat().st_mtime < cutoff:
                p.unlink()
                removed += 1
    await m.answer(f"Cleaned {removed} old files from {DOWNLOAD_DIR}.")

@router.message()
async def on_message_url(m: Message):
    url = extract_url(m.text or "")
    if not url:
        return  # ignore non-URLs
    j = job_create(m.from_user.id, url=url, fmt=None, force_generic=False)
    await send_controls(m, url, j)

@router.callback_query(F.data.startswith("act="))
async def on_cb(cb: CallbackQuery):
    data = parse_cb(cb.data or "")
    act = data.get("act", "")
    jid = data.get("jid", "")
    j = job_get(jid) if jid else None

    # If job missing → rebuild from message's URL (prevents "Job missing")
    if j is None:
        url = extract_url(cb.message.text or "") if cb.message else None
        if url:
            j = job_create(cb.from_user.id, url)
        else:
            await cb.answer("Expired. Send the URL again.", show_alert=True)
            return

    # Ensure the message references the URL (for UX)
    url_in_msg = extract_url(cb.message.text or "") if cb.message else None
    if not url_in_msg:
        url_in_msg = j.url

    if act.startswith("fmt_"):
        choice = "best" if act == "fmt_best" else ("1080p" if act == "fmt_1080" else "720p")
        j.fmt = choice
        job_update(j)
        await cb.answer(f"Starting {choice}…")
        asyncio.create_task(process_download(cb, j))
        return

    if act == "cookie":
        await cb.message.answer(
            "Reply to <b>this message</b> with your <code>Cookie</code> header copied from your browser.\n"
            "Example:\n<code>Cookie: key1=value1; key2=value2; ...</code>\n\n"
            "Tip: You can omit the leading <code>Cookie:</code> — I’ll handle it.",
        )
        await cb.answer()
        return

    if act == "recheck":
        await cb.answer("Rechecking…")
        asyncio.create_task(process_download(cb, j, fresh=True))
        return

    if act == "generic":
        j.force_generic = True
        job_update(j)
        await cb.answer("Will use generic extractor.")
        asyncio.create_task(process_download(cb, j, fresh=True))
        return

    if act == "log":
        txt = j.log or "(empty)"
        if len(txt) > 3500:
            txt = txt[-3500:]
        await cb.message.answer(f"<b>Last log</b> (tail):\n<code>{html_escape(txt)}</code>")
        await cb.answer()
        return

    if act == "cmd":
        # Reconstruct a sanitized command preview
        fmt = build_format_selector(j.fmt or "best")
        outtmpl = str(DOWNLOAD_DIR / "%(title).200B [%(id)s].%(ext)s")
        cookie = cookie_get(j.user_id, domain_from_url(j.url))
        cmd = f"yt-dlp -o '{outtmpl}' -f \"{fmt}\""
        if j.force_generic:
            cmd += " --force-generic-extractor"
        if cookie:
            cmd += f"\n# Cookie: {sanitized_cookie_preview(cookie)}"
        await cb.message.answer(f"<b>Command used</b>:\n<code>{html_escape(cmd)}</code>")
        await cb.answer()
        return

    if act == "cancel":
        j.status = "canceled"
        job_update(j)
        await cb.answer("Canceled.")
        await cb.message.edit_text(f"URL: {url_in_msg}\nJob: <code>{j.jid}</code>\nStatus: canceled")
        return

# Cookie capture: user replies to bot message with cookie header
@router.message(F.reply_to_message.as_({"text": F.startswith("URL: ")}))
async def on_cookie_reply(m: Message):
    # Accept either "Cookie: ..." or raw "key=val; key2=val2"
    text = (m.text or "").strip()
    cookie = text
    if text.lower().startswith("cookie:"):
        cookie = text.split(":", 1)[1].strip()
    # Sanity
    if ";" not in cookie or "=" not in cookie:
        await m.reply("That doesn't look like a cookie header. Paste the full line you copied from DevTools.")
        return
    url = extract_url(m.reply_to_message.text or "") or ""
    dom = domain_from_url(url)
    cookie_set(m.from_user.id, dom, cookie)
    await m.reply(f"Cookie saved for <code>{dom}</code> → {html_escape(sanitized_cookie_preview(cookie))}")

# --- Download worker --------------------------------------------------------

def html_escape(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

async def process_download(cb: CallbackQuery, j: Job, fresh: bool = False):
    # Prepare
    if fresh:
        j.status = "pending"
        j.filepath = None
        j.log = ""
        job_update(j)

    # Load cookie for this domain/user
    cookie = cookie_get(j.user_id, domain_from_url(j.url))

    # Mark running
    j.status = "running"
    job_update(j)

    # Run
    path, cmd_text, result = await run_download(j, user_cookie=cookie)

    # Handle outcomes
    if result == "ok" and path:
        j.status = "done"
        j.filepath = str(path)
        # Append short success log
        j.log = (j.log or "") + f"\nSaved: {path} ({human_bytes(path.stat().st_size)})"
        job_update(j)
        try:
            # Telegram limits: ~2GB for most accounts
            size = path.stat().st_size
            if size <= 1_900_000_000:
                await cb.message.answer_document(
                    document=path.open("rb"),
                    caption=f"✅ Done\n<code>{path.name}</code>\n{human_bytes(size)}",
                )
            else:
                await cb.message.answer(
                    f"✅ Done (local save)\n<code>{html_escape(str(path))}</code>\n{human_bytes(size)}\n"
                    f"Too large to send via Telegram."
                )
        except Exception as e:
            await cb.message.answer(f"Saved to: <code>{html_escape(str(path))}</code>\n(send failed: {html_escape(str(e))})")
        await safe_edit_status(cb, j, extra="done")
        return

    if result == "drm":
        j.status = "failed"
        j.log = (j.log or "") + "\nDRM/encrypted stream not supported by yt-dlp."
        job_update(j)
        await cb.message.answer("❌ The stream appears to be DRM/encrypted. yt-dlp can’t decrypt it.")
        await safe_edit_status(cb, j, extra="failed (DRM)")
        return

    # fail
    j.status = "failed"
    if cmd_text:
        j.log = (j.log or "") + f"\nCMD: {cmd_text}"
    job_update(j)

    hints = []
    if not ffmpeg_present():
        hints.append("Install ffmpeg: <code>sudo apt install -y ffmpeg</code>")
    hints.append("Try pasting Cookie header (login/age/region locks).")
    if not j.force_generic:
        hints.append("Try: 🧪 Force generic.")
    await cb.message.answer("❌ Download failed.\n" + "\n".join(f"• {h}" for h in hints))
    await safe_edit_status(cb, j, extra="failed")

async def safe_edit_status(cb: CallbackQuery, j: Job, extra: str = ""):
    try:
        url_in_msg = extract_url(cb.message.text or "") if cb.message else j.url
        await cb.message.edit_text(
            f"URL: {url_in_msg}\nJob: <code>{j.jid}</code>\nStatus: {j.status}{(' — ' + extra) if extra else ''}",
            reply_markup=kb_main(j.jid),
        )
    except Exception:
        pass

# --- Main -------------------------------------------------------------------

async def main():
    init_db()
    bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)
    dp = Dispatcher()
    dp.include_router(router)

    print(f"[bot] Started. Download dir: {DOWNLOAD_DIR}")
    if not ffmpeg_present():
        print("[warn] ffmpeg not found; install with: sudo apt install -y ffmpeg")

    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped.")
