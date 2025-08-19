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
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

import yt_dlp
import requests

# ===================== Config =====================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("Set BOT_TOKEN in .env")
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "downloads")
MAX_FILE_MB = int(os.environ.get("MAX_FILE_MB", "1900"))
DEFAULT_MODE = os.environ.get("DEFAULT_UPLOAD_MODE", "video").strip().lower()  # video|document
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

URL_RE = re.compile(r"(https?://[^\s]+)", re.IGNORECASE)

# ===================== Aiogram =====================
dp = Dispatcher()
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

def detect_cookiefile() -> Optional[str]:
    for c in ("cookies.txt","cookies/cookies.txt","youtube-cookies.txt"):
        if os.path.isfile(c):
            return os.path.abspath(c)
    return None

def bar(pct: float, width: int = 18) -> str:
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
    }

def http_get(url: str) -> str:
    # NOTE: some hosts need Referer=self and relaxed SSL; adjust if needed
    r = requests.get(url, headers=common_headers(url), timeout=20, allow_redirects=True)
    r.raise_for_status()
    return r.text

def find_direct_media(html: str, base_url: str) -> Tuple[List[str], List[str]]:
    """
    Return (m3u8_urls, mp4_urls) discovered in the HTML via common patterns.
    """
    m3u8 = set()
    mp4 = set()

    # <source src="...">
    for m in re.findall(r'<source[^>]+src=["\']([^"\']+)["\']', html, re.I):
        u = urljoin(base_url, m)
        if ".m3u8" in u: m3u8.add(u)
        if u.lower().endswith(".mp4"): mp4.add(u)

    # src/file/url keys in JS/JSON
    for m in re.findall(r'(?:src|file|hls|url)\s*[:=]\s*["\'](http[^"\']+)["\']', html, re.I):
        u = urljoin(base_url, m)
        if ".m3u8" in u: m3u8.add(u)
        if ".mp4" in u: mp4.add(u)

    # plain .m3u8 or .mp4
    for m in re.findall(r'https?://[^\s"\']+\.m3u8[^\s"\']*', html, re.I):
        m3u8.add(m)
    for m in re.findall(r'https?://[^\s"\']+\.mp4[^\s"\']*', html, re.I):
        mp4.add(m)

    return list(m3u8), list(mp4)

def m3u8_heights(m3u8_text: str) -> List[int]:
    # Parse #EXT-X-STREAM-INF lines for RESOLUTION=WxH
    hs = set()
    for line in m3u8_text.splitlines():
        m = re.search(r"RESOLUTION=\s*\d+x(\d+)", line)
        if m:
            hs.add(int(m.group(1)))
    return sorted(hs, reverse=True)

def fetch_text(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=common_headers(url), timeout=20, allow_redirects=True)
        r.raise_for_status()
        return r.text
    except Exception:
        return None

def probe_info(url: str) -> Tuple[Optional[dict], bool, Optional[str]]:
    """
    Try normal probe; on failure try generic. Return (info, used_generic, err_text_if_failed).
    """
    base = {
        "skip_download": True, "quiet": True, "no_warnings": True, "noplaylist": True,
        "http_headers": common_headers(url), "nocheckcertificate": True
    }
    cookiefile = detect_cookiefile()
    if cookiefile:
        base["cookiefile"] = cookiefile
    try:
        with yt_dlp.YoutubeDL(base) as y:
            return y.extract_info(url, download=False), False, None
    except Exception:
        try:
            base2 = dict(base); base2["force_generic_extractor"] = True
            with yt_dlp.YoutubeDL(base2) as y:
                return y.extract_info(url, download=False), True, None
        except Exception as e2:
            return None, False, f"{type(e2).__name__}: {e2}"

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
        "I show the real qualities (Best, 1080p, 720p, …) ➜ download with live <b>% / speed / ETA</b> ➜ send the file.\n"
        "<i>For some sites, a Netscape <code>cookies.txt</code> may be required (age/consent/anti-bot).</i>",
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

    # 1) yt-dlp probe (normal -> generic)
    info, used_generic, err = probe_info(url)
    heights: List[int] = []
    job_id = uuid.uuid4().hex[:8]
    job = {"url": url, "title": host_title(url), "msg": msg, "cancelled": False,
           "dl_url": None, "dl_is_direct": False}
    JOBS[job_id] = job

    if info:
        fmts = info.get("formats") or []
        for f in fmts:
            if f.get("vcodec") in (None, "none"):
                continue
            h = f.get("height")
            if isinstance(h, int) and h > 0:
                heights.append(h)
            else:
                # fallback parse
                res = f.get("resolution") or f.get("format_note") or ""
                mres = re.search(r"(\d+)\s*p", res)
                if mres:
                    heights.append(int(mres.group(1)))
        job["title"] = info.get("title") or job["title"]
        suffix = " (generic)" if used_generic else ""
        await msg.edit_text(
            f"🎬 <b>{esc(job['title'])}</b>{esc(suffix)}\nChoose a quality:",
            reply_markup=kb_format_choices(job_id, heights or []),
            parse_mode="HTML"
        )
        return

    # 2) Direct-media fallback (HTML scan)
    html = None
    try:
        html = http_get(url)
    except Exception as e:
        err = f"{type(e).__name__}: {e}"

    m3u8s, mp4s = ([], [])
    if html:
        m3u8s, mp4s = find_direct_media(html, url)

    if m3u8s:
        # Prefer the first master m3u8
        m3u8_url = m3u8s[0]
        # Try to fetch it and parse qualities
        mtxt = fetch_text(m3u8_url) or ""
        hts = m3u8_heights(mtxt)
        job["dl_url"] = m3u8_url
        job["dl_is_direct"] = True  # direct URL; but yt-dlp can still handle it
        await msg.edit_text(
            f"🎬 <b>{esc(job['title'])}</b>\n(Direct HLS found)\nChoose a quality:",
            reply_markup=kb_format_choices(job_id, hts or [1080, 720, 480, 360]),
            parse_mode="HTML"
        )
        return

    if mp4s:
        job["dl_url"] = mp4s[0]
        job["dl_is_direct"] = True
        await msg.edit_text(
            f"🎬 <b>{esc(job['title'])}</b>\n(Direct MP4 found)\nChoose:",
            reply_markup=kb_format_choices(job_id, [], include_best=True),
            parse_mode="HTML"
        )
        return

    # 3) As a last try: known embed hosts (e.g., vk/ok)
    if html:
        # Look for embedded VK/OK links
        embeds = re.findall(r'https?://(?:vk\.com|ok\.ru)/[^"\']+', html, re.I)
        if embeds:
            emb = embeds[0]
            info2, used_generic2, err2 = probe_info(emb)
            if info2:
                fmts = info2.get("formats") or []
                for f in fmts:
                    if f.get("vcodec") in (None, "none"):
                        continue
                    h = f.get("height")
                    if isinstance(h, int) and h > 0:
                        heights.append(h)
                job["url"] = emb  # switch to the real embed URL
                job["title"] = info2.get("title") or job["title"]
                suffix = " (embed)"
                await msg.edit_text(
                    f"🎬 <b>{esc(job['title'])}</b>{esc(suffix)}\nChoose a quality:",
                    reply_markup=kb_format_choices(job_id, heights or []),
                    parse_mode="HTML"
                )
                return

    # If we reached here, we couldn't prove formats. Still offer generic attempt.
    note = ""
    if err:
        e = err.lower()
        if any(w in e for w in ("sign in", "login", "account")):
            note = "\n<i>Site may require login; you can add a <code>cookies.txt</code>.</i>"
        else:
            note = "\n<i>Extractor failed; we can still try a generic download.</i>"
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

def token_to_format(token: str) -> str:
    if token == "best":
        return "bv*+ba/b"
    if token.startswith("h") and token[1:].isdigit():
        h = int(token[1:])
        return f"bv*[height<={h}]+ba/b[height<={h}]"
    return "bv*+ba/b"

@router.callback_query(F.data.startswith("get:"))
async def cb_get(cq: CallbackQuery):
    # data: get:<job_id>:<token>
    _, job_id, token = cq.data.split(":")
    job = JOBS.get(job_id)
    if not job:
        return await cq.answer("Job missing.", show_alert=True)

    src_url = job.get("dl_url") or job["url"]
    title = job["title"]
    msg = job["msg"]

    await cq.answer("Downloading…")
    await msg.edit_text(f"⏬ <b>{esc(title)}</b>\nPreparing…", reply_markup=kb_format_choices(job_id, [], include_best=False), parse_mode="HTML")

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
                schedule_edit(loop, msg, text, reply_markup=kb_format_choices(job_id, [], include_best=False))
        elif st == "finished":
            started["file"] = d.get("filename")
            schedule_edit(loop, msg, "✅ Download complete. Finalizing…", reply_markup=kb_format_choices(job_id, [], include_best=False))

    fmt_sel = token_to_format(token)
    outtmpl = os.path.join(DOWNLOAD_DIR, "%(title).200B [%(id)s].%(ext)s")

    def build_opts(force_generic: bool = False):
        opts = {
            "format": fmt_sel,
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
            "http_headers": common_headers(src_url),
            "nocheckcertificate": True,
        }
        cookiefile = detect_cookiefile()
        if cookiefile:
            opts["cookiefile"] = cookiefile
        if force_generic:
            opts["force_generic_extractor"] = True
        # If we already discovered a direct media URL, keep headers; yt-dlp can still handle m3u8/mp4
        return opts

    def run_dl_with_fallback():
        try:
            with yt_dlp.YoutubeDL(build_opts(False)) as y:
                info = y.extract_info(src_url, download=True)
                return started["file"] or y.prepare_filename(info)
        except Exception:
            with yt_dlp.YoutubeDL(build_opts(True)) as y:
                info = y.extract_info(src_url, download=True)
                return started["file"] or y.prepare_filename(info)

    try:
        final_path = await loop.run_in_executor(None, run_dl_with_fallback)
    except yt_dlp.utils.DownloadError as e:
        s = str(e)
        tip = ""
        if any(w in s.lower() for w in ("sign in", "login", "account")):
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
    await g_bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(g_bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
