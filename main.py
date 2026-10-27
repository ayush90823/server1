import os
import re
import hashlib
import json
import mimetypes
import shutil
import tempfile
import hmac
from html import escape as html_escape
import logging
import urllib.parse
import asyncio
import math
import time
from collections import OrderedDict
from contextlib import asynccontextmanager

import aiohttp

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, HTMLResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pyrogram import Client
from pyrogram.errors import FloodWait
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ── ENV CONFIG ────────────────────────────────────────

BOT_TOKEN        = os.getenv("BOT_TOKEN", "")
API_ID           = int(os.getenv("API_ID", "0"))
API_HASH         = os.getenv("API_HASH", "")
STORAGE_CHANNEL  = int(os.getenv("STORAGE_CHANNEL", "0"))
SECRET_KEY       = os.getenv("SECRET_KEY", "mysecretkey123")
BASE_URL         = os.getenv("BASE_URL", "http://localhost:8000")
PORT             = int(os.getenv("PORT", 8000))
ALLOWED_USERS    = os.getenv("ALLOWED_USERS", "")
FIREBASE_URL     = os.getenv("FIREBASE_URL", "")  # e.g. https://animeverse-9eada-default-rtdb.firebaseio.com
SERVER_NAME      = os.getenv("SERVER_NAME", "Player")  # Firebase mein server field ki value
ORIGIN_URL       = os.getenv("ORIGIN_URL", "")  # tunnel ka current URL (workflow khud set karta hai)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

if SECRET_KEY in ("", "mysecretkey123"):
    logger.warning("SECRET_KEY set nahi hai ya default hai! GitHub Secrets mein apna lamba random SECRET_KEY daalo.")
if not ALLOWED_USERS.strip():
    logger.warning("ALLOWED_USERS khaali hai — bot sabke liye khula hai!")

# Single global Pyrogram Client instance — Yeh Bot aur Server dono ke liye ek hi rahega
pyro = Client(
    "stream_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
    no_updates=True,
    sleep_threshold=60,
    max_concurrent_transmissions=int(os.getenv("PYRO_TRANSMISSIONS", "12")),  # parallel download/upload speed. Low-RAM host par 4 karo
)

# MESSAGE CACHE (bounded — low-RAM ke liye purane entries auto-evict honge)
message_cache: OrderedDict = OrderedDict()
MESSAGE_CACHE_LIMIT = 50  # max 50 messages cache mein
MESSAGE_CACHE_TTL = 1800  # 30 min baad entry dobara fetch hogi (file_reference expire hone se bachne ke liye)

# ── IN-MEMORY USER SETUP STORE ────────────────────────
user_setup: dict = {}

# ── ZIP FEATURE STATE ─────────────────────────────────
zip_session: dict = {}   # uid -> {"videos": [...]}  (collect mode)
pending_zip: dict = {}   # uid -> zip upload ho chuka, episode/range ka jawab baaki
zip_jobs: dict = {}      # uid -> running job (cancel flag)
pending_zip_next: dict = {}   # uid -> [pending batch, ...]  (pehle wale ka jawab aane tak line me)
heavy_queue: "HeavyQueue" = None   # run_bot() me banta hai
bot_app = None
uv_server = None
handover = None
user_locks: dict = {}    # uid -> asyncio.Lock (episode flow ko ek user ke liye serial rakhta hai)


def user_lock(uid: int) -> asyncio.Lock:
    lk = user_locks.get(uid)
    if lk is None:
        lk = user_locks[uid] = asyncio.Lock()
    return lk

# ── QUALITY BUFFER ─────────────────────────────────────
quality_buffer: dict = {}
QUALITY_COUNT = 3  # 480p, 720p, 1080p


def quality_sort_key(q: str) -> int:
    m = re.match(r"(\d+)", q or "")
    return int(m.group(1)) if m else 0


def assign_qualities(videos: list) -> dict:
    sorted_vids = sorted(enumerate(videos), key=lambda x: x[1]["size"])
    quality_names = ["480p", "720p", "1080p"]
    if len(sorted_vids) == 2:
        quality_names = ["480p", "1080p"]
    result = {}
    for i, (orig_idx, _) in enumerate(sorted_vids):
        result[orig_idx] = quality_names[i] if i < len(quality_names) else f"quality{i}"
    return result


# ── HELPERS WITH ANIMEVERSE BRANDING & LOCK ───────────

def is_allowed(user_id):
    if not ALLOWED_USERS.strip():
        return True
    return str(user_id) in [u.strip() for u in ALLOWED_USERS.split(",")]


def generate_code(msg_id, filename):
    raw = f"{SECRET_KEY}:animeverse:{msg_id}:{filename}"
    return hashlib.md5(raw.encode()).hexdigest()[:24]


def make_stream_link(msg_id, filename):
    safe = urllib.parse.quote(filename)
    code = generate_code(msg_id, filename)
    return f"{BASE_URL}/animeverse/dl/{msg_id}/{safe}?code={code}"


def make_download_link(msg_id, filename):
    safe = urllib.parse.quote(filename)
    code = generate_code(msg_id, filename)
    return f"{BASE_URL}/animeverse/dl/{msg_id}/{safe}?code={code}&dl=1"


def make_embed_link(msg_id, filename):
    safe = urllib.parse.quote(filename)
    code = generate_code(msg_id, filename)
    return f"{BASE_URL}/animeverse/watch/{msg_id}/{safe}?code={code}"


def verify_code(msg_id, filename, code):
    return hmac.compare_digest(generate_code(msg_id, filename), code or "")


# ── FIREBASE STATE SAVE / LOAD ─────────────────────────

async def publish_origin():
    """Tunnel ka current address Firebase mein likhta hai taaki Worker usko padh sake."""
    if not (FIREBASE_URL and ORIGIN_URL):
        return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(
                f"{FIREBASE_URL.rstrip('/')}/stream_origin.json",
                json={"url": ORIGIN_URL.rstrip("/"), "time": int(time.time())},
            ) as resp:
                logger.info(f"Origin publish status: {resp.status}")
    except Exception as e:
        logger.error(f"publish_origin error: {e}")


_save_lock = asyncio.Lock()
_save_dirty = False


async def save_state_to_firebase():
    """Coalesced save: kai handlers ek saath bulayen to bhi ek hi PUT chalta hai (aur order galat nahi hota)."""
    global _save_dirty
    if not FIREBASE_URL:
        return
    _save_dirty = True
    if _save_lock.locked():
        return              # jo save chal raha hai wo dirty flag dekh kar dobara likh dega
    async with _save_lock:
        while _save_dirty:
            _save_dirty = False
            await _save_state_once()


async def _save_state_once():
    try:
        db_url = FIREBASE_URL.rstrip("/")
        setup_data = {str(k): v for k, v in user_setup.items()}
        buffer_data = {
            str(uid): {str(ep): vids for ep, vids in eps.items()}
            for uid, eps in quality_buffer.items()
        }
        payload = {
            "user_setup": setup_data,
            "quality_buffer": buffer_data,
            "zip_session": {str(k): v for k, v in zip_session.items()},
            "pending_zip": {str(k): v for k, v in pending_zip.items()},
            "pending_zip_next": {str(k): v for k, v in pending_zip_next.items() if v},
            "zip_queue": heavy_queue.snapshot() if heavy_queue else [],
        }
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.put(f"{db_url}/bot_state.json", json=payload) as resp:
                if resp.status == 200:
                    logger.info("State Firebase mein save ho gaya.")
                else:
                    logger.warning(f"State save failed: {resp.status}")
    except Exception as e:
        logger.error(f"save_state_to_firebase error: {e}")


restored_queue: list = []


async def load_state_from_firebase():
    if not FIREBASE_URL:
        return
    try:
        db_url = FIREBASE_URL.rstrip("/")
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"{db_url}/bot_state.json") as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
                if not data:
                    return
                for k, v in data.get("user_setup", {}).items():
                    user_setup[int(k)] = v
                for uid_str, eps in data.get("quality_buffer", {}).items():
                    uid = int(uid_str)
                    quality_buffer[uid] = {int(ep): vids for ep, vids in eps.items()}
                for k, v in (data.get("zip_session") or {}).items():
                    v.setdefault("videos", [])
                    zip_session[int(k)] = v
                for k, v in (data.get("pending_zip") or {}).items():
                    pending_zip[int(k)] = v
                for k, v in (data.get("pending_zip_next") or {}).items():
                    pending_zip_next[int(k)] = list(v) if isinstance(v, list) else list(v.values())
                restored_queue[:] = list(data.get("zip_queue") or [])
                logger.info(f"State load ho gaya. Users: {len(user_setup)}, Buffers: {len(quality_buffer)}, Queue: {len(restored_queue)}")
    except Exception as e:
        logger.error(f"load_state_from_firebase error: {e}")


def extract_episode(text: str):
    if not text:
        return None
    t = text.upper()
    ep_num = None
    se_match = re.search(r'S\d{1,2}\s*E(\d{1,3})', t)
    if se_match:
        return int(se_match.group(1))
    ep_match = re.search(r'\bEP(?:ISODE)?\s*[-:→►\s]*\s*(\d{1,3})\b', t)
    if ep_match:
        ep_num = int(ep_match.group(1))
    if ep_num is None:
        e_match = re.search(r'\bE(\d{1,3})\b', t)
        if e_match:
            ep_num = int(e_match.group(1))
    if ep_num is None:
        cleaned = re.sub(r'\bS\d{1,2}\b', '', t)
        nums = re.findall(r'\b(\d{1,2})\b', cleaned)
        if nums:
            ep_num = int(nums[0])
    return ep_num


def extract_quality(text: str):
    if not text:
        return None
    q_match = re.search(r'\b(1080[Pp]|720[Pp]|480[Pp])\b', text)
    if q_match:
        val = q_match.group(1).lower()
        return val
    return None


async def copy_with_floodwait(context, chat_id, from_chat_id, message_id, max_retries=10):
    for attempt in range(max_retries):
        try:
            return await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=from_chat_id,
                message_id=message_id,
            )
        except RetryAfter as e:
            ra = e.retry_after
            wait_s = ra.total_seconds() if hasattr(ra, "total_seconds") else ra
            logger.warning(f"FloodWait: {wait_s}s wait kar raha hoon (attempt {attempt+1})")
            await asyncio.sleep(wait_s + 1)
    raise RuntimeError("Max retries exceeded for copy_message (FloodWait loop).")


async def save_to_firebase_with_retry(slug, season, ep_num, stream_link, quality=None, download_link=None, max_retries=3):
    for attempt in range(max_retries):
        if await save_to_firebase(slug, season, ep_num, stream_link, quality, download_link):
            return True
        await asyncio.sleep(2 * (attempt + 1))
    return False


def get_extension(filename: str, fallback: str = "mp4") -> str:
    if filename and "." in filename:
        return filename.rsplit(".", 1)[-1].lower()
    return fallback


async def save_to_firebase(slug: str, season: str, ep_num: int, stream_link: str, quality: str = None, download_link: str = None) -> bool:
    try:
        from datetime import datetime, timezone
        ep_key     = f"E{ep_num}"
        db_url     = FIREBASE_URL.rstrip("/")
        now_ts     = int(time.time())
        season_num = int(re.sub(r'[^\d]', '', season) or "1")
        date_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        if quality:
            ep_path = f"anime_links/{slug}/{season}/{ep_key}/{quality}"
        else:
            ep_path = f"anime_links/{slug}/{season}/{ep_key}"

        url1 = f"{db_url}/{ep_path}.json"
        payload1 = {
            "link"  : stream_link,
            "server": SERVER_NAME,
            "time"  : now_ts,
        }
        if download_link:
            payload1["dl_link"] = download_link

        async with aiohttp.ClientSession() as session:
            async with session.put(url1, json=payload1) as resp:
                if resp.status == 200:
                    logger.info(f"Firebase saved: {ep_path}")
                else:
                    text = await resp.text()
                    logger.error(f"Firebase error {resp.status}: {text}")
                    return False

            if quality is None or quality == "1080p":
                url2 = f"{db_url}/added_today/{date_str}/{slug}.json"
                payload2 = {
                    "e"        : ep_num,
                    "s"        : season_num,
                    "timestamp": now_ts,
                }
                async with session.put(url2, json=payload2) as resp:
                    if resp.status == 200:
                        logger.info(f"added_today saved: {date_str}/{slug} S{season_num}E{ep_num}")
                    else:
                        text = await resp.text()
                        logger.warning(f"added_today failed {resp.status}: {text}")
        return True
    except Exception as e:
        logger.error(f"Firebase save error: {e}")
        return False


# ── ZIP (FULL SEASON) SUPPORT ─────────────────────────
# Setup ke baad agar .zip file bheji jaye to wo episode ki tarah nahi, balki
# poore season ke ZIP ki tarah treat hoti hai aur Firebase me
# zip_links/{slug}/{season}/{quality} par save hoti hai (player.html yahi padhta hai).

def is_zip_file(msg, raw_name: str) -> bool:
    if not msg.document:
        return False
    mime = (getattr(msg.document, "mime_type", "") or "").lower()
    name = (raw_name or "").lower()
    return name.endswith(".zip") or mime in ("application/zip", "application/x-zip-compressed")


def extract_zip_quality(*texts):
    for t in texts:
        if not t:
            continue
        m = re.search(r'(?<!\d)(1080|720|480)\s*[pP](?![a-zA-Z0-9])', t)
        if m:
            return f"{m.group(1)}p"
    return None


def human_size(num_bytes: int) -> str:
    if not num_bytes:
        return ""
    gb = num_bytes / (1024 ** 3)
    if gb >= 1:
        return f"{gb:.1f} GB"
    return f"{num_bytes / (1024 ** 2):.0f} MB"


_QUALITY_STRIP_RE = re.compile(r'(?<!\d)(1080|720|480)\s*[pP](?![a-zA-Z0-9])')
_RANGE_RE = re.compile(
    r'(?<![\w.])(?:EP(?:ISODES?)?|E)?\s*[-:.]?\s*(\d{1,4})\s*'
    r'(?:-|–|—|→|~|to|se|TO|SE)\s*'
    r'(?:EP(?:ISODES?)?|E)?\s*[-:.]?\s*(\d{1,4})(?![\w])',
    re.IGNORECASE,
)


def extract_ep_range(*texts):
    """Caption/filename/link-text se episode range nikalta hai.
    '1-12', 'EP 1-12', 'E1-E12', 'ep1 to ep12', '13 se 24' -> (1, 12)
    Range nahi mili to None (poora season ZIP maana jayega)."""
    for t in texts:
        if not t:
            continue
        cleaned = _QUALITY_STRIP_RE.sub(" ", t)
        cleaned = re.sub(r'https?://\S+', " ", cleaned)
        m = _RANGE_RE.search(cleaned)
        if not m:
            continue
        a, b = int(m.group(1)), int(m.group(2))
        if a > b:
            a, b = b, a
        if a == 0 and b == 0:
            continue
        return a, b
    return None


async def save_zip_to_firebase(slug: str, season: str, quality: str, download_link: str,
                               size_text: str = "", ep_range=None) -> bool:
    """ep_range=(start, end) ho to:
         zip_links/{slug}/{season}/{quality}/parts/{start}-{end}
       warna (purana tareeka, poora season):
         zip_links/{slug}/{season}/{quality}
    """
    if not FIREBASE_URL:
        logger.error("FIREBASE_URL set nahi hai, ZIP save nahi ho sakta.")
        return False
    try:
        db_url = FIREBASE_URL.rstrip("/")
        payload = {
            "link"  : download_link,
            "server": SERVER_NAME,
            "time"  : int(time.time()),
        }
        if size_text:
            payload["size"] = size_text
        if ep_range:
            start, end = ep_range
            path = f"zip_links/{slug}/{season}/{quality}/parts/{start}-{end}"
            payload.update({"start": start, "end": end, "label": (f"EP {start}" if start == end else f"EP {start} - {end}")})
        else:
            path = f"zip_links/{slug}/{season}/{quality}"
        async with aiohttp.ClientSession() as session:
            # PATCH use kiya taaki full-season save karne par parts delete na hon
            async with session.patch(f"{db_url}/{path}.json", json=payload) as resp:
                if resp.status == 200:
                    logger.info(f"Firebase ZIP saved: {path}")
                    return True
                text = await resp.text()
                logger.error(f"Firebase ZIP error {resp.status}: {text}")
                return False
    except Exception as e:
        logger.error(f"Firebase ZIP save error: {e}")
        return False


async def handle_zip_upload(msg, context, setup: dict, file_obj, raw_name: str):
    quality = extract_zip_quality(msg.caption or "", raw_name)
    ep_range = extract_ep_range(msg.caption or "", raw_name)
    if not quality:
        await msg.reply_text(
            "⚠️ *ZIP ki quality nahi mili!*\n\n"
            "ZIP dobara bhejo aur caption me quality aur episodes likho, jaise:\n"
            "`720p EP 1-12`  (1080p / 720p / 480p)",
            parse_mode="Markdown",
        )
        return

    processing = await msg.reply_text("⏳ ZIP process ho raha hai...")
    try:
        forwarded = await copy_with_floodwait(context, chat_id=STORAGE_CHANNEL, from_chat_id=msg.chat_id, message_id=msg.message_id)
        storage_msg_id = forwarded.message_id

        range_tag = f"-EP{ep_range[0]}-{ep_range[1]}" if ep_range else ""
        filename = f"{setup['slug']}-{setup['season']}{range_tag}-ZIP-{quality}.zip"
        download_link = make_download_link(storage_msg_id, filename)
        size_text = human_size(file_obj.file_size or 0)

        saved = await save_zip_to_firebase(setup["slug"], setup["season"], quality, download_link, size_text, ep_range)
        await processing.delete()

        status = "✅ Firebase me save ho gaya" if saved else "⚠️ Firebase save fail — link manually daalna padega"
        await msg.reply_text(
            f"📦 *ZIP Secured!*\n\n"
            f"🎌 *Anime:* `{setup['slug']}`\n"
            f"📺 *Season:* `{setup['season']}`\n"
            f"🎞️ *Quality:* `{quality}`\n"
            f"🔢 *Episodes:* `{f'{ep_range[0]} - {ep_range[1]}' if ep_range else 'Full Season (range nahi di)'}`\n"
            f"💾 *Size:* {size_text or 'unknown'}\n"
            f"⬇️ *Download:* `{download_link}`\n\n{status}",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"handle_zip_upload error: {e}")
        try:
            await processing.delete()
        except Exception:
            pass
        await msg.reply_text("❌ ZIP process karte waqt error aaya, dobara try karo.")


# ── DIRECT LINK (R2 etc.) → ZIP SAVE ──────────────────
# Setup ke baad koi direct download link text me bhejo, quality link ke end me
# (ya link ke baad) likhi ho:
#   https://pub-xxxx.r2.dev/aot-s1-720p.zip
#   https://pub-xxxx.r2.dev/aot-s1.zip 720p
# Ek message me kai lines (alag-alag quality) bhi bhej sakte ho.

URL_RE = re.compile(r'https?://\S+')


def extract_link_quality(url: str, extra_text: str = ""):
    # 1) link ke baad/pehle likha hua text  2) link ke andar  3) link ke bilkul end me sirf number (720)
    q = extract_zip_quality(extra_text, url)
    if q:
        return q
    m = re.search(r'(?<!\d)(1080|720|480)[pP]?\s*$', (url + " " + extra_text).strip())
    return f"{m.group(1)}p" if m else None


async def fetch_remote_size(url: str) -> int:
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.head(url, allow_redirects=True) as resp:
                if resp.status == 200:
                    return int(resp.headers.get("Content-Length", "0") or 0)
    except Exception:
        pass
    return 0


async def handle_text_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    msg = update.message
    text = msg.text or ""
    if update.effective_user.id in pending_zip:
        if await handle_zip_text_answer(update, context):
            return
    if not URL_RE.search(text):
        return  # normal text — ignore

    setup = user_setup.get(update.effective_user.id)
    if not setup:
        await msg.reply_text("⚠️ Pehle `/setup <anime-slug> <season>` karo, phir link bhejo.", parse_mode="Markdown")
        return

    done, failed = [], []
    for line in text.splitlines():
        m = URL_RE.search(line)
        if not m:
            continue
        url = m.group(0).strip().rstrip(".,;)")
        extra = (line[:m.start()] + " " + line[m.end():]).strip()
        quality = extract_link_quality(url, extra)
        if not quality:
            failed.append(f"{url} — quality nahi mili")
            continue
        ep_range = extract_ep_range(extra)
        size_text = human_size(await fetch_remote_size(url))
        saved = await save_zip_to_firebase(setup["slug"], setup["season"], quality, url, size_text, ep_range)
        if saved:
            rtxt = f" EP {ep_range[0]}-{ep_range[1]}" if ep_range else " (full)"
            done.append(f"{quality}{rtxt}" + (f" ({size_text})" if size_text else ""))
        else:
            failed.append(f"{quality} — Firebase save fail")
 
    if not done and not failed:
        return
    lines = [f"📦 ZIP link save — {setup['slug']} {setup['season']}"]
    if done:
        lines.append("✅ Saved: " + ", ".join(done))
    if failed:
        lines.append("⚠️ Problem:\n" + "\n".join(failed))
        lines.append("\nFormat: <link> 720p EP 1-12")
    await msg.reply_text("\n".join(lines), disable_web_page_preview=True)
 
 
# ── MULTI-VIDEO → ZIP FEATURE ─────────────────────────
# Flow:
#   /setup <slug> <season> zip      (ya /setup ... phir /zip)
#   videos forward karo  ->  /done
#   Bot sab videos ko ek ZIP me pack karta hai (compression nahi, video pehle se compressed hota hai),
#   STORAGE_CHANNEL me upload karta hai, link banata hai, phir puchta hai:
#   "konsa episode / kaunsi range me save karu?"  ->  Firebase zip_links/... me save.
#
# RAM kam rakhne ke liye video seedha Telegram se stream hoke ZIP me likha jata hai
# (4MB buffer). Disk par sirf ek ZIP part rehta hai, upload ke baad turant delete.
 
ZIP_PART_MB    = int(os.getenv("ZIP_PART_MB", "1900"))     # Telegram bot upload limit ~2000MB; isse chhota rakho
# NOTE: pyrofork har fetch par naya Telegram connection banata hai (+ auth export). Chhote tukde / zyada workers =
# bahut saare naye connection -> Telegram throttle karke atka deta hai. Isliye bade tukde + kam parallel.
ZIP_SEG_CHUNKS = int(os.getenv("ZIP_SEG_CHUNKS", "8"))     # 8 x 1MB per fetch
ZIP_WORKERS    = int(os.getenv("ZIP_WORKERS", "4"))        # kitne tukde ek saath Telegram se aayenge (parallel)
ZIP_TMP_DIR    = os.getenv("ZIP_TMP_DIR", tempfile.gettempdir())
VIDEO_EXTS     = {"mp4", "mkv", "avi", "webm", "mov", "m4v", "ts", "flv"}
 
try:
    from pyrogram import StopTransmission
except Exception:  # purane/alag Pyrogram fork ke liye
    class StopTransmission(Exception):
        pass
 
 
from fastzip import ZipCancelled, ZipProgress, build_zip_part, make_robust_fetch, fmt_dur
 
 
async def run_blocking(fn, *args):
    return await asyncio.get_running_loop().run_in_executor(None, fn, *args)
 
 
from statuskit import StatusMsg  # heartbeat wala, atakne wala nahi
from taskqueue import HeavyQueue, Job
import autorestart


def fmt_ep_range(a: int, b: int) -> str:
    return f"EP {a}" if a == b else f"EP {a}-{b}"
 
 
def is_video_media(msg) -> bool:
    if msg.video:
        return True
    if msg.document:
        mime = (msg.document.mime_type or "").lower()
        name = (msg.document.file_name or "").lower()
        ext = name.rsplit(".", 1)[-1] if "." in name else ""
        return mime.startswith("video/") or ext in VIDEO_EXTS
    return False
 
 
def zip_video_sort_key(v: dict):
    # episode number wale pehle (ep order me), baaki forward hone ke order me
    return (v.get("ep") is None, v.get("ep") or 0, v.get("n", 0))
 
 
def plan_zip_parts(videos: list, limit_bytes: int) -> list:
    parts, cur, cur_size = [], [], 0
    for v in videos:
        size = v.get("size", 0)
        if cur and cur_size + size > limit_bytes:
            parts.append(cur)
            cur, cur_size = [], 0
        cur.append(v)
        cur_size += size
    if cur:
        parts.append(cur)
    return parts
 
 
def part_ep_hint(vids: list):
    eps = [v.get("ep") for v in vids]
    if eps and all(e is not None for e in eps):
        return [min(eps), max(eps)]
    return None
 
 
def zip_arcname(v: dict, setup: dict, used: set) -> str:
    name = (v.get("name") or "").strip()
    if not name:
        ep = v.get("ep")
        tail = f"E{ep}" if ep is not None else f"video{v.get('n', 0)}"
        name = f"{setup['slug']}-{setup['season']}-{tail}.{v.get('ext', 'mp4')}"
    name = re.sub(r'[\\/:*?"<>|]', "_", name)
    if "." in name:
        stem, ext = name.rsplit(".", 1)
        ext = "." + ext
    else:
        stem, ext = name, ""
    final, i = name, 2
    while final in used:
        final = f"{stem}_{i}{ext}"
        i += 1
    used.add(final)
    return final
 
 
def start_zip_mode(uid: int):
    """(ok, message) return karta hai."""
    old = zip_session.get(uid)
    if old and old.get("videos"):
        n = len(old["videos"])
        return False, f"⚠️ ZIP mode pehle se ON hai ({n} video collected).\nZIP banane ke liye /done, hatane ke liye /cancelzip."
    zip_session[uid] = {"videos": [], "status_msg_id": None, "last_edit": 0}
    busy = ""
    if heavy_queue and heavy_queue.user_jobs(uid):
        busy = "\nℹ️ Tumhara pichla ZIP abhi queue/process me hai — ye naya batch uske baad banega.\n"
    return True, (
        "📦 ZIP MODE ON\n" + busy + "\n"
        "Ab saare videos forward karo (480p/720p/1080p jo bhi ho).\n"
        "Sab bhej do to /done likho — main unka ZIP bana ke storage channel me daal dunga.\n"
        "Cancel karna ho to /cancelzip"
    )
 
 
async def zip_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    uid = update.effective_user.id
    if uid not in user_setup:
        await update.message.reply_text("⚠️ Pehle /setup <anime-slug> <season> karo.")
        return
    ok, text = start_zip_mode(uid)
    if ok:
        await save_state_to_firebase()
    await update.message.reply_text(text)
 
 
async def cancel_zip_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    uid = update.effective_user.id
    had = False
    job = zip_jobs.get(uid)
    if job:
        job["cancel"] = True
        had = True
    if heavy_queue and heavy_queue.remove_user(uid):    # line me khade zip bhi hata do
        had = True
    if zip_session.pop(uid, None) is not None:
        had = True
    if pending_zip.pop(uid, None) is not None:
        had = True
    if pending_zip_next.pop(uid, None):
        had = True
    await save_state_to_firebase()
    await update.message.reply_text("🛑 ZIP cancel/clear ho gaya." if had else "ℹ️ Koi zip session active nahi hai.")
 
 
_collect_tasks: dict = {}


async def _collect_status_loop(bot, chat_id: int, uid: int):
    """Ek hi updater per user: bahut saare video ek saath aaye to bhi count/size hamesha LATEST dikhe."""
    try:
        while True:
            sess = zip_session.get(uid)
            if not sess:
                return
            sess["dirty"] = False
            count = len(sess["videos"])
            total = sum(v.get("size", 0) for v in sess["videos"])
            text = f"📥 {count} video collected • {human_size(total)}\nSab bhej do to /done likho."
            try:
                if not sess.get("status_msg_id"):
                    m = await bot.send_message(chat_id, text)
                    sess["status_msg_id"] = m.message_id
                else:
                    await bot.edit_message_text(chat_id=chat_id, message_id=sess["status_msg_id"], text=text)
            except Exception as e:
                if "not modified" not in str(e).lower():
                    logger.warning(f"collect status edit: {e}")
            await asyncio.sleep(1.5)
            if not sess.get("dirty"):
                return
    finally:
        _collect_tasks.pop(uid, None)


async def collect_zip_video(update: Update, context: ContextTypes.DEFAULT_TYPE, file_obj, raw_name: str, ep_num):
    msg = update.message
    uid = update.effective_user.id
    sess = zip_session[uid]
 
    if not is_video_media(msg):
        await msg.reply_text("⚠️ Ye video nahi hai, skip kar diya.")
        return
 
    uniq = file_obj.file_unique_id
    if any(v.get("uniq") == uniq for v in sess["videos"]):
        await msg.reply_text("♻️ Ye video pehle se add hai, skip kiya.")
        return
 
    ext = get_extension(raw_name, fallback="mp4" if msg.video else "mkv")
    sess["videos"].append({
        "fid"  : file_obj.file_id,
        "cid"  : msg.chat_id,        # fresh Message dobara laane ke liye (file_reference expire hone par)
        "mid"  : msg.message_id,
        "uniq" : uniq,
        "name" : raw_name,
        "size" : file_obj.file_size or 0,
        "ep"   : ep_num,
        "q"    : extract_zip_quality(msg.caption or "", raw_name),
        "ext"  : ext,
        "n"    : len(sess["videos"]),
    })
    sess["dirty"] = True
    if uid not in _collect_tasks:
        _collect_tasks[uid] = asyncio.create_task(_collect_status_loop(context.bot, msg.chat_id, uid))
    await save_state_to_firebase()
 
 
async def done_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    uid = update.effective_user.id
    sess = zip_session.get(uid)
    setup = user_setup.get(uid)
    if not setup:
        await update.message.reply_text("⚠️ Pehle /setup <anime-slug> <season> karo.")
        return
    if not sess or not sess.get("videos"):
        await update.message.reply_text("⚠️ Koi video collect nahi hua. Pehle /zip karo, phir videos forward karo.")
        return

    # videos ko job me shift karo -> user turant naya /zip ya koi bhi chhota kaam kar sakta hai
    videos = list(sess["videos"])
    zip_session.pop(uid, None)
    n, total = len(videos), sum(v.get("size", 0) for v in videos)
    label = f"ZIP {setup['slug']} {setup['season']} • {n} video • {human_size(total)}"
    payload = {"chat_id": update.effective_chat.id, "setup": dict(setup), "videos": videos}
    job, ahead = heavy_queue.submit("zip", uid, label, payload)
    await save_state_to_firebase()
    if ahead == 0:
        await update.message.reply_text("🚀 ZIP job shuru ho raha hai...\n(Is dauraan chhote kaam — episode, link — normal chalte rahenge.)")
    else:
        await update.message.reply_text(
            f"⏳ Heavy task queue me hai — number #{ahead} (aage {ahead} job).\n"
            f"{label}\n\nJab number aayega apne aap shuru ho jayega. Queue dekhne ke liye /queue"
        )


async def queue_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    await update.message.reply_text(heavy_queue.describe(update.effective_user.id))


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    lines = ["🤖 BOT STATUS", "━━━━━━━━━━━━━━━━━━━━━━"]
    if handover:
        lines.append(f"⏱ Uptime: {fmt_dur(handover.uptime())}")
        if handover.began:
            lines.append("♻️ Auto-restart (handover) chal raha hai…")
        else:
            lines.append(f"♻️ Agla auto-restart: {fmt_dur(handover.seconds_to_restart())} baad (GitHub limit se pehle)")
    else:
        lines.append("♻️ Auto-restart: band (GitHub Actions par hi chalta hai)")
    lines.append(f"📦 Heavy queue: {len(heavy_queue.running)} chal raha, {len(heavy_queue.queued)} line me")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    await update.message.reply_text("\n".join(lines))


async def restartnow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not ALLOWED_USERS.strip() or not is_allowed(uid):
        return
    if not handover:
        await update.message.reply_text("ℹ️ Auto-restart sirf GitHub Actions par chalta hai.")
        return
    if handover.began:
        await update.message.reply_text("♻️ Handover pehle se chal raha hai.")
        return
    await update.message.reply_text("♻️ Handover shuru: naya run line me lag raha hai, chal raha ZIP job (agar hai) khatam hone dunga.")
    asyncio.create_task(handover.begin("manual /restartnow"))


async def zip_job_handler(job: Job):
    """Queue worker yahan se ZIP job chalata hai."""
    p = job.payload
    uid, chat_id = job.uid, int(p["chat_id"])
    zip_jobs[uid] = {"cancel": False}
    await run_zip_job(bot_app.bot, chat_id, uid, p["setup"], p["videos"])


async def resume_queue():
    """Restart ke baad Firebase se queue wapas line me lagao."""
    if not heavy_queue:
        return
    items = list(restored_queue)
    restored_queue.clear()
    if not items:
        return
    # jo job already queue me hai usko dobara mat daalo
    have = {j["id"] for j in heavy_queue.snapshot()}
    items = [d for d in items if d.get("id") not in have]
    n = heavy_queue.restore(items)
    logger.info(f"Queue restore: {n} job(s)")
    for d in items:
        try:
            await bot_app.bot.send_message(
                int(d["payload"]["chat_id"]),
                f"♻️ Bot restart ke baad tumhara job wapas line me laga diya:\n{d.get('label', '')}",
            )
        except Exception:
            pass


# ── FAST ZIP PIPELINE (progress dashboard: fastzip.py me) ──────────

async def upload_zip_part(job: dict, prog: ZipProgress, p_no: int, zip_path: str, zip_name: str, caption: str) -> int:
    prog.start_upload(p_no, os.path.getsize(zip_path))

    def progress(current, total):   # sync + halka: upload ko kabhi block nahi karta
        if job["cancel"]:
            raise StopTransmission
        prog.set_upload(current, total)

    last = None
    for attempt in range(3):
        try:
            sent = await pyro.send_document(
                chat_id=STORAGE_CHANNEL,
                document=zip_path,
                file_name=zip_name,
                caption=caption,
                force_document=True,
                progress=progress,
            )
            if sent is None:
                raise ZipCancelled()
            return getattr(sent, "id", None) or sent.message_id
        except (ZipCancelled, asyncio.CancelledError):
            raise
        except Exception as e:
            last = e
            if job["cancel"]:
                raise ZipCancelled()
            logger.warning(f"upload attempt {attempt + 1}/3 fail: {e!r}")
            prog.start_upload(p_no, os.path.getsize(zip_path))
            prog.set_note(f"Upload retry {attempt + 1}/3: {type(e).__name__}")
            await asyncio.sleep(3 * (attempt + 1))
    raise RuntimeError(f"Upload fail ho gaya: {last!r}")


def _zip_part_names(parts, setup: dict, quality):
    """Har part ka (zip_name, hint, arcnames) pehle se nikal lo."""
    out = []
    for p_no, vids in enumerate(parts, 1):
        hint = part_ep_hint(vids)
        q_tag = f"-{quality}" if quality else ""
        if hint:
            r_tag = f"-EP{hint[0]}" if hint[0] == hint[1] else f"-EP{hint[0]}-{hint[1]}"
        elif len(parts) > 1:
            r_tag = f"-part{p_no}"
        else:
            r_tag = ""
        zip_name = f"{setup['slug']}-{setup['season']}{r_tag}-ZIP{q_tag}.zip"
        used: set = set()
        arcs = [zip_arcname(v, setup, used) for v in vids]
        out.append((zip_name, hint, arcs))
    return out


async def run_zip_job(bot, chat_id: int, uid: int, setup: dict, job_videos: list):
    job = zip_jobs[uid]
    sess = {"videos": job_videos}
    ok = False
    interrupted = False      # bot restart/shutdown (job queue me persisted rehta hai)
    tmp_dir = None
    sm = None
    upload_task = None
    try:
        videos = sorted(sess["videos"], key=zip_video_sort_key)
        limit = ZIP_PART_MB * 1024 * 1024
        too_big = [v for v in videos if v.get("size", 0) > limit]
        if too_big:
            names = "\n".join(f"• {v.get('name') or v['uniq']} ({human_size(v['size'])})" for v in too_big)
            await bot.send_message(chat_id, f"❌ Ye video ZIP part limit ({ZIP_PART_MB} MB) se bade hain:\n{names}\n\nInhe hata kar dobara /done karo.")
            return

        parts = plan_zip_parts(videos, limit)
        qs = [v.get("q") for v in videos if v.get("q")]
        quality = qs[0] if qs and len(set(qs)) == 1 else None
        names_plan = _zip_part_names(parts, setup, quality)

        status_msg = await bot.send_message(
            chat_id,
            f"📦 {len(videos)} video • {len(parts)} ZIP part banenge\n⏳ Shuru ho raha hai...",
        )
        sm = StatusMsg(bot, chat_id, status_msg.message_id)
        prog = ZipProgress(setup["slug"], setup["season"], sum(v.get("size", 0) for v in videos), len(videos), len(parts))
        tmp_dir = tempfile.mkdtemp(prefix="zipjob_", dir=ZIP_TMP_DIR)
        sm.start(prog.render)   # heartbeat: job se independent, har ~4s message refresh

        # Har video ka fresh Telegram Message (file_reference expire na ho). Na mile to file_id se kaam chalega.
        by_fid = {v["fid"]: v for v in videos}
        src_cache: dict = {}          # fid -> (time, Message)
        src_locks: dict = {}          # fid -> Lock (10 workers ek saath get_messages na karein)

        async def get_src(fid, force=False):
            v = by_fid.get(fid) or {}
            lock = src_locks.setdefault(fid, asyncio.Lock())
            async with lock:
                ent = src_cache.get(fid)
                if ent and not force and time.time() - ent[0] < 900:
                    return ent[1]
                if v.get("cid") and v.get("mid"):
                    try:
                        m = await pyro.get_messages(v["cid"], v["mid"])
                        if m and not m.empty and (m.video or m.document):
                            src_cache[fid] = (time.time(), m)
                            return m
                    except FloodWait as e:
                        await asyncio.sleep(e.value + 1)
                    except Exception as e:
                        logger.warning(f"zip src refresh fail ({v.get('name')}): {e!r}")
                src_cache.pop(fid, None)
                return fid                # fallback: Bot API file_id

        async def raw_fetch(fid, idx, n):
            data = await fetch_segment(await get_src(fid), idx, n, retries=0)
            if not data:                  # khaali aaya -> fresh Message lekar ek baar aur
                data = await fetch_segment(await get_src(fid, force=True), idx, n, retries=0)
            return data
        fetch = make_robust_fetch(raw_fetch, prog)

        results = [None] * len(parts)

        async def upload_and_record(i: int, zip_path: str, zip_name: str, hint, count: int):
            caption = f"📦 {setup['slug']} {setup['season']} ZIP" + (f" • {fmt_ep_range(*hint)}" if hint else "")
            size_bytes = os.path.getsize(zip_path)
            msg_id = await upload_zip_part(job, prog, i + 1, zip_path, zip_name, caption)
            prog.finish_part()
            try:
                os.remove(zip_path)
            except OSError:
                pass
            results[i] = {
                "msg_id": msg_id, "name": zip_name,
                "link": make_download_link(msg_id, zip_name),
                "size_text": human_size(size_bytes), "size_bytes": size_bytes,
                "count": count, "hint": hint,
            }

        def _abort_on_upload_error(t: asyncio.Task):
            if not t.cancelled() and t.exception() is not None:
                job["cancel"] = True     # upload fail -> build bhi turant ruk jaye
                job["internal_abort"] = True

        for i, vids in enumerate(parts):
            zip_name, hint, arcs = names_plan[i]
            zip_path = os.path.join(tmp_dir, zip_name)
            need = sum(v.get("size", 0) for v in vids) + 512 * 1024 * 1024
            if upload_task and shutil.disk_usage(tmp_dir).free < need:
                await upload_task            # disk kam hai -> pehle purana part upload hone do
                upload_task = None

            await build_zip_part(fetch, job, prog, i + 1, vids, arcs, zip_path, ZIP_WORKERS, ZIP_SEG_CHUNKS)

            if upload_task:                  # ek waqt me ek hi upload
                await upload_task
            upload_task = asyncio.create_task(upload_and_record(i, zip_path, zip_name, hint, len(vids)))
            upload_task.add_done_callback(_abort_on_upload_error)

        if upload_task:
            await upload_task
            upload_task = None
        results = [r for r in results if r]

        batch = {
            "slug": setup["slug"], "season": setup["season"],
            "quality": quality, "idx": 0, "parts": results,
        }
        first_batch = uid not in pending_zip
        if first_batch:
            pending_zip[uid] = batch
        else:                                   # pichle ZIP ka jawab baaki hai -> line me rakho
            pending_zip_next.setdefault(uid, []).append(batch)
        ok = True
        await sm.stop(prog.final_text(sum(r["size_bytes"] for r in results)))
        sm = None

        lines = []
        for i, r in enumerate(results, 1):
            lines.append(
                f"📦 *Part {i}/{len(results)}* — {r['count']} video • {r['size_text']}\n"
                f"⬇️ `{r['link']}`"
            )
        await bot.send_message(chat_id, "🎉 *ZIP Secured!*\n\n" + "\n\n".join(lines), parse_mode="Markdown")
        if first_batch:
            await ask_zip_question(bot, chat_id, uid)
        else:
            await bot.send_message(chat_id, "ℹ️ Pehle wale ZIP ka jawab do, uske baad is ZIP ka episode/range puchunga.")

    except asyncio.CancelledError:
        interrupted = True
        raise
    except ZipCancelled:
        # upload task ke asli error ko pakdo (agar build uski wajah se ruka)
        err = None
        if upload_task is not None and upload_task.done() and not upload_task.cancelled():
            err = upload_task.exception()
        if err is not None and not isinstance(err, ZipCancelled):
            logger.error(f"upload error: {err!r}")
            await bot.send_message(chat_id, f"❌ Upload me error aaya:\n{err}\n\nCollected videos safe hain — /done se dobara try kar sakte ho.")
        else:
            await bot.send_message(chat_id, "🛑 ZIP cancel ho gaya.")
    except Exception as e:
        logger.error(f"run_zip_job error: {e}", exc_info=True)
        await bot.send_message(
            chat_id,
            f"❌ ZIP banate waqt error aaya:\n{e}\n\nCollected videos safe hain — /done se dobara try kar sakte ho.",
        )
    finally:
        if upload_task is not None and not upload_task.done():
            upload_task.cancel()
            await asyncio.gather(upload_task, return_exceptions=True)
        if sm is not None:
            try:
                await sm.stop()
            except Exception:
                pass
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        zip_jobs.pop(uid, None)
        if not ok and job_videos and not interrupted:
            # fail: videos wapas collect-session me (agar user ne naya session shuru nahi kiya)
            user_cancelled = job.get("cancel") and not job.get("internal_abort")
            if not user_cancelled and zip_session.get(uid) is None:
                zip_session[uid] = {"videos": list(job_videos), "status_msg_id": None, "last_edit": 0}
        await save_state_to_firebase()


async def ask_zip_question(bot, chat_id: int, uid: int):
    pend = pending_zip.get(uid)
    if not pend:
        return
    if not pend.get("quality"):
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("480p", callback_data="zq:480p"),
            InlineKeyboardButton("720p", callback_data="zq:720p"),
            InlineKeyboardButton("1080p", callback_data="zq:1080p"),
        ]])
        await bot.send_message(chat_id, "🎞️ Is ZIP ki quality konsi hai?", reply_markup=kb)
        return
 
    n = len(pend["parts"])
    part = pend["parts"][pend["idx"]]
    hint = part.get("hint")
    rows = []
    if hint:
        rows.append([InlineKeyboardButton(f"✅ {fmt_ep_range(*hint)} (detected)", callback_data="zr:use")])
    if n == 1:
        rows.append([InlineKeyboardButton("📚 Full Season", callback_data="zr:full")])
    rows.append([InlineKeyboardButton("⏭️ Skip (save mat karo)", callback_data="zr:skip")])
 
    head = f"Part {pend['idx'] + 1}/{n}" if n > 1 else "Ye ZIP"
    await bot.send_message(
        chat_id,
        f"❓ {head} ({part['count']} video, {part['size_text']}, {pend['quality']}) konsa episode / range me save karu?\n\n"
        f"✍️ Type karo:\n"
        f"• Single episode: 5\n"
        f"• Range: 1-12  (ya 13 se 24)\n"
        f"• Poora season: full" + ("  (multi-part me allowed nahi)" if n > 1 else ""),
        reply_markup=InlineKeyboardMarkup(rows),
    )
 
 
async def commit_zip_part(bot, chat_id: int, uid: int, ep_range=None, skip: bool = False):
    pend = pending_zip.get(uid)
    if not pend:
        return
    part = pend["parts"][pend["idx"]]
    n = len(pend["parts"])
    label = f"Part {pend['idx'] + 1}/{n}" if n > 1 else "ZIP"
 
    if skip:
        await bot.send_message(chat_id, f"⏭️ {label} Firebase me save nahi kiya.")
    else:
        saved = await save_zip_to_firebase(
            pend["slug"], pend["season"], pend["quality"], part["link"], part["size_text"],
            tuple(ep_range) if ep_range else None,
        )
        rtxt = fmt_ep_range(*ep_range) if ep_range else "Full Season"
        status = "✅ Firebase me save ho gaya" if saved else "⚠️ Firebase save fail — link manually daalna padega"
        await bot.send_message(
            chat_id,
            f"📦 {label} saved\n🎌 {pend['slug']} {pend['season']} • {pend['quality']}\n🔢 {rtxt}\n{status}",
        )
 
    pend["idx"] += 1
    if pend["idx"] >= n:
        pending_zip.pop(uid, None)
        nxt = pending_zip_next.get(uid) or []
        if nxt:
            pending_zip[uid] = nxt.pop(0)
            if not nxt:
                pending_zip_next.pop(uid, None)
            await save_state_to_firebase()
            await bot.send_message(chat_id, "✅ Ye ZIP ho gaya. Ab queue wale agle ZIP ka sawal:")
            await ask_zip_question(bot, chat_id, uid)
            return
        await save_state_to_firebase()
        await bot.send_message(chat_id, "🏁 Sab ZIP process ho gaye. Naya batch karna ho to /zip likho.")
    else:
        await save_state_to_firebase()
        await ask_zip_question(bot, chat_id, uid)
 
 
async def zip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = q.from_user.id
    if not is_allowed(uid):
        await q.answer()
        return
    pend = pending_zip.get(uid)
    if not pend:
        await q.answer("Koi pending zip nahi hai.", show_alert=True)
        return
    await q.answer()
    try:
        await q.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
 
    chat_id = q.message.chat_id
    data = q.data or ""
    if data.startswith("zq:"):
        pend["quality"] = data[3:]
        await save_state_to_firebase()
        await ask_zip_question(context.bot, chat_id, uid)
        return
    if not pend.get("quality"):
        await ask_zip_question(context.bot, chat_id, uid)
        return
 
    part = pend["parts"][pend["idx"]]
    if data == "zr:use" and part.get("hint"):
        await commit_zip_part(context.bot, chat_id, uid, ep_range=part["hint"])
    elif data == "zr:full" and len(pend["parts"]) == 1:
        await commit_zip_part(context.bot, chat_id, uid, ep_range=None)
    elif data == "zr:skip":
        await commit_zip_part(context.bot, chat_id, uid, skip=True)
 
 
_SINGLE_EP_RE = re.compile(r'(?:ep(?:isode)?|e)?\s*[-:.]?\s*(\d{1,4})')
 
 
async def handle_zip_text_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Pending ZIP ka text jawab (quality / episode / range). True = message consume ho gaya."""
    uid = update.effective_user.id
    pend = pending_zip.get(uid)
    if not pend:
        return False
    msg = update.message
    text = (msg.text or "").strip()
    if URL_RE.search(text):
        return False  # direct-link flow ko chhodo
 
    low = text.lower()
    q = extract_zip_quality(text)
    if not q and not pend.get("quality"):
        m = re.fullmatch(r'(1080|720|480)p?', low)
        if m:
            q = f"{m.group(1)}p"
    if q:
        pend["quality"] = q
        await save_state_to_firebase()
 
    cleaned = _QUALITY_STRIP_RE.sub(" ", text).strip()
    low = cleaned.lower()
 
    if not pend.get("quality"):
        await ask_zip_question(context.bot, msg.chat_id, uid)
        return True
    if not low:
        await ask_zip_question(context.bot, msg.chat_id, uid)
        return True
 
    if low in ("skip", "cancel"):
        await commit_zip_part(context.bot, msg.chat_id, uid, skip=True)
        return True
    if low in ("full", "all", "season", "full season", "poora", "pura"):
        if len(pend["parts"]) > 1:
            await msg.reply_text("⚠️ Multi-part ZIP me 'full' allowed nahi, har part ki range batao (jaise 1-12).")
            return True
        await commit_zip_part(context.bot, msg.chat_id, uid, ep_range=None)
        return True
 
    rng = extract_ep_range(cleaned)
    if rng:
        await commit_zip_part(context.bot, msg.chat_id, uid, ep_range=list(rng))
        return True
    m = _SINGLE_EP_RE.fullmatch(low)
    if m:
        ep = int(m.group(1))
        await commit_zip_part(context.bot, msg.chat_id, uid, ep_range=[ep, ep])
        return True
 
    await msg.reply_text("⚠️ Samajh nahi aaya. Aise likho: 5   ya   1-12   ya   full   ya   skip")
    return True
 
 
# ── BOT HANDLERS ──────────────────────────────────────
 
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        await update.message.reply_text("⛔ Unauthorized.")
        return
    await update.message.reply_text(
        "👋 *AnimeVerse Storage Bot*\n\n"
        "📌 *Setup karo:*\n`/setup <anime-slug> <season>`\n"
        "_Example: /setup attack-on-titan 1_\n\n"
        "Phir video forward karo — caption mein episode number hona chahiye.\n"
        "Bot automatically filename aur secured links banayega! 🚀\n\n"
        "📦 *Multi-video ZIP mode:*\n"
        "`/setup <anime-slug> <season> zip` → videos forward karo → `/done`\n"
        "Bot ZIP bana ke storage me daalega, link dega aur puchega kis episode/range me save karna hai.\n"
        "Cancel: `/cancelzip`   •   Queue dekho: `/queue`\n\n"
        "⚡ ZIP bante waqt bhi episode add / link generate normal chalta hai — ZIP heavy queue me chalta hai.",
        parse_mode="Markdown",
    )
 
 
async def setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("❌ *Usage:* `/setup <anime-slug> <season-number> [zip]`\n\n", parse_mode="Markdown")
        return
    slug = args[0].lower().strip()
    raw_season = args[1].strip()
    if raw_season.isdigit():
        season = f"S{raw_season}"
    else:
        season = raw_season.upper()
 
    uid = update.effective_user.id
    user_setup[uid] = {"slug": slug, "season": season}
    zip_session.pop(uid, None)  # naya setup = purana adhura collect hata do (queue wale jobs ka apna setup hai)
    zip_text = ""
    if len(args) >= 3 and args[2].lower() in ("zip", "zipmode"):
        _, zip_text = start_zip_mode(uid)
    await save_state_to_firebase()
    if zip_text:
        await update.message.reply_text(
            f"✅ Setup Saved for AnimeVerse!\n\n🎌 Anime Slug: {slug}\n📺 Season: {season}\n\n{zip_text}"
        )
    else:
        await update.message.reply_text(
            f"✅ *Setup Saved for AnimeVerse!*\n\n🎌 *Anime Slug:* `{slug}`\n📺 *Season:* `{season}`",
            parse_mode="Markdown",
        )
 
 
async def clear_setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    uid = update.effective_user.id
    user_setup.pop(uid, None)
    quality_buffer.pop(uid, None)
    zip_session.pop(uid, None)
    pending_zip.pop(uid, None)
    pending_zip_next.pop(uid, None)
    await save_state_to_firebase()
    await update.message.reply_text("🗑️ Setup clear ho gaya.", parse_mode="Markdown")
 
 
async def current_setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    setup = user_setup.get(update.effective_user.id)
    if not setup:
        await update.message.reply_text("⚠️ Koi setup nahi hai.", parse_mode="Markdown")
        return
    await update.message.reply_text(f"📋 *Current Setup:*\n🎌 *Anime:* `{setup['slug']}`\n📺 *Season:* `{setup['season']}`", parse_mode="Markdown")
 
 
 
 
# ── VIDEO DETAILS (sirf info batata hai, kuch change/convert nahi karta) ──
VIDEO_INFO = os.getenv("VIDEO_INFO", "1") != "0"   # 0 = details band
_probe_lock = asyncio.Semaphore(1)                 # ek time pe ek hi ffprobe
 
CODEC_NAMES = {
    "h264": "H.264", "hevc": "H.265 (HEVC)", "av1": "AV1", "vp9": "VP9", "vp8": "VP8",
    "mpeg4": "MPEG-4", "mpeg2video": "MPEG-2", "vc1": "VC-1",
    "aac": "AAC", "mp3": "MP3", "ac3": "AC3", "eac3": "E-AC3", "dts": "DTS",
    "flac": "FLAC", "opus": "Opus", "vorbis": "Vorbis", "truehd": "TrueHD",
    "ass": "ASS", "subrip": "SRT", "webvtt": "VTT", "mov_text": "mov_text", "hdmv_pgs_subtitle": "PGS",
}
 
 
def _md(text) -> str:
    """Markdown tootne wale characters hata do."""
    return re.sub(r"[_*`\[\]]", " ", str(text))
 
 
def _mmss(sec: float) -> str:
    sec = max(0, int(sec))
    h_, rem = divmod(sec, 3600)
    m, s_ = divmod(rem, 60)
    return f"{h_}:{m:02d}:{s_:02d}" if h_ else f"{m:02d}:{s_:02d}"
 
 
def _channels(n: int) -> str:
    return {1: "mono", 2: "stereo", 6: "5.1", 8: "7.1"}.get(n, f"{n}ch" if n else "")
 
 
async def get_video_details(storage_msg_id: int, ext: str):
    """Storage mein padi video ki details ffprobe se nikalta hai (poori file download nahi hoti).
    Kuch bhi fail ho to None return karta hai — bot ka baaki kaam nahi rukta."""
    if not VIDEO_INFO or not shutil.which("ffprobe"):
        return None
    probe_name = f"probe.{ext}"
    code = generate_code(storage_msg_id, probe_name)
    url = f"http://127.0.0.1:{PORT}/animeverse/dl/{storage_msg_id}/{urllib.parse.quote(probe_name)}?code={code}"
    try:
        async with _probe_lock:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "error", "-probesize", "8M", "-analyzeduration", "8M",
                "-print_format", "json", "-show_format", "-show_streams", url,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            try:
                out, err = await asyncio.wait_for(proc.communicate(), 60)
            except asyncio.TimeoutError:
                proc.kill()
                logger.error(f"ffprobe timeout for msg {storage_msg_id}")
                return None
        if proc.returncode != 0:
            logger.error(f"ffprobe failed for msg {storage_msg_id}: {err.decode(errors='ignore')[-200:]}")
            return None
        info = json.loads(out.decode() or "{}")
    except Exception as e:
        logger.error(f"get_video_details error: {e}")
        return None
 
    streams = info.get("streams") or []
    fmt = info.get("format") or {}
    vs = [x for x in streams if x.get("codec_type") == "video" and not (x.get("disposition") or {}).get("attached_pic")]
    aus = [x for x in streams if x.get("codec_type") == "audio"]
    subs = [x for x in streams if x.get("codec_type") == "subtitle"]
    if not vs:
        return None
    v = vs[0]
 
    names = (fmt.get("format_name") or "").split(",")
    container = "MP4" if "mp4" in names else ("MKV / WebM" if "matroska" in names else (names[0].upper() or "?"))
    try:
        duration = float(fmt.get("duration") or v.get("duration") or 0)
    except ValueError:
        duration = 0.0
    try:
        bitrate = int(fmt.get("bit_rate") or 0)
    except ValueError:
        bitrate = 0
    fps = 0.0
    try:
        n, d = (v.get("r_frame_rate") or "0/1").split("/")
        fps = float(n) / float(d) if float(d) else 0.0
    except Exception:
        pass
 
    vcodec = (v.get("codec_name") or "").lower()
    pix = (v.get("pix_fmt") or "").lower()
    m = re.search(r"p(\d{2})(?:le|be)", pix)
    depth = int(m.group(1)) if m else int(v.get("bits_per_raw_sample") or 8)
 
    audios = []
    for a in aus:
        audios.append({
            "codec": (a.get("codec_name") or "").lower(),
            "ch": int(a.get("channels") or 0),
            "lang": (a.get("tags") or {}).get("language", ""),
        })
 
    # Browser mein chalegi ya nahi
    issues = []
    if not (vcodec == "h264" and pix in ("yuv420p", "yuvj420p")):
        issues.append(f"video {CODEC_NAMES.get(vcodec, vcodec.upper())}" + (f" {depth}-bit" if depth > 8 else ""))
    if audios and audios[0]["codec"] not in ("aac", "mp3"):
        issues.append(f"audio {CODEC_NAMES.get(audios[0]['codec'], audios[0]['codec'].upper())}")
    if "mp4" not in names:
        issues.append(f"container {container}")
 
    return {
        "ext": ext, "container": container, "duration": duration, "bitrate": bitrate,
        "width": int(v.get("width") or 0), "height": int(v.get("height") or 0), "fps": fps,
        "vcodec": vcodec, "vprofile": v.get("profile") or "", "depth": depth,
        "audios": audios, "subs": [{"codec": (x.get("codec_name") or "").lower(), "lang": (x.get("tags") or {}).get("language", "")} for x in subs],
        "issues": issues,
    }
 
 
def format_details(d: dict, header: str = "📋 *Video Details*") -> str:
    """Poori details (Markdown). header=None ho to heading nahi lagti (report ke liye)."""
    if not d:
        return ""
    vname = CODEC_NAMES.get(d["vcodec"], d["vcodec"].upper())
    L = [header] if header else []
    L.append(f"📄 Extension: `.{_md(d['ext'])}`")
    L.append(f"📦 Container: {_md(d['container'])}")
    if d["duration"]:
        L.append(f"⏱ Duration: {_mmss(d['duration'])}")
    if d["width"]:
        L.append(f"📐 Resolution: {d['width']}x{d['height']}" + (f" @ {d['fps']:.2f} fps" if d["fps"] else ""))
    vline = vname + (f" · {_md(d['vprofile'])}" if d["vprofile"] else "") + (f" · {d['depth']}-bit" if d["depth"] > 8 else "")
    L.append(f"🎞 Video: {vline}")
    if d["audios"]:
        L.append(f"🔊 Audio ({len(d['audios'])} track):")
        for i, a in enumerate(d["audios"], 1):
            parts = [CODEC_NAMES.get(a["codec"], a["codec"].upper()), _channels(a["ch"]), _md(a["lang"])]
            L.append(f"   {i}. " + " · ".join(x for x in parts if x))
    else:
        L.append("🔊 Audio: nahi hai")
    if d["subs"]:
        kinds = ", ".join(CODEC_NAMES.get(x["codec"], x["codec"].upper()) for x in d["subs"][:6])
        L.append(f"💬 Subtitles ({len(d['subs'])}): {kinds}")
    if d["bitrate"]:
        L.append(f"📊 Bitrate: {d['bitrate'] / 1_000_000:.1f} Mbps")
    if d["issues"]:
        L.append("⚠️ Browser mein shayad na chale: " + ", ".join(_md(x) for x in d["issues"]))
    else:
        L.append("✅ Browser mein chalegi (H.264 + AAC + MP4)")
    return "\n".join(L)
 
 
async def send_episode_report(msg, setup: dict, ep_num, items: list):
    """3 quality ke links dene ke BAAD ek report: teeno files ki details ek message mein."""
    if not VIDEO_INFO or not shutil.which("ffprobe"):
        return
    wait = None
    try:
        wait = await msg.reply_text("📊 Teeno files ki report taiyar ho rahi hai...")
        blocks, marks, got_any = [], [], False
        for it in sorted(items, key=lambda x: quality_sort_key(x["quality"]), reverse=True):
            d = await get_video_details(it["sid"], it["ext"])
            head = f"🎬 *{it['quality']}* — {it['size_mb']} MB"
            if d:
                got_any = True
                blocks.append(head + "\n" + format_details(d, header=None))
                marks.append(f"{it['quality']} {'⚠️' if d['issues'] else '✅'}")
            else:
                blocks.append(head + "\n⚠️ Details nahi mil saki")
                marks.append(f"{it['quality']} ❔")
        if not got_any:
            await wait.delete()
            return
        text = (f"📊 *Episode Report*\n🎌 `{setup['slug']}` · Season {setup['season']} · Episode {ep_num}\n\n"
                + "\n\n".join(blocks)
                + "\n\n📌 *Summary:* " + "  |  ".join(marks)
                + "\n(✅ browser mein chalegi, ⚠️ shayad na chale)")
        if len(text) <= 4000:
            await wait.edit_text(text, parse_mode="Markdown")
        else:
            await wait.delete()
            for i in range(0, len(text), 3900):
                await msg.reply_text(text[i:i + 3900], parse_mode="Markdown")
    except Exception as e:
        logger.error(f"send_episode_report error: {e}")
        if wait:
            try:
                await wait.delete()
            except Exception:
                pass
 
 
async def send_details(msg, storage_msg_id: int, ext: str):
    """Links dene ke BAAD alag message mein details bhejta hai (links kabhi ruk'te nahi)."""
    try:
        text = format_details(await get_video_details(storage_msg_id, ext))
        if text:
            await msg.reply_text(text, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"send_details error: {e}")
 
 
async def _handle_episode_media(update, context, setup: dict, file_obj, raw_name: str, ep_num: int):
    msg = update.message
    uid = update.effective_user.id
    ext = get_extension(raw_name, fallback="mp4" if (msg.video or msg.video_note) else "mkv")
    file_size = file_obj.file_size or 0
    processing = await msg.reply_text("⏳ Processing for AnimeVerse...")
    try:
        forwarded = await copy_with_floodwait(context, chat_id=STORAGE_CHANNEL, from_chat_id=msg.chat_id, message_id=msg.message_id)
        storage_msg_id = forwarded.message_id
 
        if uid not in quality_buffer:
            quality_buffer[uid] = {}
        if ep_num not in quality_buffer[uid]:
            quality_buffer[uid][ep_num] = []
 
        quality_buffer[uid][ep_num].append({"size": file_size, "sid": storage_msg_id, "ext": ext})
        await save_state_to_firebase()
        collected = len(quality_buffer[uid][ep_num])
        await processing.delete()
 
        if collected < QUALITY_COUNT:
            await msg.reply_text(f"✅ *Video {collected}/{QUALITY_COUNT} mila!*", parse_mode="Markdown")
            return
 
        videos = quality_buffer[uid][ep_num]
        quality_map = assign_qualities(videos)
        results = []
 
        for i, vid in enumerate(videos):
            quality = quality_map[i]
            filename = f"{setup['slug']}-{setup['season']}-E{ep_num}-{quality}.{vid['ext']}"
            stream_link = make_stream_link(vid["sid"], filename)
            download_link = make_download_link(vid["sid"], filename)
            embed_link = make_embed_link(vid["sid"], filename)
 
            fb_saved = await save_to_firebase_with_retry(setup["slug"], setup["season"], ep_num, stream_link, quality, download_link)
            results.append({"quality": quality, "sid": vid["sid"], "ext": vid["ext"], "link": stream_link, "dl_link": download_link, "embed_link": embed_link, "size_mb": round(vid["size"] / (1024*1024), 2), "saved": fb_saved})
 
        del quality_buffer[uid][ep_num]
        await save_state_to_firebase()
 
        quality_lines = "\n".join([
            f"  *{r['quality']}* — {r['size_mb']} MB\n  ▶️ Stream: `{r['link']}`\n  🖼️ Embed: `{r['embed_link']}`\n  ⬇️ Download: `{r['dl_link']}`"
            for r in sorted(results, key=lambda x: quality_sort_key(x["quality"]), reverse=True)
        ])
        await msg.reply_text(f"🎉 *Teeno Quality Secured & Saved!*\n\n🎌 *Anime:* `{setup['slug']}`\n{quality_lines}", parse_mode="Markdown")
        await send_episode_report(msg, setup, ep_num, results)  # teeno ki details ek report mein
    except Exception as e:
        logger.error(f"handle_media error: {e}")
        try:
            await processing.delete()
        except Exception:
            pass
        await msg.reply_text("❌ Process karte waqt error aaya, dobara try karo.")


async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    msg = update.message
    uid = update.effective_user.id
    file_obj = None
    raw_name = ""
 
    if msg.video:
        file_obj = msg.video
        raw_name = msg.video.file_name or ""
    elif msg.document:
        file_obj = msg.document
        raw_name = msg.document.file_name or ""
    elif msg.audio:
        file_obj = msg.audio
        raw_name = msg.audio.file_name or ""
    elif msg.video_note:
        file_obj = msg.video_note
        raw_name = ""
    else:
        return
 
    caption_text = msg.caption or ""
    ep_num = extract_episode(caption_text)
    if ep_num is None and raw_name:
        ep_num = extract_episode(raw_name)
 
    setup = user_setup.get(uid)
    if setup:
        # ZIP MODE ON: videos sirf collect honge, /done par zip banega
        if uid in zip_session:
            await collect_zip_video(update, context, file_obj, raw_name, ep_num)
            return
        # ZIP file (poora season) — episode flow se alag handle hota hai
        if is_zip_file(msg, raw_name):
            await handle_zip_upload(msg, context, setup, file_obj, raw_name)
            return
        if ep_num is None:
            await msg.reply_text("⚠️ *Episode number nahi mila!*", parse_mode="Markdown")
            return
        async with user_lock(uid):      # ek user ke episode/quality collect serial (concurrent updates me race na ho)
            await _handle_episode_media(update, context, setup, file_obj, raw_name, ep_num)
    else:
        filename = raw_name or f"video_{file_obj.file_unique_id}.mp4"
        processing = await msg.reply_text("⏳ Processing...")
        try:
            forwarded = await copy_with_floodwait(context, chat_id=STORAGE_CHANNEL, from_chat_id=msg.chat_id, message_id=msg.message_id)
            storage_msg_id = forwarded.message_id
            stream_link = make_stream_link(storage_msg_id, filename)
            download_link = make_download_link(storage_msg_id, filename)
            embed_link = make_embed_link(storage_msg_id, filename)
            await processing.delete()
            await msg.reply_text(
                f"▶️ *Stream Link:*\n`{stream_link}`\n\n🖼️ *Embed Link:*\n`{embed_link}`\n\n⬇️ *Download Link:*\n`{download_link}`",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Stream", url=stream_link)],
                    [InlineKeyboardButton("🖼️ Embed", url=embed_link)],
                    [InlineKeyboardButton("⬇️ Download", url=download_link)],
                ]),
            )
            await send_details(msg, storage_msg_id, get_extension(filename, fallback="mp4"))
        except Exception as e:
            logger.error(f"handle_media error: {e}")
            try:
                await processing.delete()
            except Exception:
                pass
            await msg.reply_text("❌ Process karte waqt error aaya, dobara try karo.")
 
 
async def get_link_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    args = context.args
    if len(args) < 2:
        return
    try:
        msg_id = int(args[0])
        filename = " ".join(args[1:])
        link = make_stream_link(msg_id, filename)
        embed_link = make_embed_link(msg_id, filename)
        dl_link = make_download_link(msg_id, filename)
        await update.message.reply_text(f"🔗 *Secured Links:*\n`{link}`\n`{embed_link}`\n`{dl_link}`", parse_mode="Markdown")
    except ValueError:
        pass
 
 
# ── FASTAPI SERVER WITH SINGLE CLIENT FIX ─────────────────────────────
 
@asynccontextmanager
async def lifespan(app: FastAPI):
    # App startup par global pyro client ko ek hi baar start karega
    await pyro.start()
    logger.info("Single Pyrogram Client successfully started!")
    try:
        if STORAGE_CHANNEL:
            chat_info = await pyro.get_chat(STORAGE_CHANNEL)
            logger.info(f"Connected and Cached Storage Channel: {chat_info.title}")
    except Exception as e:
        logger.error(f"Startup peer channel cache error: {e}")
    await load_state_from_firebase()
    await publish_origin()
    if heavy_queue:
        heavy_queue.start()
        await resume_queue()
    yield
    await pyro.stop()
 
 
web_app = FastAPI(title="AnimeVerse TG Stream Server", lifespan=lifespan)
 
web_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["Range", "Content-Type", "Authorization"],
)
 
 
@web_app.get("/")
async def index():
    return HTMLResponse("<html><body style='font-family:sans-serif;text-align:center;padding:80px;background:#000;color:#fff'><h1>🎬 AnimeVerse Stream Server</h1><p>Active & Secured 🔒</p></body></html>")
 
 
# BRANDED ROUTE 1: WATCH PLAYER
@web_app.get("/animeverse/watch/{msg_id}/{filename:path}")
async def watch_file(msg_id: int, filename: str, code: str):
    decoded = urllib.parse.unquote(filename)
    if not verify_code(msg_id, decoded, code):
        raise HTTPException(status_code=403, detail="Invalid Link or Brand verification failed.")
 
    stream_url = f"/animeverse/dl/{msg_id}/{urllib.parse.quote(decoded)}?code={code}"
    safe_title = html_escape(decoded, quote=True)
 
    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>{safe_title}</title>
<style>body {{ background:#000; display:flex; justify-content:center; align-items:center; min-height:100vh; color:#fff }} video {{ width:100%; max-width:960px }}</style>
</head>
<body><video src="{stream_url}" controls playsinline></video></body>
</html>"""
    return HTMLResponse(html)
 
 
# ── PARALLEL STREAM SETTINGS ──────────────────────────
SEG_CHUNKS = int(os.getenv("SEG_CHUNKS", "2"))   # ek fetch task = 2 x 1MB (0.15GB RAM plan ke liye)
PREFETCH   = int(os.getenv("PREFETCH", "2"))     # kitne segments ek saath Telegram se aayenge
# RAM use ~ SEG_CHUNKS * PREFETCH MB per viewer (default ~12MB). Low RAM ho to PREFETCH=2 karo.
 
 
async def fetch_segment(message, seg_offset: int, seg_limit: int, retries: int = 2) -> bytes:
    """Telegram se seg_limit chunks (1MB each) lata hai, fail hone par retry."""
    last = None
    for attempt in range(retries + 1):
        try:
            parts = []
            async for c in pyro.stream_media(message, offset=seg_offset, limit=seg_limit):
                parts.append(c)
            return b"".join(parts)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            last = e
        except Exception as e:
            last = e
            await asyncio.sleep(0.3 * (attempt + 1))
    raise last
 
 
# BRANDED ROUTE 2: STREAM / DOWNLOAD (FIXED EXTRACTION)
@web_app.api_route("/animeverse/dl/{msg_id}/{filename:path}", methods=["GET", "HEAD"])
async def stream_file(msg_id: int, filename: str, code: str, request: Request, dl: int = 0):
    decoded = urllib.parse.unquote(filename)
 
    if not verify_code(msg_id, decoded, code):
        raise HTTPException(status_code=403, detail="Access Denied: Link manipulation detected.")
 
    cached = message_cache.get(msg_id)
    if cached and time.time() - cached[0] > MESSAGE_CACHE_TTL:
        message_cache.pop(msg_id, None)
        cached = None
 
    if cached:
        message = cached[1]
        message_cache.move_to_end(msg_id)  # LRU: recently used ko end pe rakho
    else:
        try:
            # Main pyro server instance se call ho raha hai bina kisi peer loss ke
            message = await pyro.get_messages(STORAGE_CHANNEL, msg_id)
            if not message or message.empty:
                raise ValueError("Empty message from Telegram")
            message_cache[msg_id] = (time.time(), message)
            if len(message_cache) > MESSAGE_CACHE_LIMIT:
                message_cache.popitem(last=False)  # sabse purana entry hata do (RAM bounded rakhne ke liye)
        except Exception as e:
            logger.error(f"Media extraction crash for msg_id {msg_id}: {e}")
            raise HTTPException(status_code=404, detail="Media extraction failed from Telegram Storage.")
 
    media = message.video or message.document or message.audio or message.video_note
    if not media:
        raise HTTPException(status_code=404, detail="No media found in the specified storage ID.")
        
    file_size = media.file_size
    mime_type = getattr(media, "mime_type", None)
    if not mime_type or mime_type == "application/octet-stream":
        mime_type = mimetypes.guess_type(decoded)[0] or "video/mp4"
    CHUNK_SIZE = 1 * 1024 * 1024
 
    # ---- Range parsing (suffix range "bytes=-N" bhi supported) ----
    range_header = request.headers.get("range")
    start = 0
    end = file_size - 1
    if range_header:
        try:
            spec = range_header.replace("bytes=", "").split(",")[0].strip()
            a, b = spec.split("-", 1)
            if a == "":                      # bytes=-N  -> last N bytes
                n = int(b)
                start = max(file_size - n, 0)
            else:
                start = int(a)
                if b != "":
                    end = min(int(b), file_size - 1)
        except Exception:
            raise HTTPException(status_code=416, detail="Bad Range header")
        if start >= file_size or start > end:
            raise HTTPException(status_code=416, detail="Range not satisfiable",
                                headers={"Content-Range": f"bytes */{file_size}"})
 
    content_length = end - start + 1
    offset = start // CHUNK_SIZE
    first_chunk_cut = start % CHUNK_SIZE
    limit = math.ceil((content_length + first_chunk_cut) / CHUNK_SIZE)
 
    download_filename = f"[AnimeVerse] {decoded}"
    safe_filename = urllib.parse.quote(download_filename)
 
    response_headers = {
        "Content-Type": mime_type,
        "Accept-Ranges": "bytes",
        "Content-Disposition": f"{'attachment' if dl else 'inline'}; filename*=UTF-8''{safe_filename}",
        "Content-Length": str(content_length),
        "Cache-Control": "public, max-age=3600",
    }
    if range_header:
        response_headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
 
    if request.method == "HEAD":
        return Response(status_code=206 if range_header else 200, headers=response_headers)
 
    # ---- Parallel read-ahead plan ----
    # Pehla segment sirf 1 chunk (jaldi pehla byte), baaki SEG_CHUNKS ke segments.
    segs = []
    i = 0
    while i < limit:
        n = 1 if i == 0 else min(SEG_CHUNKS, limit - i)
        segs.append((offset + i, n))
        i += n
 
    async def generator():
        tasks = []
        next_i = 0
        bytes_sent = 0
        first = True
        try:
            while next_i < len(segs) and len(tasks) < PREFETCH:
                tasks.append(asyncio.create_task(fetch_segment(message, *segs[next_i])))
                next_i += 1
            while tasks:
                data = await tasks.pop(0)
                if next_i < len(segs):
                    tasks.append(asyncio.create_task(fetch_segment(message, *segs[next_i])))
                    next_i += 1
                if first:
                    data = data[first_chunk_cut:]
                    first = False
                remaining = content_length - bytes_sent
                if remaining <= 0:
                    break
                if len(data) > remaining:
                    data = data[:remaining]
                if not data:
                    break
                yield data
                bytes_sent += len(data)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Stream error msg_id {msg_id}: {e}")
        finally:
            for t in tasks:
                t.cancel()
 
    return StreamingResponse(generator(), status_code=206 if range_header else 200, headers=response_headers)
 
 
# ── MAIN RUNNER ───────────────────────────────────────
 
async def run_bot():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connection_pool_size(32)          # default 1 tha -> parallel edits PoolTimeout se fail
        .pool_timeout(15)
        .connect_timeout(15)
        .read_timeout(30)
        .write_timeout(30)
        .get_updates_connection_pool_size(4)
        .get_updates_read_timeout(30)
        .concurrent_updates(True)          # ek handler (heavy/slow) baaki sabko block na kare
        .build()
    )
    # restart ke gap me aaye messages ko drop mat karo (DROP_PENDING_UPDATES=1 se purana behaviour)
    await app.bot.delete_webhook(drop_pending_updates=os.getenv("DROP_PENDING_UPDATES", "0") == "1")
    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("setup",       setup_cmd))
    app.add_handler(CommandHandler("mysetup",     current_setup_cmd))
    app.add_handler(CommandHandler("clearsetup",  clear_setup_cmd))
    app.add_handler(CommandHandler("zip",         zip_cmd))
    app.add_handler(CommandHandler("done",        done_cmd))
    app.add_handler(CommandHandler("cancelzip",   cancel_zip_cmd))
    app.add_handler(CommandHandler("queue",       queue_cmd))
    app.add_handler(CommandHandler("status",      status_cmd))
    app.add_handler(CommandHandler("restartnow",  restartnow_cmd))
    app.add_handler(CallbackQueryHandler(zip_callback, pattern=r"^z[qr]:"))
    app.add_handler(CommandHandler("getlink",     get_link_cmd))
    app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL | filters.AUDIO | filters.VIDEO_NOTE, handle_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_link))
    global bot_app, heavy_queue
    bot_app = app
    heavy_queue = HeavyQueue(workers=int(os.getenv("HEAVY_WORKERS", "1")), on_change=save_state_to_firebase)
    heavy_queue.register("zip", zip_job_handler)
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    return app
 
 
async def run_server():
    global uv_server
    config = uvicorn.Config(web_app, host="0.0.0.0", port=PORT, log_level="info")
    uv_server = uvicorn.Server(config)
    await uv_server.serve()


def _chat_for_uid(uid: int) -> int:
    for d in heavy_queue.snapshot():
        if int(d["uid"]) == uid and d.get("payload", {}).get("chat_id"):
            return int(d["payload"]["chat_id"])
    return uid


async def _handover_stop():
    """Restart ke liye: queue snapshot freeze -> running job cancel -> server band."""
    heavy_queue.freeze()
    await save_state_to_firebase()
    await heavy_queue.shutdown()
    if uv_server:
        uv_server.should_exit = True


async def main():
    global handover
    bot_app_ = await run_bot()
    if autorestart.ENABLED:
        handover = autorestart.Handover(
            started_at=autorestart.run_started_at(),
            pause_queue=lambda: heavy_queue.pause(),
            running_done=heavy_queue.wait_running_done,
            queued_users=lambda: [j.uid for j in list(heavy_queue.running.values()) + heavy_queue.queued],
            save_state=save_state_to_firebase,
            notify=lambda uid, text: bot_app_.bot.send_message(_chat_for_uid(uid), text),
            stop_app=_handover_stop,
        )
        handover.start()
    else:
        logger.info("Auto-restart band (GitHub Actions ke bahar ya AUTO_RESTART=0).")
    try:
        await run_server()
    finally:
        await bot_app_.updater.stop()
        await bot_app_.stop()
        await bot_app_.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
