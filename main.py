import os
import re
import hashlib
import mimetypes
import hmac
from html import escape as html_escape
import logging
import urllib.parse
import asyncio
import math
import shutil
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

import media_fix
from telegram.ext import (
    Application,
    CommandHandler,
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
    max_concurrent_transmissions=4,  # 0.25GB RAM tier ke liye kam kiya (pehle 8 tha)
)

# MESSAGE CACHE (bounded — low-RAM ke liye purane entries auto-evict honge)
message_cache: OrderedDict = OrderedDict()
MESSAGE_CACHE_LIMIT = 50  # max 50 messages cache mein
MESSAGE_CACHE_TTL = 1800  # 30 min baad entry dobara fetch hogi (file_reference expire hone se bachne ke liye)

# ── IN-MEMORY USER SETUP STORE ────────────────────────
user_setup: dict = {}

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


async def save_state_to_firebase():
    if not FIREBASE_URL:
        return
    try:
        db_url = FIREBASE_URL.rstrip("/")
        setup_data = {str(k): v for k, v in user_setup.items()}
        buffer_data = {
            str(uid): {str(ep): vids for ep, vids in eps.items()}
            for uid, eps in quality_buffer.items()
        }
        payload = {"user_setup": setup_data, "quality_buffer": buffer_data}
        async with aiohttp.ClientSession() as session:
            async with session.put(f"{db_url}/bot_state.json", json=payload) as resp:
                if resp.status == 200:
                    logger.info("State Firebase mein save ho gaya.")
                else:
                    logger.warning(f"State save failed: {resp.status}")
    except Exception as e:
        logger.error(f"save_state_to_firebase error: {e}")


async def load_state_from_firebase():
    if not FIREBASE_URL:
        return
    try:
        db_url = FIREBASE_URL.rstrip("/")
        async with aiohttp.ClientSession() as session:
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
                logger.info(f"State load ho gaya. Users: {len(user_setup)}, Buffers: {len(quality_buffer)}")
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


async def save_zip_to_firebase(slug: str, season: str, quality: str, download_link: str, size_text: str = "") -> bool:
    if not FIREBASE_URL:
        logger.error("FIREBASE_URL set nahi hai, ZIP save nahi ho sakta.")
        return False
    try:
        db_url = FIREBASE_URL.rstrip("/")
        path = f"zip_links/{slug}/{season}/{quality}"
        payload = {
            "link"  : download_link,
            "server": SERVER_NAME,
            "time"  : int(time.time()),
        }
        if size_text:
            payload["size"] = size_text
        async with aiohttp.ClientSession() as session:
            async with session.put(f"{db_url}/{path}.json", json=payload) as resp:
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
    if not quality:
        await msg.reply_text(
            "⚠️ *ZIP ki quality nahi mili!*\n\n"
            "ZIP dobara bhejo aur caption me quality likho, jaise: `720p` (1080p / 720p / 480p).",
            parse_mode="Markdown",
        )
        return

    processing = await msg.reply_text("⏳ ZIP process ho raha hai...")
    try:
        forwarded = await copy_with_floodwait(context, chat_id=STORAGE_CHANNEL, from_chat_id=msg.chat_id, message_id=msg.message_id)
        storage_msg_id = forwarded.message_id

        filename = f"{setup['slug']}-{setup['season']}-ZIP-{quality}.zip"
        download_link = make_download_link(storage_msg_id, filename)
        size_text = human_size(file_obj.file_size or 0)

        saved = await save_zip_to_firebase(setup["slug"], setup["season"], quality, download_link, size_text)
        await processing.delete()

        status = "✅ Firebase me save ho gaya" if saved else "⚠️ Firebase save fail — link manually daalna padega"
        await msg.reply_text(
            f"📦 *ZIP Secured!*\n\n"
            f"🎌 *Anime:* `{setup['slug']}`\n"
            f"📺 *Season:* `{setup['season']}`\n"
            f"🎞️ *Quality:* `{quality}`\n"
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
        size_text = human_size(await fetch_remote_size(url))
        saved = await save_zip_to_firebase(setup["slug"], setup["season"], quality, url, size_text)
        if saved:
            done.append(f"{quality}" + (f" ({size_text})" if size_text else ""))
        else:
            failed.append(f"{quality} — Firebase save fail")

    if not done and not failed:
        return
    lines = [f"📦 ZIP link save — {setup['slug']} {setup['season']}"]
    if done:
        lines.append("✅ Saved: " + ", ".join(done))
    if failed:
        lines.append("⚠️ Problem:\n" + "\n".join(failed))
        lines.append("\nLink ke end me 1080p / 720p / 480p likho.")
    await msg.reply_text("\n".join(lines), disable_web_page_preview=True)


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
        "Bot automatically filename aur secured links banayega! 🚀",
        parse_mode="Markdown",
    )


async def setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("❌ *Usage:* `/setup <anime-slug> <season-number>`\n\n", parse_mode="Markdown")
        return
    slug = args[0].lower().strip()
    raw_season = args[1].strip()
    if raw_season.isdigit():
        season = f"S{raw_season}"
    else:
        season = raw_season.upper()

    user_setup[update.effective_user.id] = {"slug": slug, "season": season}
    await save_state_to_firebase()
    await update.message.reply_text(f"✅ *Setup Saved for AnimeVerse!*\n\n🎌 *Anime Slug:* `{slug}`\n📺 *Season:* `{season}`", parse_mode="Markdown")


async def clear_setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    uid = update.effective_user.id
    user_setup.pop(uid, None)
    quality_buffer.pop(uid, None)
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


async def ensure_compatible(processing, storage_msg_id: int, raw_name: str, ext: str, file_size: int, mime: str = "") -> dict:
    """
    Storage mein gayi video ko check karta hai. Browser-compatible hai to kuch nahi karta.
    Nahi hai to plan dikhata hai, convert karke naya message banata hai, purana delete karta hai.
    Hamesha dict return karta hai: {sid, ext, size, note}. Fail hone par original as-is rehti hai.
    """
    default = {"sid": storage_msg_id, "ext": ext, "size": file_size, "note": ""}
    try:
        if not (media_fix.AUTO_CONVERT and media_fix.tools_available() and media_fix.is_video_like(ext, mime)):
            return default
 
        probe_name = f"probe.{ext}"
        code = generate_code(storage_msg_id, probe_name)
        url = f"http://127.0.0.1:{PORT}/animeverse/dl/{storage_msg_id}/{urllib.parse.quote(probe_name)}?code={code}"
 
        try:
            await processing.edit_text("🔍 Video analyze ho rahi hai...")
        except Exception:
            pass
 
        logger.info(f"[fix] msg {storage_msg_id}: probe start ({ext}, {file_size} bytes)")
        info = await media_fix.probe(url)
        plan = media_fix.analyze(info) if info else None
        logger.info(f"[fix] msg {storage_msg_id}: plan = {plan.mode if plan else 'NONE (analyze fail)'}")
        if plan is None:
            default["note"] = "⚠️ Analyze nahi ho saki — original as-is store hui"
            return default
 
        if not plan.needs_work:
            return {"sid": storage_msg_id, "ext": "mp4", "size": file_size, "note": plan.summary}
 
        display_name = raw_name or f"video.{ext}"
 
        if file_size and file_size > media_fix.MAX_PROCESS_BYTES:
            default["note"] = "⚠️ Browser mein shayad na chale, par file bahut badi hai — as-is store hui"
            return default
 
        os.makedirs(media_fix.WORK_DIR, exist_ok=True)
        if shutil.disk_usage(media_fix.WORK_DIR).free < (file_size or 0) * 2.5:
            default["note"] = "⚠️ Convert ke liye disk space kam hai — as-is store hui"
            return default
 
        ui = media_fix.StatusUI(processing, display_name, file_size, plan)
        if media_fix.convert_lock.locked():
            ui.queued = True
            await ui.push(force=True)
 
        async with media_fix.convert_lock:
            ui.queued = False
            logger.info(f"[fix] msg {storage_msg_id}: pipeline start (mode={plan.mode})")
            await ui.start()
            try:
                sent = await media_fix.run_pipeline(
                    url=url, name=display_name, size=file_size, p=plan, ui=ui,
                    pyro=pyro, chat_id=STORAGE_CHANNEL,
                )
            except Exception as e:
                logger.error(f"convert failed for msg {storage_msg_id}: {e}")
                await ui.stop(footer="❌ Convert fail hua — original video as-is store hui.")
                default["note"] = "❌ Convert fail hua — original as-is store hui"
                return default
            await ui.stop(footer="🎉 Complete!")
            logger.info(f"[fix] msg {storage_msg_id}: pipeline done -> new msg {sent.id}")
 
        try:
            await pyro.delete_messages(STORAGE_CHANNEL, storage_msg_id)  # purani (incompatible) copy hatao
        except Exception as e:
            logger.warning(f"old storage msg delete nahi hua ({storage_msg_id}): {e}")
 
        final_size = getattr(getattr(sent, "video", None), "file_size", 0) or file_size
        return {"sid": sent.id, "ext": "mp4", "size": final_size, "note": plan.summary}
    except Exception as e:
        logger.error(f"ensure_compatible error: {e}")
        return default
 
 
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
        # ZIP file (poora season) — episode flow se alag handle hota hai
        if is_zip_file(msg, raw_name):
            await handle_zip_upload(msg, context, setup, file_obj, raw_name)
            return
        if ep_num is None:
            await msg.reply_text("⚠️ *Episode number nahi mila!*", parse_mode="Markdown")
            return
        ext = get_extension(raw_name, fallback="mp4" if (msg.video or msg.video_note) else "mkv")
        file_size = file_obj.file_size or 0
        processing = await msg.reply_text("⏳ Processing for AnimeVerse...")
        try:
            forwarded = await copy_with_floodwait(context, chat_id=STORAGE_CHANNEL, from_chat_id=msg.chat_id, message_id=msg.message_id)
            storage_msg_id = forwarded.message_id
 
            fixed = await ensure_compatible(
                processing, storage_msg_id,
                raw_name or f"{setup['slug']}-{setup['season']}-E{ep_num}.{ext}",
                ext, file_size, getattr(file_obj, "mime_type", "") or "",
            )
 
            if uid not in quality_buffer:
                quality_buffer[uid] = {}
            if ep_num not in quality_buffer[uid]:
                quality_buffer[uid][ep_num] = []
 
            # size = original size (quality order ke liye), final_size = convert ke baad ki size
            quality_buffer[uid][ep_num].append({
                "size": file_size, "sid": fixed["sid"], "ext": fixed["ext"],
                "note": fixed["note"], "final_size": fixed["size"],
            })
            collected = len(quality_buffer[uid][ep_num])  # await se pehle — concurrent handlers mein race na ho
            await save_state_to_firebase()
            await processing.delete()
 
            if collected < QUALITY_COUNT:
                note_line = f"\n{fixed['note']}" if fixed["note"] else ""
                await msg.reply_text(f"✅ *Video {collected}/{QUALITY_COUNT} mila!*{note_line}", parse_mode="Markdown")
                return
            if collected > QUALITY_COUNT:
                await msg.reply_text(f"⚠️ Is episode ke liye {QUALITY_COUNT} videos pehle se aa chuki hain.")
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
                results.append({"quality": quality, "link": stream_link, "dl_link": download_link, "embed_link": embed_link, "size_mb": round((vid.get("final_size") or vid["size"]) / (1024*1024), 2), "note": vid.get("note", ""), "saved": fb_saved})
 
            del quality_buffer[uid][ep_num]
            await save_state_to_firebase()
 
            quality_lines = "\n".join([
                f"  *{r['quality']}* — {r['size_mb']} MB{(chr(10) + '  ' + r['note']) if r['note'] else ''}\n  ▶️ Stream: `{r['link']}`\n  🖼️ Embed: `{r['embed_link']}`\n  ⬇️ Download: `{r['dl_link']}`"
                for r in sorted(results, key=lambda x: quality_sort_key(x["quality"]), reverse=True)
            ])
            await msg.reply_text(f"🎉 *Teeno Quality Secured & Saved!*\n\n🎌 *Anime:* `{setup['slug']}`\n{quality_lines}", parse_mode="Markdown")
        except Exception as e:
            logger.error(f"handle_media error: {e}")
            try:
                await processing.delete()
            except Exception:
                pass
            await msg.reply_text("❌ Process karte waqt error aaya, dobara try karo.")
    else:
        filename = raw_name or f"video_{file_obj.file_unique_id}.mp4"
        processing = await msg.reply_text("⏳ Processing...")
        try:
            forwarded = await copy_with_floodwait(context, chat_id=STORAGE_CHANNEL, from_chat_id=msg.chat_id, message_id=msg.message_id)
            storage_msg_id = forwarded.message_id
            orig_ext = get_extension(filename, fallback="mp4")
            fixed = await ensure_compatible(
                processing, storage_msg_id, filename, orig_ext,
                file_obj.file_size or 0, getattr(file_obj, "mime_type", "") or "",
            )
            storage_msg_id = fixed["sid"]
            if fixed["ext"] != orig_ext:
                filename = os.path.splitext(filename)[0] + "." + fixed["ext"]
            stream_link = make_stream_link(storage_msg_id, filename)
            download_link = make_download_link(storage_msg_id, filename)
            embed_link = make_embed_link(storage_msg_id, filename)
            await processing.delete()
            await msg.reply_text(
                f"▶️ *Stream Link:*\n`{stream_link}`\n\n🖼️ *Embed Link:*\n`{embed_link}`\n\n⬇️ *Download Link:*\n`{download_link}`"
                + (f"\n\n{fixed['note']}" if fixed["note"] else ""),
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Stream", url=stream_link)],
                    [InlineKeyboardButton("🖼️ Embed", url=embed_link)],
                    [InlineKeyboardButton("⬇️ Download", url=download_link)],
                ]),
            )
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
    app = Application.builder().token(BOT_TOKEN).concurrent_updates(True).build()
    await app.bot.delete_webhook(drop_pending_updates=True)
    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("setup",       setup_cmd))
    app.add_handler(CommandHandler("mysetup",     current_setup_cmd))
    app.add_handler(CommandHandler("clearsetup",  clear_setup_cmd))
    app.add_handler(CommandHandler("getlink",     get_link_cmd))
    app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL | filters.AUDIO | filters.VIDEO_NOTE, handle_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_link))
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    return app
 
 
async def run_server():
    config = uvicorn.Config(web_app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(conffig)
    await server.serve()
 
 
async def main():
    bot_app = await run_bot()
    try:
        await run_server()
    finally:
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()
 
if __name__ == "__main__":
    asyncio.run(main())
