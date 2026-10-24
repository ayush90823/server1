"""
media_fix.py — AnimeVerse auto video fixer

Video storage channel mein jaane ke baad:
  1. ffprobe se check karta hai ki video browser mein chalegi ya nahi
  2. Plan banata hai (kya change hoga) aur Telegram message mein dikhata hai
  3. Zarurat ho to hi: download -> ffmpeg convert -> upload (live progress bar ke saath)
  4. Zarurat nahi ho to kuch nahi karta
"""

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from html import escape as h
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# ── SETTINGS (env se badal sakte ho) ──────────────────
AUTO_CONVERT = os.getenv("AUTO_CONVERT", "1") != "0"          # 0 = feature band
FFMPEG = os.getenv("FFMPEG_BIN", "ffmpeg")
FFPROBE = os.getenv("FFPROBE_BIN", "ffprobe")
X264_PRESET = os.getenv("X264_PRESET", "veryfast")             # veryfast = jaldi, medium = chhoti file
X264_CRF = os.getenv("X264_CRF", "22")                         # kam = achhi quality
MAX_PROCESS_BYTES = int(float(os.getenv("MAX_PROCESS_MB", "1900")) * 1024 * 1024)
WORK_DIR = os.getenv("WORK_DIR", "/tmp/avwork")
UI_INTERVAL = float(os.getenv("UI_INTERVAL", "3"))             # progress message kitni der baad edit ho

# Ek time pe ek hi conversion (CPU / disk bachane ke liye)
convert_lock = asyncio.Lock()

VIDEO_EXTS = {"mp4", "m4v", "mkv", "webm", "avi", "mov", "ts", "flv", "wmv", "mpg", "mpeg", "3gp"}

CODEC_NAMES = {
    "h264": "H.264", "hevc": "H.265 (HEVC)", "av1": "AV1", "vp9": "VP9", "vp8": "VP8",
    "mpeg4": "MPEG-4", "mpeg2video": "MPEG-2", "vc1": "VC-1", "wmv3": "WMV",
    "aac": "AAC", "mp3": "MP3", "ac3": "AC3", "eac3": "E-AC3", "dts": "DTS",
    "flac": "FLAC", "opus": "Opus", "vorbis": "Vorbis", "truehd": "TrueHD", "pcm_s16le": "PCM",
}


def tools_available() -> bool:
    return bool(shutil.which(FFMPEG) and shutil.which(FFPROBE))


def is_video_like(ext: str, mime: str = "") -> bool:
    return (mime or "").lower().startswith("video/") or (ext or "").lower() in VIDEO_EXTS


# ── FORMAT HELPERS ────────────────────────────────────

def human_bytes(n: float) -> str:
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit in ("B", "KB") else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


def mmss(sec: float) -> str:
    sec = max(0, int(sec))
    h_, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h_}:{m:02d}:{s:02d}" if h_ else f"{m:02d}:{s:02d}"


def bar(pct: Optional[float], width: int = 10) -> str:
    if pct is None:
        return "▱" * width
    pct = min(max(pct, 0.0), 100.0)
    filled = int(round(pct / 100 * width))
    return "▰" * filled + "▱" * (width - filled)


def _pretty_codec(name: str) -> str:
    return CODEC_NAMES.get((name or "").lower(), (name or "unknown").upper())


def _channels_label(n: int) -> str:
    return {1: "mono", 2: "stereo", 6: "5.1", 8: "7.1"}.get(n, f"{n}ch" if n else "")


# ── ANALYSIS ──────────────────────────────────────────

@dataclass
class Plan:
    mode: str = "none"              # none | remux | audio | full
    container: str = ""
    container_ok: bool = True
    v_ok: bool = True
    a_ok: bool = True
    has_audio: bool = True
    duration: float = 0.0
    width: int = 0
    height: int = 0
    v_label: str = ""
    a_label: str = ""
    c_label: str = ""
    dropped: list = field(default_factory=list)
    lines: list = field(default_factory=list)   # Telegram message ke liye plan lines
    summary: str = ""                           # chhota note (final result mein)

    @property
    def needs_work(self) -> bool:
        return self.mode != "none"


async def probe(url: str, timeout: int = 120) -> Optional[dict]:
    """ffprobe HTTP stream pe chalta hai — poori file download nahi hoti."""
    try:
        proc = await asyncio.create_subprocess_exec(
            FFPROBE, "-v", "error", "-print_format", "json",
            "-show_format", "-show_streams", url,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout)
        except asyncio.TimeoutError:
            proc.kill()
            logger.error("ffprobe timeout")
            return None
        if proc.returncode != 0:
            logger.error(f"ffprobe failed: {err.decode(errors='ignore')[-300:]}")
            return None
        return json.loads(out.decode() or "{}")
    except Exception as e:
        logger.error(f"probe error: {e}")
        return None


def analyze(info: dict) -> Optional[Plan]:
    """ffprobe JSON -> Plan. Video stream na ho to None."""
    streams = info.get("streams") or []
    fmt = info.get("format") or {}

    vs = [s for s in streams if s.get("codec_type") == "video" and not (s.get("disposition") or {}).get("attached_pic")]
    aus = [s for s in streams if s.get("codec_type") == "audio"]
    subs = [s for s in streams if s.get("codec_type") == "subtitle"]
    if not vs:
        return None
    v = vs[0]

    p = Plan()
    p.width, p.height = int(v.get("width") or 0), int(v.get("height") or 0)
    try:
        p.duration = float(fmt.get("duration") or v.get("duration") or 0)
    except ValueError:
        p.duration = 0.0

    # container
    names = (fmt.get("format_name") or "").split(",")
    p.container = "MP4" if "mp4" in names else ("MKV" if "matroska" in names else (names[0].upper() or "unknown"))
    p.container_ok = "mp4" in names

    # video
    v_codec = (v.get("codec_name") or "").lower()
    pix = (v.get("pix_fmt") or "").lower()
    m = re.search(r"p(\d{2})(?:le|be)", pix)
    depth = int(m.group(1)) if m else int(v.get("bits_per_raw_sample") or 8)
    p.v_ok = v_codec == "h264" and pix in ("yuv420p", "yuvj420p")
    p.v_label = _pretty_codec(v_codec) + (f" {depth}-bit" if depth > 8 else "")

    # audio
    p.has_audio = bool(aus)
    if aus:
        a = aus[0]
        a_codec = (a.get("codec_name") or "").lower()
        ch = int(a.get("channels") or 0)
        lang = (a.get("tags") or {}).get("language", "")
        p.a_ok = a_codec in ("aac", "mp3")
        p.a_label = " ".join(x for x in (_pretty_codec(a_codec), _channels_label(ch), f"({lang})" if lang else "") if x)
    else:
        p.a_ok = True

    # kya hataya jayega (sirf tab jab kaam hona hi hai)
    if len(aus) > 1:
        p.dropped.append(f"{len(aus) - 1} extra audio track")
    if subs:
        p.dropped.append(f"{len(subs)} subtitle track")

    # mode
    if p.v_ok and p.a_ok and p.container_ok:
        p.mode = "none"
    elif not p.v_ok:
        p.mode = "full"
    elif not p.a_ok:
        p.mode = "audio"
    else:
        p.mode = "remux"

    p.c_label = p.container
    p.lines = _plan_lines(p)
    p.summary = _summary(p)
    return p


def _plan_lines(p: Plan) -> list:
    L = []
    if p.mode == "none":
        L.append(f"✅ Video: {p.v_label} — theek hai")
        L.append(f"✅ Audio: {p.a_label or 'nahi hai'} — theek hai" if p.has_audio else "➖ Audio: nahi hai")
        L.append(f"✅ Container: {p.container} — theek hai")
        return L

    L.append(f"{'✅' if p.v_ok else '⚠️'} Video: {p.v_label}" + (" — theek hai" if p.v_ok else " → H.264 8-bit (re-encode)"))
    if p.has_audio:
        L.append(f"{'✅' if p.a_ok else '⚠️'} Audio: {p.a_label}" + (" — theek hai" if p.a_ok else " → AAC stereo"))
    else:
        L.append("➖ Audio: nahi hai")
    L.append(f"{'✅' if p.container_ok else '⚠️'} Container: {p.container}" + (" — theek hai" if p.container_ok else " → MP4 (faststart)"))
    if p.dropped:
        L.append("🗑 Hataya jayega: " + ", ".join(p.dropped) + " (MP4/browser ke liye)")
    L.append({
        "remux": "🛠 Kaam: sirf container badlega — tez, quality bilkul same",
        "audio": "🛠 Kaam: sirf audio convert hogi, video copy — quality same",
        "full": "🛠 Kaam: video re-encode hogi — thoda time lagega",
    }[p.mode])
    return L


def _summary(p: Plan) -> str:
    if p.mode == "none":
        return "✅ Already compatible — kuch change nahi kiya"
    parts = []
    if not p.v_ok:
        parts.append(f"{p.v_label}→H.264")
    if p.has_audio and not p.a_ok:
        parts.append(f"{p.a_label.split(' ')[0]}→AAC")
    if not p.container_ok:
        parts.append(f"{p.container}→MP4")
    return "🔧 Fixed: " + ", ".join(parts)


# ── FFMPEG COMMAND ────────────────────────────────────

def build_ffmpeg_cmd(src: str, dst: str, p: Plan) -> list:
    cmd = [FFMPEG, "-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-i", src,
           "-map", "0:v:0"]
    if p.has_audio:
        cmd += ["-map", "0:a:0"]
    cmd += ["-sn", "-dn", "-map_chapters", "-1"]

    if p.v_ok:
        cmd += ["-c:v", "copy"]
    else:
        cmd += ["-c:v", "libx264", "-preset", X264_PRESET, "-crf", X264_CRF,
                "-pix_fmt", "yuv420p", "-profile:v", "high",
                "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2"]
    if p.has_audio:
        if p.a_ok:
            cmd += ["-c:a", "copy"]
        else:
            cmd += ["-c:a", "aac", "-b:a", "160k", "-ac", "2"]

    if p.v_ok:
        cmd += ["-avoid_negative_ts", "make_zero"]
    cmd += ["-movflags", "+faststart", "-progress", "pipe:1", "-nostats", dst]
    return cmd


# ── LIVE STATUS MESSAGE ───────────────────────────────

class StatusUI:
    """Telegram message ko throttle karke edit karta hai (progress bar ke saath)."""

    def __init__(self, message, name: str, size: int, p: Plan):
        self.message = message
        self.name = name
        self.size = size
        self.p = p
        label = {"remux": "Remux", "audio": "Audio fix", "full": "Convert"}.get(p.mode, "Convert")
        self.stages = [("download", "Download"), ("convert", label), ("upload", "Upload")]
        self.state = {k: {"status": "pending", "pct": 0.0, "extra": ""} for k, _ in self.stages}
        self.queued = False
        self.footer = ""
        self._last_text = ""
        self._last_push = 0.0
        self._task = None
        self._stop = False
        self._blocked_until = 0.0

    # --- state setters (sync, callbacks se safe) ---
    def begin(self, stage: str):
        self.state[stage].update(status="active", pct=0.0, extra="")

    def progress(self, stage: str, pct: Optional[float], extra: str = ""):
        s = self.state[stage]
        s["pct"] = pct if pct is not None else 0.0
        s["extra"] = extra
        s["indeterminate"] = pct is None

    def done(self, stage: str):
        self.state[stage].update(status="done", pct=100.0, extra="", indeterminate=False)

    def fail(self, stage: str):
        self.state[stage].update(status="failed")

    # --- render ---
    def render(self) -> str:
        p = self.p
        head = [
            "🎬 <b>AnimeVerse Processor</b>",
            f"📁 <code>{h(self.name)}</code>",
            f"📦 {human_bytes(self.size)}" + (f"  •  {p.width}x{p.height}" if p.width else "")
            + (f"  •  {mmss(p.duration)}" if p.duration else ""),
            "",
            "🔍 <b>Analysis</b>",
        ]
        head += [f" • {h(x)}" for x in p.lines]
        head.append("")

        if self.queued:
            head.append("⏳ <b>Queue mein hai</b> — pehle wali video khatam hote hi shuru hogi...")
            return "\n".join(head)

        head.append("⚙️ <b>Progress</b>")
        icons = {"pending": "⏳", "active": "🔄", "done": "✅", "failed": "❌"}
        for key, label in self.stages:
            s = self.state[key]
            pct_txt = "  …" if s.get("indeterminate") else f"{s['pct']:>4.0f}%"
            extra = f"  {h(s['extra'])}" if s["extra"] else ""
            head.append(f"{icons[s['status']]} {label:<9} <code>{bar(None if s.get('indeterminate') else s['pct'])} {pct_txt}</code>{extra}")
        if self.footer:
            head += ["", self.footer]
        return "\n".join(head)

    # --- telegram edit ---
    async def push(self, force: bool = False):
        now = time.monotonic()
        if not force and (now - self._last_push < UI_INTERVAL or now < self._blocked_until):
            return
        text = self.render()
        if text == self._last_text:
            return
        self._last_push = now
        try:
            await self.message.edit_text(text, parse_mode="HTML", disable_web_page_preview=True)
            self._last_text = text
        except Exception as e:
            msg = str(e).lower()
            if "not modified" in msg:
                self._last_text = text
            elif "retry" in msg or "flood" in msg:
                m = re.search(r"(\d+)", msg)
                self._blocked_until = time.monotonic() + (int(m.group(1)) if m else 5) + 1
            else:
                logger.debug(f"status edit error: {e}")

    async def _loop(self):
        while not self._stop:
            await asyncio.sleep(UI_INTERVAL)
            await self.push()

    async def start(self):
        await self.push(force=True)
        self._task = asyncio.create_task(self._loop())

    async def stop(self, footer: str = ""):
        self._stop = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if footer:
            self.footer = footer
        await self.push(force=True)


# ── PIPELINE: download -> convert -> upload ───────────

async def _download(url: str, dst: str, expected: int, ui: StatusUI):
    ui.begin("download")
    t0 = time.monotonic()
    got = 0
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=180)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise RuntimeError(f"download HTTP {resp.status}")
            total = int(resp.headers.get("Content-Length") or expected or 0)
            with open(dst, "wb") as f:
                async for chunk in resp.content.iter_chunked(1 << 20):
                    f.write(chunk)
                    got += len(chunk)
                    el = max(time.monotonic() - t0, 0.001)
                    speed = got / el
                    eta = (total - got) / speed if speed and total else 0
                    ui.progress("download", (got / total * 100) if total else None,
                                f"{human_bytes(got)} / {human_bytes(total)} · ⚡ {human_bytes(speed)}/s · ⏱ {mmss(eta)}")
    if total and got != total:
        raise RuntimeError(f"download incomplete: {got}/{total}")
    ui.done("download")


async def _convert(src: str, dst: str, p: Plan, ui: StatusUI):
    ui.begin("convert")
    cmd = build_ffmpeg_cmd(src, dst, p)
    logger.info("ffmpeg: " + " ".join(cmd))
    t0 = time.monotonic()
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

    err_buf = []

    async def drain_err():
        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            err_buf.append(line.decode(errors="ignore"))
            del err_buf[:-30]

    err_task = asyncio.create_task(drain_err())
    speed = 0.0
    try:
        while True:
            raw = await proc.stdout.readline()
            if not raw:
                break
            line = raw.decode(errors="ignore").strip()
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k == "speed":
                try:
                    speed = float(v.replace("x", "").strip())
                except ValueError:
                    pass
            elif k in ("out_time_us", "out_time_ms"):
                try:
                    cur = int(v) / 1_000_000
                except ValueError:
                    continue
                if cur < 0:
                    continue
                if p.duration > 0:
                    pct = min(cur / p.duration * 100, 99.9)
                    eta = (p.duration - cur) / speed if speed > 0 else 0
                    extra = f"⚡ {speed:.1f}x" + (f" · ⏱ {mmss(eta)}" if eta else "")
                    ui.progress("convert", pct, extra)
                else:
                    ui.progress("convert", None, f"{mmss(cur)} process hua")
        rc = await proc.wait()
    except asyncio.CancelledError:
        proc.kill()
        raise
    finally:
        await err_task
    if rc != 0:
        raise RuntimeError("ffmpeg failed: " + "".join(err_buf)[-400:])
    if not os.path.exists(dst) or os.path.getsize(dst) == 0:
        raise RuntimeError("ffmpeg ne output file nahi banayi")
    ui.done("convert")


async def _upload(pyro, chat_id: int, path: str, p: Plan, ui: StatusUI):
    ui.begin("upload")
    t0 = time.monotonic()

    def cb(current, total):
        el = max(time.monotonic() - t0, 0.001)
        speed = current / el
        eta = (total - current) / speed if speed and total else 0
        ui.progress("upload", (current / total * 100) if total else None,
                    f"{human_bytes(current)} / {human_bytes(total)} · ⚡ {human_bytes(speed)}/s · ⏱ {mmss(eta)}")

    sent = await pyro.send_video(
        chat_id=chat_id, video=path, duration=int(p.duration), width=p.width, height=p.height,
        supports_streaming=True, file_name=os.path.basename(path), progress=cb,
    )
    ui.done("upload")
    return sent


def safe_name(name: str) -> str:
    base = os.path.splitext(name or "video")[0]
    base = re.sub(r"[^\w.\- ]", "_", base).strip() or "video"
    return base[:120] + ".mp4"


async def run_pipeline(*, url: str, name: str, size: int, p: Plan, ui: StatusUI, pyro, chat_id: int):
    """Poora kaam. Naya Telegram message (sent) return karta hai. Temp files hamesha delete hoti hain."""
    os.makedirs(WORK_DIR, exist_ok=True)
    wd = tempfile.mkdtemp(prefix="job_", dir=WORK_DIR)
    stage = "download"
    try:
        src = os.path.join(wd, "source.bin")
        dst = os.path.join(wd, safe_name(name))
        await _download(url, src, size, ui)
        stage = "convert"
        await _convert(src, dst, p, ui)
        os.remove(src)  # disk jaldi khaali
        stage = "upload"
        return await _upload(pyro, chat_id, dst, p, ui)
    except BaseException:
        ui.fail(stage)
        raise
    finally:
        shutil.rmtree(wd, ignore_errors=True)
