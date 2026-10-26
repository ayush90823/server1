"""
fastzip.py — tez ZIP engine + progress dashboard.

Purana system (slow kyun tha):
  * Telegram se video 4MB ke tukde EK-EK karke aata tha (serial) -> speed = 1 request / RTT.
  * Har tukda likhne ke baad hi agla maangta tha (download aur disk-write overlap nahi).
  * Pehle poora zip bante tha, PHIR upload — dono kabhi saath nahi chalte the.

Ab:
  * `workers` tukde ek saath (parallel read-ahead) aate hain, order me likhe jate hain.
    Video ki boundary par bhi read-ahead nahi rukta (agle video ke tukde pehle se aa rahe hote hain).
  * Disk write alag thread me, download ke saath overlap.
  * Part N upload hota hai tab Part N+1 ban raha hota hai (pipeline).
  * Har tukde par timeout + backoff retry; FloodWait par user ko note dikhta hai.
"""

import asyncio
import logging
import math
import time
import zipfile
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Awaitable, Callable, List, Optional

logger = logging.getLogger(__name__)

CHUNK = 1024 * 1024          # Telegram stream_media ka 1 chunk = 1MB
SEG_TIMEOUT = 75             # ek tukde ka max time (isse zyada -> retry)
SEG_RETRIES = 6


class ZipCancelled(Exception):
    pass


class ShortSegment(Exception):
    """Telegram ne expected se kam (ya 0) bytes diye."""


# ── formatting helpers ────────────────────────────────

def human_size(n: int) -> str:
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.2f} {unit}"
        n /= 1024
    return "0 B"


def fmt_dur(sec) -> str:
    if sec is None or sec != sec or sec < 0:
        return "calculating…"
    sec = int(sec)
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h {m:02d}m"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def fmt_speed(bps: float) -> str:
    return f"{bps / 1048576:.1f} MB/s" if bps else "—"


def make_bar(pct: float, width: int = 14) -> str:
    pct = max(0.0, min(100.0, pct))
    filled = int(width * pct / 100)
    return "█" * filled + "░" * (width - filled)


class _Speed:
    """Smoothed (EMA) speed meter. 8 sec se zyada data na aaye to speed 0 dikhegi."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.ema, self.t, self.b, self.last_feed = 0.0, None, 0, 0.0

    def feed(self, cum_bytes: int):
        now = time.time()
        if self.t is None:
            self.t, self.b, self.last_feed = now, cum_bytes, now
            return
        dt = now - self.t
        if dt >= 1.0:
            inst = (cum_bytes - self.b) / dt
            self.ema = inst if self.ema == 0 else 0.3 * inst + 0.7 * self.ema
            self.t, self.b, self.last_feed = now, cum_bytes, now

    def value(self) -> float:
        if self.last_feed and time.time() - self.last_feed > 8:
            return 0.0
        return self.ema


# ── progress dashboard ────────────────────────────────

class ZipProgress:
    """Download+Zip aur Upload ek saath chal sakte hain (pipeline), isliye dono ki alag state."""

    def __init__(self, slug: str, season: str, total_bytes: int, n_videos: int, n_parts: int):
        self.slug, self.season = slug, season
        self.total_bytes = max(total_bytes, 1)
        self.n_videos, self.n_parts = n_videos, n_parts
        self.started = time.time()
        # download/zip side
        self.dl_done = 0
        self.dl_part = 0               # abhi kaunsa part ban raha hai (0 = koi nahi)
        self.dl_part_done = self.dl_part_total = 0
        self.vid_i = self.vid_n = 0
        self.name = ""
        self.dl_speed = _Speed()
        self.zipped_parts = 0
        # upload side
        self.up_done = 0
        self.up_part = 0               # abhi kaunsa part upload ho raha hai (0 = koi nahi)
        self.up_base = 0
        self.up_part_done = self.up_part_total = 0
        self.up_speed = _Speed()
        self.uploaded_parts = 0
        # health
        self.last_activity = time.time()
        self.note = ""
        self.finished = False

    # activity / health
    def touch(self, note: str = ""):
        self.last_activity = time.time()
        self.note = note

    def set_note(self, note: str):
        """Retry / FloodWait ka message (activity time nahi badhata)."""
        self.note = note

    def live_line(self) -> str:
        idle = time.time() - self.last_activity
        clock = time.strftime("%H:%M:%S")
        if idle > 12:
            why = self.note or "Telegram slow hai, retry chal raha hai"
            return f"🟡 {why} ({int(idle)}s se naya data nahi) • {clock}"
        return f"🟢 Live • {clock}"

    # download side
    def start_part(self, p_no: int, vids: list):
        self.dl_part, self.vid_n, self.vid_i = p_no, len(vids), 1
        self.dl_part_total = sum(v.get("size", 0) for v in vids) or 1
        self.dl_part_done = 0
        self.dl_speed.reset()
        self.touch()

    def set_video(self, i: int, name: str):
        self.vid_i, self.name = i, name

    def add_dl(self, n: int):
        self.dl_done += n
        self.dl_part_done += n
        self.dl_speed.feed(self.dl_done)
        self.touch()

    def finish_zip_part(self):
        self.zipped_parts += 1
        self.dl_part = 0
        self.name = ""

    # upload side
    def start_upload(self, p_no: int, part_bytes: int):
        self.up_part = p_no
        self.up_part_done, self.up_part_total = 0, max(part_bytes, 1)
        self.up_base = self.up_done
        self.up_speed.reset()
        self.touch()

    def set_upload(self, cur: int, total: int):
        self.up_part_done = cur
        self.up_part_total = total or self.up_part_total
        self.up_done = self.up_base + cur
        self.up_speed.feed(self.up_done)
        self.touch()

    def finish_part(self):
        self.up_done = self.up_base + self.up_part_total
        self.uploaded_parts += 1
        self.up_part = 0

    def eta(self):
        dl_rate = self.dl_speed.ema
        up_rate = self.up_speed.ema
        if not dl_rate and not up_rate:
            return None
        dl_rate = dl_rate or up_rate
        up_rate = up_rate or dl_rate
        rem_dl = max(self.total_bytes - self.dl_done, 0)
        rem_up = max(self.total_bytes - self.up_done, 0)
        # dono pipeline me chalte hain -> jo zyada der lega wahi ETA hai
        return max(rem_dl / dl_rate, rem_up / up_rate)

    def render(self) -> str:
        overall = (self.dl_done + self.up_done) * 100 / (2 * self.total_bytes)
        elapsed = time.time() - self.started
        z_icon = "🔄" if self.dl_part else ("✅" if self.zipped_parts >= self.n_parts else "⚪")
        u_icon = "🔄" if self.up_part else ("✅" if self.uploaded_parts >= self.n_parts else "⚪")
        lines = [
            f"📦 ZIP BUILDER — {self.slug} • {self.season}",
            "━━━━━━━━━━━━━━━━━━━━━━",
            f"📊 Overall   {make_bar(overall)}  {overall:.0f}%",
            f"⏱ Elapsed {fmt_dur(time.time() - self.started)}   •   ⏳ ETA {fmt_dur(self.eta())}",
            f"{z_icon} Zipping  ➜  {u_icon} Upload  ➜  ⚪ Link",
            f"🧩 Parts: {self.zipped_parts}/{self.n_parts} zipped • {self.uploaded_parts}/{self.n_parts} uploaded",
            "",
        ]
        if self.dl_part:
            pct = self.dl_part_done * 100 / self.dl_part_total
            lines += [
                f"⬇️ Download+Zip — Part {self.dl_part}/{self.n_parts} • 🎞 Video {self.vid_i}/{self.vid_n}",
                f"📄 {self.name}" if self.name else "",
                f"{make_bar(pct)}  {pct:.0f}%",
                f"💾 {human_size(self.dl_part_done)} / {human_size(self.dl_part_total)}   ⚡ {fmt_speed(self.dl_speed.value())}",
                "",
            ]
        if self.up_part:
            pct = self.up_part_done * 100 / self.up_part_total
            lines += [
                f"⬆️ Upload — Part {self.up_part}/{self.n_parts}",
                f"{make_bar(pct)}  {pct:.0f}%",
                f"💾 {human_size(self.up_part_done)} / {human_size(self.up_part_total)}   ⚡ {fmt_speed(self.up_speed.value())}",
                "",
            ]
        lines = [l for i, l in enumerate(lines) if l != "" or (i and lines[i - 1] != "")]
        lines += [self.live_line(), "━━━━━━━━━━━━━━━━━━━━━━", "🛑 Cancel: /cancelzip"]
        return "\n".join(lines)

    def final_text(self, total_zip_bytes: int) -> str:
        elapsed = time.time() - self.started
        return (
            f"📦 ZIP BUILDER — {self.slug} • {self.season}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Overall   {make_bar(100)}  100%\n"
            "✅ Zipping  ➜  ✅ Upload  ➜  ✅ Link\n\n"
            f"🎉 Complete!\n"
            f"🧩 Parts: {self.n_parts}   •   🎞 Videos: {self.n_videos}\n"
            f"💾 Total size: {human_size(total_zip_bytes)}\n"
            f"⏱ Total time: {fmt_dur(elapsed)}\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        )


# ── robust segment fetch ──────────────────────────────

def make_robust_fetch(raw_fetch: Callable[..., Awaitable[bytes]], prog: Optional[ZipProgress] = None):
    """raw_fetch(fid, idx, n) -> bytes ko timeout + backoff retry ke saath wrap karta hai."""

    async def fetch(fid, idx: int, n: int, expected: int = 0) -> bytes:
        last = None
        for attempt in range(SEG_RETRIES):
            try:
                data = await asyncio.wait_for(raw_fetch(fid, idx, n), timeout=SEG_TIMEOUT)
                # khaali / adhura data bhi failure hai -> retry (pehle ye seedha "chhota aaya" error banta tha)
                if expected and len(data) < expected:
                    raise ShortSegment(f"{len(data)}/{expected} bytes")
                return data
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last = e
                wait = min(2 ** attempt, 20)
                fw = getattr(e, "value", None)
                if isinstance(fw, (int, float)) and fw > 0:    # Telegram FloodWait: utna ruko
                    wait = max(wait, min(fw + 1, 120))
                msg = f"Retry {attempt + 1}/{SEG_RETRIES}: {type(e).__name__}"
                logger.warning(f"segment {idx} fail ({msg}): {e!r} -> {wait}s")
                if prog:
                    prog.set_note(msg)
                await asyncio.sleep(wait)
        raise RuntimeError(f"Telegram se data nahi aa raha (segment {idx}): {last!r}")

    return fetch


# ── parallel build ────────────────────────────────────

def plan_segments(vids: list, seg_chunks: int):
    """Flat plan: [(video_idx, chunk_offset, n_chunks)] — poore part ke liye."""
    plan = []
    for vi, v in enumerate(vids):
        size = int(v.get("size") or 0)
        if size <= 0:
            raise RuntimeError(f"{v.get('name') or v.get('uniq')}: file size pata nahi, ZIP nahi ban sakta")
        total_chunks = math.ceil(size / CHUNK)
        i = 0
        while i < total_chunks:
            n = min(seg_chunks, total_chunks - i)
            plan.append((vi, i, n))
            i += n
    return plan


async def build_zip_part(fetch, job: dict, prog: ZipProgress, p_no: int, vids: list,
                         arcnames: List[str], zip_path: str, workers: int = 10, seg_chunks: int = 2):
    """vids ko parallel download karke STORED zip me likhta hai."""
    prog.start_part(p_no, vids)
    plan = plan_segments(vids, seg_chunks)
    loop = asyncio.get_running_loop()
    writer = ThreadPoolExecutor(max_workers=1, thread_name_prefix="zipwriter")

    zf = zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED, allowZip64=True)
    dest = None
    cur_vi, written = -1, 0
    pending_write: Optional[asyncio.Future] = None
    tasks: deque = deque()
    it = iter(plan)

    def refill():
        while len(tasks) < workers:
            nxt = next(it, None)
            if nxt is None:
                return
            vi, idx, n = nxt
            vsize = int(vids[vi].get("size") or 0)
            exp = max(min(n * CHUNK, vsize - idx * CHUNK), 0)
            tasks.append((nxt, asyncio.create_task(fetch(vids[vi]["fid"], idx, n, exp))))

    async def close_current():
        nonlocal dest, pending_write
        if pending_write is not None:
            await pending_write
            pending_write = None
        if dest is not None:
            size = int(vids[cur_vi].get("size") or 0)
            if written != size:
                raise RuntimeError(f"{arcnames[cur_vi]}: download adhura reh gaya ({written}/{size} bytes)")
            dest.close()
            dest = None

    try:
        refill()
        while tasks:
            (vi, idx, n), t = tasks.popleft()
            data = await t
            refill()
            if job.get("cancel"):
                raise ZipCancelled()

            if vi != cur_vi:
                await close_current()
                zi = zipfile.ZipInfo(arcnames[vi], date_time=time.localtime()[:6])
                zi.compress_type = zipfile.ZIP_STORED
                dest = zf.open(zi, "w", force_zip64=True)
                cur_vi, written = vi, 0
                prog.set_video(vi + 1, arcnames[vi])

            size = int(vids[vi].get("size") or 0)
            expected = min(n * CHUNK, size - idx * CHUNK)
            if len(data) < expected:
                raise RuntimeError(f"{arcnames[vi]}: segment {idx} chhota aaya ({len(data)}/{expected})")
            if len(data) > expected:
                data = data[:expected]

            if pending_write is not None:
                await pending_write          # pichla write khatam (download iske saath chal raha tha)
            pending_write = loop.run_in_executor(writer, dest.write, data)
            written += len(data)
            prog.add_dl(len(data))

        await close_current()
    finally:
        for _, t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*[t for _, t in tasks], return_exceptions=True)
        if pending_write is not None:
            try:
                await pending_write
            except Exception:
                pass
        try:
            if dest is not None:
                dest.close()
        except Exception:
            pass
        try:
            zf.close()
        except Exception:
            pass
        writer.shutdown(wait=False)
    prog.finish_zip_part()
