"""
tgdl.py — ZIP ke liye tez + stable Telegram downloader (pyrofork ke upar).

Problem (pyrofork 2.3.69 ka Client.get_file / stream_media):
  * HAR call par naya Session (naya TCP connection) banata hai.
  * Agar file dusre DC par hai to HAR call par nayi auth key ka poora DH handshake (Auth.create) + ExportAuthorization karta hai.
  * Session.start() connect fail par chupke infinite retry karta hai, invoke() 10 x 15s chupke retry karta hai.
  -> 8MB ke har tukde par ye sab hota tha: slow, Telegram throttle, aur "Telegram se data nahi aa raha".

Yahan:
  * Auth key har DC ke liye SIRF EK BAAR banti hai (+ ek baar ExportAuthorization/ImportAuthorization).
  * `size` media Session hamesha khule rehte hain (pool), har tukda unhi par aata hai.
  * Har request par apna tez timeout; atke to wo session phek kar naya liya jata hai aur download
    jahan tak aaya wahin se resume hota hai (aaye hue chunk waste nahi).
Agar kuch bhi is raste par unsupported ho to main.py purane stream_media wale raste par fallback karta hai.
"""

import asyncio
import logging
import time
from typing import List, Optional, Union

from pyrogram import raw
from pyrogram.errors import FloodWait
from pyrogram.file_id import FileId, FileType
from pyrogram.session import Auth, Session

logger = logging.getLogger(__name__)

CH = 1024 * 1024                 # Telegram GetFile ka 1 chunk
REQ_TIMEOUT = 20                 # ek chunk ka jawab itni der me na aaye -> session kharab maano
SESSION_START_TIMEOUT = 20
MEDIA_KINDS = ("audio", "document", "photo", "sticker", "animation", "video", "voice", "video_note")


class PoolUnsupported(Exception):
    """Is file/type ke liye pool use nahi ho sakta -> purana raasta."""


class SourceExpired(Exception):
    """file_reference / file_id expire ya invalid -> Message dobara laao."""


def file_id_of(src) -> str:
    """Message ya file_id string se file_id string."""
    if isinstance(src, str):
        return src
    for kind in MEDIA_KINDS:
        media = getattr(src, kind, None)
        if media is not None:
            return media.file_id
    raise PoolUnsupported("message me media nahi mila")


class MediaPool:
    def __init__(self, client, size: int = 6):
        self.client = client
        self.size = max(1, int(size))
        self._dc: dict = {}
        self._lock = asyncio.Lock()
        self.on_note = None            # callable(str) — status me dikhane ke liye (kahan atka)

    def _note(self, msg: str):
        logger.info(f"tgdl: {msg}")
        if self.on_note:
            try:
                self.on_note(msg)
            except Exception:
                pass

    # ── DC state (auth key ek baar) ─────────────────
    async def _dc_state(self, dc_id: int) -> dict:
        st = self._dc.get(dc_id)
        if st:
            return st
        async with self._lock:
            st = self._dc.get(dc_id)
            if st:
                return st
            test_mode = await self.client.storage.test_mode()
            home = await self.client.storage.dc_id()
            if dc_id == home:
                auth_key = await self.client.storage.auth_key()
                needs_import = False
            else:
                self._note(f"DC{dc_id} ki auth key ban rahi hai…")
                t0 = time.time()
                auth_key = await asyncio.wait_for(Auth(self.client, dc_id, test_mode).create(), 40)
                self._note(f"DC{dc_id} auth key ready ({time.time() - t0:.1f}s)")
                needs_import = True
            st = {
                "auth_key": auth_key, "test_mode": test_mode, "needs_import": needs_import, "imported": False,
                "idle": [], "count": 0, "cond": asyncio.Condition(), "create_lock": asyncio.Lock(),
            }
            self._dc[dc_id] = st
            return st

    async def _kill(self, s: Session):
        try:
            await asyncio.wait_for(s.stop(), 5)
        except BaseException:
            pass

    async def _make_session(self, st: dict, dc_id: int) -> Session:
        async with st["create_lock"]:              # pehla session import karta hai, baaki uske baad
            s = Session(self.client, dc_id, st["auth_key"], st["test_mode"], is_media=True)
            t0 = time.time()
            self._note(f"DC{dc_id} connection khul raha hai…")
            try:
                await asyncio.wait_for(s.start(), SESSION_START_TIMEOUT)
                self._note(f"DC{dc_id} connection ready ({time.time() - t0:.1f}s)")
                if st["needs_import"] and not st["imported"]:
                    self._note("Telegram login transfer (ImportAuthorization)…")
                    exported = await self.client.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc_id))
                    await s.invoke(
                        raw.functions.auth.ImportAuthorization(id=exported.id, bytes=exported.bytes),
                        retries=1, timeout=REQ_TIMEOUT,
                    )
                    st["imported"] = True
            except BaseException:
                await self._kill(s)
                raise
            return s

    async def _acquire(self, st: dict, dc_id: int) -> Session:
        async with st["cond"]:
            while True:
                while st["idle"]:
                    s = st["idle"].pop()
                    if s.is_started.is_set():
                        return s
                    st["count"] -= 1               # mara hua session
                    asyncio.create_task(self._kill(s))
                if st["count"] < self.size:
                    st["count"] += 1
                    break
                await st["cond"].wait()
        try:
            return await self._make_session(st, dc_id)
        except BaseException:
            async with st["cond"]:
                st["count"] -= 1
                st["cond"].notify()
            raise

    async def _release(self, st: dict, s: Session, ok: bool):
        async with st["cond"]:
            if ok and s.is_started.is_set():
                st["idle"].append(s)
            else:
                st["count"] -= 1
                asyncio.create_task(self._kill(s))
            st["cond"].notify()

    async def close(self):
        for st in self._dc.values():
            async with st["cond"]:
                idle, st["idle"] = st["idle"], []
                st["count"] -= len(idle)
            for s in idle:
                await self._kill(s)

    # ── download ─────────────────────────────────────
    async def fetch_into(self, src: Union[str, object], offset: int, n: int, parts: List[bytes]) -> None:
        """offset (chunk no.) se n chunk, parts list me append karta hai (exception par bhi jo aaya wo parts me rehta hai)."""
        fid = FileId.decode(file_id_of(src))
        if fid.file_type in (FileType.PHOTO, FileType.CHAT_PHOTO):
            raise PoolUnsupported("photo")
        location = raw.types.InputDocumentFileLocation(
            id=fid.media_id, access_hash=fid.access_hash,
            file_reference=fid.file_reference, thumb_size=fid.thumbnail_size,
        )
        st = await self._dc_state(fid.dc_id)
        s = await self._acquire(st, fid.dc_id)
        ok = False
        try:
            for i in range(n):
                r = await asyncio.wait_for(
                    s.invoke(
                        raw.functions.upload.GetFile(location=location, offset=(offset + i) * CH, limit=CH),
                        retries=0, timeout=REQ_TIMEOUT, sleep_threshold=30,
                    ),
                    timeout=REQ_TIMEOUT + 35,
                )
                if not isinstance(r, raw.types.upload.File):
                    raise PoolUnsupported(type(r).__name__)     # CDN redirect wagairah
                parts.append(r.bytes)
                if len(r.bytes) < CH:                           # file khatam
                    break
            ok = True
        except Exception as e:
            name = type(e).__name__
            if name in ("FileReferenceExpired", "FileReferenceInvalid", "FileIdInvalid", "FileReferenceEmpty"):
                raise SourceExpired(name) from e
            raise
        finally:
            await self._release(st, s, ok)


async def fetch_segment(pool: MediaPool, src, seg_offset: int, n_chunks: int, max_resumes: int = 8) -> bytes:
    """n_chunks chunk laata hai; atke/fail ho to naye session par jahan tak aaya wahin se resume."""
    parts: List[bytes] = []
    resumes = 0
    last: Optional[BaseException] = None
    while len(parts) < n_chunks and not (parts and len(parts[-1]) < CH):
        try:
            await pool.fetch_into(src, seg_offset + len(parts), n_chunks - len(parts), parts)
        except (PoolUnsupported, SourceExpired, asyncio.CancelledError):
            raise
        except FloodWait as e:
            last = e
            await asyncio.sleep(min(e.value + 1, 60))
        except Exception as e:
            last = e
            await asyncio.sleep(0.5)
        else:
            continue
        resumes += 1
        if resumes > max_resumes:
            raise last if last else RuntimeError("tgdl resume limit")
        logger.warning(f"tgdl resume {resumes}/{max_resumes} @chunk {seg_offset + len(parts)}: {type(last).__name__} {last}")
        pool._note(f"Retry {resumes}/{max_resumes}: {type(last).__name__} {last}"[:120])
    return b"".join(parts)
