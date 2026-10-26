"""
statuskit.py — atakne wala nahi, "live" status message.

Purani problem:
  * edit_message_text download/upload loop ke ANDAR await hota tha -> Telegram slow/hang to poora
    job ruk jata tha.
  * koi bhi error (flood, pool timeout, network) chupchap nigal liya jata tha, aur agle 3 sec tak
    dobara try bhi nahi hota tha -> bar "atak" jata tha.
  * data na aaye (retry / FloodWait) to message update hi nahi hota tha.

Ab:
  * Ek alag background heartbeat task har `interval` sec me message edit karta hai (job se
    bilkul independent). Job ko kabhi block nahi karta.
  * RetryAfter (flood) -> utna ruk kar dobara, interval khud badh jata hai.
  * Timeout / network error -> retry, job chalta rehta hai.
  * Message delete ho gaya ho to naya message bhej deta hai.
  * Final update 3 baar retry hota hai.
"""

import asyncio
import logging
import time
from typing import Callable, Optional

from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut

logger = logging.getLogger(__name__)

EDIT_TIMEOUT = 20.0     # ek edit call ka max time
MIN_GAP = 1.2           # do edits ke beech minimum gap (flood se bachne ke liye)
MAX_INTERVAL = 15.0     # flood ke baad interval isse zyada nahi badhega


class StatusMsg:
    def __init__(self, bot, chat_id: int, message_id: int, interval: float = 4.0,
                 render: Optional[Callable[[], str]] = None):
        self.bot, self.chat_id, self.message_id = bot, chat_id, message_id
        self.base_interval = interval
        self.interval = interval
        self.render = render
        self.pending = ""
        self.last_text = ""
        self.cooldown_until = 0.0
        self.last_edit_ts = 0.0
        self.failures = 0
        self._task: Optional[asyncio.Task] = None
        self._wake = asyncio.Event()
        self._stop = False

    # ── public API ────────────────────────────────────
    def start(self, render: Optional[Callable[[], str]] = None):
        if render:
            self.render = render
        if self._task is None:
            self._stop = False
            self._task = asyncio.create_task(self._loop(), name="statusmsg-heartbeat")
        return self

    async def update(self, text: str, force: bool = False):
        """Non-blocking: sirf text rakhta hai, bhejta heartbeat task hai."""
        self.pending = text
        if force:
            self._wake.set()
        if self._task is None:
            # heartbeat nahi chal raha (purana tareeka) -> ek baar seedha try karo
            await self._try_edit(text)

    async def stop(self, final_text: Optional[str] = None):
        self._stop = True
        self._wake.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=EDIT_TIMEOUT + 5)
            except Exception:
                self._task.cancel()
            self._task = None
        if final_text is not None:
            for attempt in range(3):
                if await self._try_edit(final_text, force=True):
                    break
                await asyncio.sleep(1.5 * (attempt + 1))

    # ── internals ─────────────────────────────────────
    async def _loop(self):
        while not self._stop:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            if self._stop:
                break
            try:
                text = self.render() if self.render else self.pending
            except Exception as e:  # render me bug ho to bhi loop zinda rahe
                logger.error(f"status render error: {e}")
                continue
            if text:
                await self._try_edit(text)

    async def _try_edit(self, text: str, force: bool = False) -> bool:
        if not text:
            return False
        if text == self.last_text:
            return True
        now = time.time()
        if now < self.cooldown_until:
            if not force:
                return False
            await asyncio.sleep(self.cooldown_until - now)
        gap = MIN_GAP - (time.time() - self.last_edit_ts)
        if gap > 0:
            await asyncio.sleep(gap)
        try:
            await asyncio.wait_for(
                self.bot.edit_message_text(chat_id=self.chat_id, message_id=self.message_id, text=text),
                timeout=EDIT_TIMEOUT,
            )
            self._ok(text)
            return True
        except RetryAfter as e:
            ra = e.retry_after
            secs = ra.total_seconds() if hasattr(ra, "total_seconds") else float(ra)
            self.cooldown_until = time.time() + secs + 1
            self.interval = min(max(self.interval, self.base_interval) * 1.5, MAX_INTERVAL)
            logger.warning(f"status edit flood: {secs}s ruk raha hoon")
            return False
        except BadRequest as e:
            low = str(e).lower()
            if "not modified" in low:
                self._ok(text)
                return True
            if "not found" in low or "can't be edited" in low or "message_id_invalid" in low:
                return await self._resend(text)
            logger.warning(f"status edit BadRequest: {e}")
            return False
        except (TimedOut, NetworkError, asyncio.TimeoutError) as e:
            self.failures += 1
            logger.warning(f"status edit network/timeout ({self.failures}): {e}")
            return False
        except Exception as e:
            self.failures += 1
            logger.warning(f"status edit error: {e}")
            return False

    async def _resend(self, text: str) -> bool:
        try:
            m = await asyncio.wait_for(self.bot.send_message(chat_id=self.chat_id, text=text), timeout=EDIT_TIMEOUT)
            self.message_id = m.message_id
            self._ok(text)
            return True
        except Exception as e:
            logger.warning(f"status resend fail: {e}")
            return False

    def _ok(self, text: str):
        self.last_text = text
        self.last_edit_ts = time.time()
        self.failures = 0
        # theek chal raha hai -> interval dheere dheere normal par
        self.interval = max(self.base_interval, self.interval * 0.8)
