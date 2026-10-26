"""
taskqueue.py — heavy tasks ki queue.

Chhote kaam (episode add, link generate, /getlink, quality/range jawab...) turant, ek saath chalte hain.
Heavy kaam (ZIP banana — bahut bandwidth/disk leta hai) is queue me jate hain aur HEAVY_WORKERS (default 1)
ek saath chalte hain, baaki line me rehte hain. Queue Firebase me save hoti hai, isliye bot restart
(GitHub Actions ka naya run) ke baad queue wahin se aage chalti hai.
"""

import asyncio
import logging
import time
from typing import Awaitable, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class Job:
    def __init__(self, job_id: str, kind: str, uid: int, label: str, payload: dict):
        self.id, self.kind, self.uid, self.label, self.payload = job_id, kind, uid, label, payload
        self.created = time.time()
        self.started: Optional[float] = None

    def to_dict(self) -> dict:
        return {"id": self.id, "kind": self.kind, "uid": self.uid, "label": self.label,
                "payload": self.payload, "created": self.created}

    @classmethod
    def from_dict(cls, d: dict) -> "Job":
        j = cls(d["id"], d["kind"], int(d["uid"]), d.get("label", ""), d.get("payload") or {})
        j.created = d.get("created", time.time())
        return j


class HeavyQueue:
    def __init__(self, workers: int = 1, on_change: Optional[Callable[[], Awaitable[None]]] = None):
        self.workers = max(1, workers)
        self.on_change = on_change
        self.queued: List[Job] = []
        self.running: Dict[str, Job] = {}
        self.handlers: Dict[str, Callable[[Job], Awaitable[None]]] = {}
        self.paused = False
        self._frozen = None      # shutdown ke time ka snapshot (running job ka record na kho jaye)
        self._wake = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._tasks: List[asyncio.Task] = []
        self._seq = 0

    # ── setup ─────────────────────────────────────────
    def register(self, kind: str, fn: Callable[[Job], Awaitable[None]]):
        self.handlers[kind] = fn

    def start(self):
        if self._tasks:
            return
        for n in range(self.workers):
            self._tasks.append(asyncio.create_task(self._worker(), name=f"heavy-worker-{n}"))

    async def shutdown(self):
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    # ── submit / manage ───────────────────────────────
    def new_id(self) -> str:
        self._seq += 1
        return f"{int(time.time())}-{self._seq}"

    def submit(self, kind: str, uid: int, label: str, payload: dict, job: Optional[Job] = None):
        """(job, ahead) — ahead = is job se pehle kitne heavy job hain (0 = turant shuru)."""
        job = job or Job(self.new_id(), kind, uid, label, payload)
        ahead = len(self.running) + len(self.queued)
        self.queued.append(job)
        self._idle.clear()
        self._wake.set()
        self._changed()
        return job, ahead

    def remove_user(self, uid: int) -> List[Job]:
        """Is user ke sirf LINE me khade jobs hata do (chal raha job alag se cancel hota hai)."""
        gone = [j for j in self.queued if j.uid == uid]
        if gone:
            self.queued = [j for j in self.queued if j.uid != uid]
            self._refresh_idle()
            self._changed()
        return gone

    def user_jobs(self, uid: int) -> List[Job]:
        return [j for j in list(self.running.values()) + self.queued if j.uid == uid]

    def snapshot(self) -> List[dict]:
        """Persist karne ke liye: running + queued (restart ke baad sab dobara line me)."""
        if self._frozen is not None:
            return self._frozen
        return [j.to_dict() for j in list(self.running.values()) + self.queued]

    def restore(self, items: List[dict]) -> int:
        n = 0
        for d in items or []:
            try:
                self.submit(d["kind"], int(d["uid"]), d.get("label", ""), d.get("payload") or {}, job=Job.from_dict(d))
                n += 1
            except Exception as e:
                logger.error(f"queue restore skip: {e}")
        return n

    def describe(self, uid: Optional[int] = None) -> str:
        if not self.running and not self.queued:
            return "✅ Queue khali hai — koi heavy task nahi chal raha."
        lines = ["📋 HEAVY TASK QUEUE", "━━━━━━━━━━━━━━━━━━━━━━"]
        for j in self.running.values():
            mine = " (tumhara)" if uid is not None and j.uid == uid else ""
            lines.append(f"🔄 Chal raha: {j.label}{mine}")
        for i, j in enumerate(self.queued, 1):
            mine = " (tumhara)" if uid is not None and j.uid == uid else ""
            lines.append(f"⏳ #{i}: {j.label}{mine}")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("ℹ️ Chhote kaam (episode, link, /getlink) queue ke bina turant hote hain.")
        return "\n".join(lines)

    # ── pause / drain (restart ke time) ───────────────
    def pause(self):
        self.paused = True

    def freeze(self):
        """Ab se snapshot() wahi rahega jo abhi hai (running job cancel hone par bhi queue me bana rahe)."""
        self._frozen = None
        self._frozen = self.snapshot()

    def resume(self):
        self.paused = False
        self._wake.set()

    def is_idle(self) -> bool:
        return not self.running

    async def wait_running_done(self, timeout: float) -> bool:
        """Chal rahe jobs khatam hone ka wait (queued jobs ka nahi). True = sab khatam."""
        end = time.time() + timeout
        while self.running and time.time() < end:
            await asyncio.sleep(1)
        return not self.running

    # ── internals ─────────────────────────────────────
    def _refresh_idle(self):
        if not self.running and not self.queued:
            self._idle.set()

    def _changed(self):
        if self.on_change:
            try:
                asyncio.get_running_loop().create_task(self.on_change())
            except Exception:
                pass

    async def _worker(self):
        while True:
            if self.paused or not self.queued:
                self._wake.clear()
                await self._wake.wait()
                continue
            job = self.queued.pop(0)
            self.running[job.id] = job
            job.started = time.time()
            cancelled = False
            try:
                fn = self.handlers.get(job.kind)
                if fn is None:
                    logger.error(f"queue: handler nahi mila for {job.kind}")
                else:
                    await fn(job)
            except asyncio.CancelledError:
                cancelled = True    # restart/shutdown: job snapshot persisted rehna chahiye
                raise
            except Exception as e:
                logger.error(f"heavy job {job.id} crash: {e}", exc_info=True)
            finally:
                self.running.pop(job.id, None)
                self._refresh_idle()
                if not cancelled:
                    self._changed()
