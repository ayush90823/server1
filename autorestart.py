"""
autorestart.py — GitHub Actions band hone se PEHLE bot khud naya run shuru kar deta hai.

GitHub Actions ek job ko max ~6 ghante chalne deta hai (workflow me timeout-minutes: 350). Uske baad job
kill ho jata hai aur bot band. Ye module:

  1. RESTART_AFTER_MIN (default 325 min) par "handover" shuru karta hai:
       - heavy queue me naya job start hona band (queued jobs Firebase me safe rehte hain)
       - GitHub API se isi workflow ka naya run dispatch karta hai
         (workflow me concurrency group hai, isliye naya run PENDING rehta hai aur purana
          khatam hote hi apne aap shuru ho jata hai — dono ek saath polling nahi karte)
       - jo heavy job chal raha hai use HARD_EXIT_MIN (default 343) tak khatam hone ka time deta hai
  2. State save karke gracefully exit(0). Naya run Firebase se state + queue uthakar wahin se chalta hai.
  3. /restartnow se manual handover bhi ho sakta hai.

Sirf GitHub Actions me active hota hai (GITHUB_ACTIONS=true). AUTO_RESTART=0 se band kar sakte ho.
Dispatch ke liye default GITHUB_TOKEN kaafi hai (workflow me `permissions: actions: write`).
Agar kisi wajah se na chale to GH_PAT secret (Personal Access Token, actions:write) bhi support hai.
"""

import asyncio
import logging
import os
import time
from typing import Awaitable, Callable, List, Optional

import aiohttp

logger = logging.getLogger(__name__)

ENABLED = os.getenv("AUTO_RESTART", "1") != "0" and os.getenv("GITHUB_ACTIONS", "") == "true"
RESTART_AFTER_MIN = float(os.getenv("RESTART_AFTER_MIN", "325"))
HARD_EXIT_MIN = float(os.getenv("HARD_EXIT_MIN", "343"))


def run_started_at() -> float:
    """Workflow run ka start time (crash-restart par bhi clock reset na ho)."""
    try:
        return float(os.getenv("RUN_STARTED_AT", ""))
    except ValueError:
        return time.time()


def _workflow_file() -> str:
    ref = os.getenv("GITHUB_WORKFLOW_REF", "")          # owner/repo/.github/workflows/main.yml@refs/heads/main
    if ".github/workflows/" in ref:
        return ref.split(".github/workflows/", 1)[1].split("@", 1)[0]
    return os.getenv("WORKFLOW_FILE", "main.yml")


async def dispatch_next_run(retries: int = 5) -> bool:
    """Isi repo/workflow ka naya run trigger karta hai. True = GitHub ne accept kiya (HTTP 204)."""
    repo = os.getenv("GITHUB_REPOSITORY", "")
    token = os.getenv("GH_PAT") or os.getenv("GITHUB_TOKEN", "")
    ref = os.getenv("GITHUB_REF_NAME", "main")
    if not repo or not token:
        logger.error("dispatch: GITHUB_REPOSITORY / token nahi mila")
        return False
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{_workflow_file()}/dispatches"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    for attempt in range(1, retries + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.post(url, headers=headers, json={"ref": ref}) as r:
                    if r.status == 204:
                        logger.info("Naya workflow run dispatch ho gaya.")
                        return True
                    body = (await r.text())[:300]
                    logger.error(f"dispatch HTTP {r.status} (try {attempt}/{retries}): {body}")
        except Exception as e:
            logger.error(f"dispatch error (try {attempt}/{retries}): {e!r}")
        await asyncio.sleep(3 * attempt)
    return False


class Handover:
    """Restart ka poora flow. Sab kaam callbacks se hota hai, isliye alag se test ho sakta hai."""

    def __init__(self, *, started_at: float,
                 pause_queue: Callable[[], None],
                 running_done: Callable[[float], Awaitable[bool]],
                 queued_users: Callable[[], List[int]],
                 save_state: Callable[[], Awaitable[None]],
                 notify: Callable[[int, str], Awaitable[None]],
                 stop_app: Callable[[], Awaitable[None]],
                 dispatch: Callable[[], Awaitable[bool]] = dispatch_next_run,
                 restart_after_min: float = RESTART_AFTER_MIN,
                 hard_exit_min: float = HARD_EXIT_MIN):
        self.started_at = started_at
        self.pause_queue, self.running_done, self.queued_users = pause_queue, running_done, queued_users
        self.save_state, self.notify, self.stop_app, self.dispatch = save_state, notify, stop_app, dispatch
        self.restart_after = restart_after_min * 60
        self.hard_exit = max(hard_exit_min, restart_after_min) * 60
        self.began = False
        self.dispatched: Optional[bool] = None
        self._task: Optional[asyncio.Task] = None

    # ── info (for /status) ─────────────────────────────
    def uptime(self) -> float:
        return time.time() - self.started_at

    def seconds_to_restart(self) -> float:
        return max(0.0, self.restart_after - self.uptime())

    # ── lifecycle ─────────────────────────────────────
    def start(self):
        self._task = asyncio.create_task(self._timer(), name="auto-restart-timer")

    async def _timer(self):
        wait = self.seconds_to_restart()
        logger.info(f"Auto-restart scheduler: {wait / 60:.0f} min baad handover shuru hoga.")
        await asyncio.sleep(wait)
        await self.begin("GitHub 6h limit paas hai")

    async def begin(self, reason: str):
        if self.began:
            return
        self.began = True
        logger.warning(f"HANDOVER shuru: {reason}")
        # 1) naya run pehle dispatch (concurrency group ki wajah se wo PENDING rehta hai, purana abhi chalta rahe)
        #    fail ho to har 90s retry — jab tak deadline na aa jaye; tab tak bot normal kaam karta rahta hai.
        while True:
            self.dispatched = await self.dispatch()
            if self.dispatched or self.uptime() > self.hard_exit - 90:
                break
            await asyncio.sleep(90)
        # 2) ab naya heavy job start nahi hoga
        self.pause_queue()
        await self.save_state()

        left = max(self.hard_exit - self.uptime(), 5)
        for uid in set(self.queued_users()):
            try:
                await self.notify(
                    uid,
                    f"♻️ Auto-restart ({reason}).\n"
                    f"Bot ~{int(left // 60)} min me naye run par shift hoga, ZIP job/queue safe hai "
                    f"aur restart ke baad apne aap wahin se chalega. Chhote kaam 1-2 min ke gap ke baad phir normal.",
                )
            except Exception:
                pass

        finished = await self.running_done(left)             # chal rahe job ko khatam hone ka time
        if not finished:
            logger.warning("Heavy job deadline tak khatam nahi hua — restart ke baad queue se dobara chalega.")
        await self.save_state()                              # running job ka snapshot bhi persist
        await self.stop_app()
