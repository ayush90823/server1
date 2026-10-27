# Kya badla (server1 upgrade)

## Files
- `statuskit.py`  – atakne wala nahi, heartbeat progress message
- `fastzip.py`    – parallel ZIP engine + dashboard
- `taskqueue.py`  – heavy task queue (Firebase me persist)
- `autorestart.py`– GitHub 6h limit se pehle self-restart
- `main.py`, `.github/workflows/main.yml` – inhe use karne ke liye update

## Commands (naye)
- `/queue`      – heavy queue dekho
- `/status`     – uptime, agla auto-restart, queue count
- `/restartnow` – manual handover (sirf ALLOWED_USERS)
- `/done`       – ab ZIP ko queue me daalta hai; turant naya `/zip` ya koi chhota kaam kar sakte ho

## Settings (env, sab optional)
| Name | Default | Matlab |
|---|---|---|
| ZIP_WORKERS | 10 | parallel tukde (speed) |
| ZIP_SEG_CHUNKS | 2 | 1 tukda = N x 1MB |
| PYRO_TRANSMISSIONS | 12 | Pyrogram parallel limit (low-RAM host par 4) |
| HEAVY_WORKERS | 1 | ek saath kitne heavy job |
| RESTART_AFTER_MIN | 325 | itne min baad handover shuru |
| HARD_EXIT_MIN | 343 | chal rahe job ko is time tak deadline |
| AUTO_RESTART | 1 | 0 = band |
| DROP_PENDING_UPDATES | 0 | 1 = start par purane messages drop |

## GitHub me ek baar check karo
Settings -> Actions -> General -> Workflow permissions = "Read and write permissions".
Agar auto-dispatch fail ho (log me `dispatch HTTP 403`), to ek Personal Access Token
(actions:write) banao aur repo secret `GH_PAT` me daalo.
Cron safety-net tab bhi bot ko 20 min ke andar wapas utha dega.

## Fix: "segment 0 chhota aaya (0/...)" (ZIP error)
- `fastzip.py`: Telegram se khaali/adhura segment aaye to ab retry hota hai (pehle seedha error aata tha). `ShortSegment` add hua.
- `main.py`: ZIP ke liye video ka chat_id/message_id save hota hai; zip banate waqt fresh Message (get_messages) se download hota hai,
  taaki expire hua file_id/file_reference problem na kare. Khaali aaye to Message refresh karke dobara try.
- Purane collected videos (jinme cid/mid nahi) file_id se hi chalenge. Pakka fix ke liye unhe dobara bhejna behtar hai.

## Fix: "Telegram se data nahi aa raha (segment N): TimeoutError"
- Wajah: pyrofork har fetch par naya Telegram connection (+ ExportAuthorization) banata hai. 2MB tukde x 10 parallel = bahut saare connection, Telegram throttle karke atka deta hai.
- Ab default: `ZIP_SEG_CHUNKS=8` (8MB/tukda), `ZIP_WORKERS=4`; retries 8, timeout 90s. `main.yml` me bhi yahi values.
- Agar GitHub secrets/variables ya host env me `ZIP_WORKERS=10` ya `ZIP_SEG_CHUNKS=2` set hai to use hata do/badlo.
