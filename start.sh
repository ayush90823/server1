#!/bin/bash
# 1) Tunnel chalao
cloudflared tunnel --url http://localhost:8000 --no-autoupdate > tunnel.log 2>&1 &

# 2) Tunnel ka URL milne ka wait karo (max 60 sec)
URL=""
for i in $(seq 1 60); do
  URL=$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' tunnel.log | head -1)
  [ -n "$URL" ] && break
  sleep 1
done

if [ -z "$URL" ]; then
  echo "Tunnel URL nahi mila!"; cat tunnel.log; exit 1
fi

echo "Tunnel URL: $URL"
export ORIGIN_URL="$URL"

# BASE_URL = Worker ka fixed URL (GitHub Secret se aata hai), yahan change nahi karna
python main.py
