import os
import sys
import time
import requests

url = os.environ.get("GROBID_URL", "http://grobid:8070").rstrip("/") + "/api/isalive"
print(f"[wait_for_grobid] Waiting for GROBID at {url}...")

for i in range(120):  # up to 10 minutes
    try:
        r = requests.get(url, timeout=3)
        if r.ok:
            print(f"[wait_for_grobid] GROBID is alive: {r.text}")
            sys.exit(0)
    except Exception:
        pass
    time.sleep(5)

print("[wait_for_grobid] GROBID did not become ready in time.")
sys.exit(1)