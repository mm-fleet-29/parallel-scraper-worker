"""Generic URL mirror shard (runs on a public Actions runner; this file is public).

Reads <manifest_base>/<shard>.jsonl ({"u": url, "k": key}) with the READ SAS, fetches each
URL with plain GET at a fixed per-shard rate, and PUTs the bytes to <dest_base>/<k> with the
WRITE SAS. Writes <dest_base>/_results/<shard>.jsonl. Logs counts and status codes only.

env: MEDIA_BLOB_READ_SAS, MEDIA_BLOB_WRITE_SAS (query strings or full SAS URLs)
usage: python mirror.py --manifest-base <blob prefix> --dest-base <blob prefix> --shard s00 --rate 0.8
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time

import requests

BLOB_HOST = ".blob.core.windows.net"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/145.0.0.0 Safari/537.36 PlaywrightResearchBot/1.0 (automated, read-only research)")
MAX_BYTES = 512 * 1024 * 1024


def _q(var: str) -> str:
    s = os.environ.get(var, "")
    if not s:
        raise SystemExit(f"{var} missing")
    return s.split("?", 1)[1] if "?" in s else s.lstrip("?")


def signed(url: str, q: str) -> str:
    return url if BLOB_HOST not in url or "sig=" in url else f"{url}?{q}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest-base", required=True)
    ap.add_argument("--dest-base", required=True)
    ap.add_argument("--shard", required=True)
    ap.add_argument("--rate", type=float, required=True, help="requests per second for this shard")
    a = ap.parse_args()
    rq, wq = _q("MEDIA_BLOB_READ_SAS"), _q("MEDIA_BLOB_WRITE_SAS")
    s = requests.Session()
    m = s.get(signed(f"{a.manifest_base}/{a.shard}.jsonl", rq), timeout=60)
    m.raise_for_status()
    items = [json.loads(l) for l in m.text.splitlines() if l.strip()]
    print(f"shard={a.shard} items={len(items)} rate={a.rate}")
    results, counts, next_t = [], {}, 0.0
    for it in items:
        wait = next_t - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        next_t = max(time.monotonic(), next_t) + 1.0 / a.rate
        row = {"u": it["u"], "k": it["k"], "t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        t0 = time.monotonic()
        try:
            r = s.get(it["u"], headers={"User-Agent": UA, "Accept": "*/*"}, timeout=120, stream=True, allow_redirects=True)
            row["status"] = r.status_code
            row["ct"] = r.headers.get("content-type")
            row["retry_after"] = r.headers.get("retry-after")
            body = b""
            for chunk in r.iter_content(1 << 20):
                body += chunk
                if len(body) > MAX_BYTES:
                    row["error"] = "too-large"; body = b""; break
            row["ms"] = int((time.monotonic() - t0) * 1000)
            if r.status_code == 200 and body:
                row["size"] = len(body)
                row["sha256"] = hashlib.sha256(body).hexdigest()
                p = s.put(signed(f"{a.dest_base}/{it['k']}", wq), data=body,
                          headers={"x-ms-blob-type": "BlockBlob", "Content-Type": row["ct"] or "application/octet-stream",
                                   "x-ms-meta-sha256": row["sha256"]}, timeout=300)
                row["put"] = p.status_code
            if r.status_code in (429, 403, 503):
                counts["strain"] = counts.get("strain", 0) + 1
                time.sleep(float(row["retry_after"] or 30))
        except Exception as e:
            row["error"] = type(e).__name__
        counts[str(row.get("status", "err"))] = counts.get(str(row.get("status", "err")), 0) + 1
        results.append(row)
        if counts.get("strain", 0) >= 3:
            print("stopping: repeated strain"); break
    body = ("\n".join(json.dumps(r) for r in results) + "\n").encode()
    p = s.put(signed(f"{a.dest_base}/_results/{a.shard}.jsonl", wq), data=body,
              headers={"x-ms-blob-type": "BlockBlob", "Content-Type": "application/x-ndjson"}, timeout=120)
    print(f"done shard={a.shard} fetched={len(results)} status_counts={counts} results_put={p.status_code}")
    return 0 if p.status_code in (200, 201) else 1


if __name__ == "__main__":
    sys.exit(main())
