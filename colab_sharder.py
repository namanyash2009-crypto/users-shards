"""
Google Colab Sharding Pipeline (Streams 117GB directly to GitHub Releases)
Run this inside a Google Colab notebook. Zero PC disk, zero PC bandwidth.

Indexes built:
  - mobile / alt  -> shard_<4-digit-prefix>.json   (full records)
  - id            -> id_<4-digit-prefix>.json      (full records)
  - email         -> kw_email_<domain>.json        (chain -> mobile)
  - name / fname  -> kw_name_<md5[0:2]>.json       (chain -> mobile)
  - address       -> kw_addr_<md5[0:2]>.json       (chain -> mobile)
  - circle        -> kw_circle.json                (chain -> mobile)

GitHub release cap is 2GB per asset, so assets are split across
release tags v1.0, v1.1, v1.2, ... automatically.
"""
import urllib.request
import zlib
import re
import json
import os
import sys
import time
import hashlib
import threading
import subprocess
from collections import defaultdict

UPSTREAM_URL = "https://white-unit-5eae.conipy.workers.dev/dlink-fetch?token=8A3J8BxlX9eiEuBKvj2azBOmnKNb2NmzqscFjmpWRaf9b3GmCdBJMn_8QjQ14LZN04nMNlJhBF1W7a2c9PuAr3h6vdAeUS8GRr2uXcYJkz0DKXogjIvzrKV2MsQHG6udlKBfWk1Dkoz_g04OaccMykZ11R34fPbCzr7psiX9KVhNZbxqNNaKDuUE9ZAtg7Ex2F4G0FiG4n_Ip6-ONBMCkZGxY7vv6kBQi-nG7GUqNu1gwqgTbc9S-S3cXJk8RBDqUovl6A-PPeLxhXF2tTOxSQuZGhVZk8BCDUyUQWJBKF2dbWW48hhlerYGABCeBneHAva1BcVra748FM584idCvMgd3BUGUWik8vinVU5w9KoDcXoODxOcR4TYhGagW65PhaMKZXZzvHTir77g&file_name=users_data.zip"

GITHUB_REPO = "namanyash2009-crypto/users-shards"
HEADER_OFFSET = 93          # local file header size of users_data.json inside the zip
CHUNK_SIZE = 4 << 20        # 4 MB download chunks
FLUSH_EVERY_SECONDS = 300   # write partial shards every 5 min (crash safe)
SINGLE_FLUSH_AT = 8000      # flush a core shard as soon as it reaches this many records
KEYWORD_FLUSH_AT = 50000    # flush a keyword bucket at this size (max ~1.6MB json)

QSIZE = 1024
FIN = "__FIN__"

q_drv = threading.Queue(QSIZE)
q_out = threading.Queue(QSIZE)

# buffers
core = defaultdict(list)   # shard_XXXX / id_XXXX / alt_XXXX  (full records)
kmail = defaultdict(dict)  # kw_email_<domain>.json
kname = defaultdict(dict)  # kw_name_<hex>.json
kaddr = defaultdict(dict)  # kw_addr_<hex>.json
kcir = {}                  # kw_circle.json
meta = {}                  # id -> mobile (tiny, for id reverse lookups)

# asset split: file name -> (release tag, asset name)
assets = {}
upload_q = threading.Queue(QSIZE)
stats = {"records": 0, "core": 0, "corrupt": 0}
start_time = time.time()
last_flush = [time.time()]


def md5hex(s):
    return hashlib.md5(s.encode("utf-8", "ignore")).hexdigest()


def norm_name(v):
    return " ".join(str(v).lower().split())


def norm_addr(v):
    return " ".join(re.sub(r"[^a-z0-9]+", " ", str(v).lower()).split())


def norm_cir(v):
    return " ".join(str(v).upper().split())


# ------------------------------------------------------------------ github

def g(args, check=True):
    return subprocess.run(["gh"] + args, capture_output=True, text=True, check=check)


def ensure_github():
    r = g(["auth", "status"], check=False)
    if r.returncode != 0:
        tok = os.environ.get("GITHUB_TOKEN") or input("Paste a GitHub token with repo scope: ").strip()
        if not tok:
            sys.exit("No GitHub token.")
        p = subprocess.run(["gh", "auth", "login", "--with-token"], input=tok, text=True,
                           capture_output=True)
        if p.returncode != 0:
            sys.exit("GitHub login failed: " + (p.stderr or ""))


class ReleasePool:
    """One release tag per 1600 assets (keeps each asset well under the 2GB cap)."""

    def __init__(self):
        self.tags = {}

    def tag_for(self, group, n):
        key = (group, n // 1600)
        if key not in self.tags:
            tag = "v1.%d" % (len(self.tags) + 1)
            g(["release", "create", tag, "--repo", GITHUB_REPO,
               "--title", "shards " + tag, "--notes", "Auto generated shard batch"], check=False)
            self.tags[key] = tag
        return self.tags[key]

    def asset(self, group, fname, fpath):
        n = assets.get((group, fname))
        if n is None:
            n = assets[(group, fname)] = sum(1 for k in assets if k[0] == group)
        tag = self.tag_for(group, n)
        g(["release", "upload", tag, fpath, "--repo", GITHUB_REPO, "--clobber"], check=False)


pool = ReleasePool()


# ------------------------------------------------------------------ driver (stream -> decompress)

def driver():
    req = urllib.request.Request(UPSTREAM_URL, headers={
        "User-Agent": "Mozilla/5.0",
        "Range": "bytes=%d-" % HEADER_OFFSET,
    })
    d = zlib.decompressobj(-zlib.MAX_WBITS)
    with urllib.request.urlopen(req, timeout=120) as resp:
        while True:
            chunk = resp.read(CHUNK_SIZE)
            if not chunk:
                break
            try:
                q_drv.put(d.decompress(chunk))
            except Exception:
                break
        try:
            tail = d.flush()
            if tail:
                q_drv.put(tail)
        except Exception:
            pass
    q_drv.put(FIN)


def parse_driver():
    buf = ""
    while True:
        c = q_drv.get()
        if c == FIN:
            break
        buf += c.decode("utf-8", "ignore")
        # split complete JSON objects with a brace-depth scanner
        depth = 0
        start = -1
        i = 0
        n = len(buf)
        while i < n:
            ch = buf[i]
            if ch == "{":
                if depth == 0:
                    start = i
                depth += 1
            elif ch == "}":
                if depth:
                    depth -= 1
                    if depth == 0 and start >= 0:
                        q_out.put(buf[start:i + 1])
                        start = -1
            i += 1
        buf = buf[start:] if start >= 0 else ""
    q_out.put(FIN)


# ------------------------------------------------------------------ writer (index + bucket build)

def split_bucket(store, key, mobile):
    h = md5hex(key)
    b = store.setdefault(h[:2], {})
    b[key] = mobile


def write_core(name, rows):
    if not rows:
        return
    fname = name + ".json"
    tmp = fname + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, fname)
    upload_q.put((name, fname))


def write_kw(name, table):
    """table: {bucket_key: {key: mobile}} -> split buckets, cap each bucket at 50k keys"""
    for bk, inner in list(table.items()):
        if not inner:
            continue
        parts = [inner]
        if len(inner) > KEYWORD_FLUSH_AT:
            parts = []
            cur = {}
            for k, v in inner.items():
                cur[k] = v
                if len(cur) >= KEYWORD_FLUSH_AT:
                    parts.append(cur)
                    cur = {}
            if cur:
                parts.append(cur)
        for idx, part in enumerate(parts):
            fname = "%s_%s%s.json" % (name, bk, "" if idx == 0 else "_%d" % idx)
            tmp = fname + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(part, f, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp, fname)
            upload_q.put((name, fname))
        table[bk] = {}


def flush_all():
    for k in list(core):
        write_core(k, core.pop(k))
    write_kw("kw_email", kmail)
    write_kw("kw_name", kname)
    write_kw("kw_addr", kaddr)
    write_kw("kw_circle", {"c": kcir} if kcir else {})
    if kcir:
        kcir.clear()
    last_flush[0] = time.time()


def writer():
    while True:
        obj = q_out.get()
        if obj == FIN:
            break
        try:
            rec = json.loads(obj)
        except Exception:
            stats["corrupt"] += 1
            continue
        if not isinstance(rec, dict):
            stats["corrupt"] += 1
            continue

        mobile = str(rec.get("mobile", "")).strip()
        rid = re.sub(r"\D", "", str(rec.get("id", "")))

        # keyword buckets (chain -> mobile)
        nm = norm_name(rec.get("name", ""))
        fn = norm_name(rec.get("fname", ""))
        ad = norm_addr(rec.get("address", ""))
        ci = norm_cir(rec.get("circle", ""))
        em = str(rec.get("email", "")).strip().lower()

        if mobile and nm:
            split_bucket(kname, nm, mobile)
        if mobile and fn:
            split_bucket(kname, fn, mobile)
        if mobile and ad:
            split_bucket(kaddr, ad, mobile)
        if mobile and ci:
            kcir[ci] = mobile
        if mobile and em and "@" in em:
            dom = em.rsplit("@", 1)[1]
            loc = em.rsplit("@", 1)[0]
            b = kmail.setdefault(dom, {})
            b[loc] = mobile
            if len(b) >= KEYWORD_FLUSH_AT:
                write_kw("kw_email", {dom: b})
        if rid and mobile:
            meta[rid] = mobile

        # full-record shards
        if len(mobile) >= 9:
            core["shard_%s" % mobile[:4]].append(rec)
            alt = str(rec.get("alt", "")).strip()
            if alt and alt[:4] != mobile[:4] and len(alt) >= 9:
                core["alt_%s" % alt[:4]].append(rec)
            stats["core"] += 1
            if len(core["shard_%s" % mobile[:4]]) >= SINGLE_FLUSH_AT:
                write_core("shard_%s" % mobile[:4], core["shard_%s" % mobile[:4]])
                core["shard_%s" % mobile[:4]] = []
        if rid and len(rid) >= 4:
            core["id_%s" % rid[:4]].append(rec)

        stats["records"] += 1
        if stats["records"] % 100000 == 0:
            el = time.time() - start_time
            rate = stats["records"] / el if el else 0
            print("[%6.1f min] %,d records | %,d core shards | %,d corrupt | %.0f rec/s"
                  % (el / 60, stats["records"], stats["core"], stats["corrupt"], rate), flush=True)
            if len(meta) > 4000000:
                meta.clear()   # keep RAM low; meta is only a safety net

        if time.time() - last_flush[0] > FLUSH_EVERY_SECONDS:
            flush_all()


def uploader():
    while True:
        item = upload_q.get()
        if item == FIN:
            break
        group, fname = item
        pool.asset(group, fname, fname)
        try:
            os.remove(fname)
        except OSError:
            pass


def main():
    ensure_github()
    print("Upstream streaming from worker URL -> %s" % GITHUB_REPO, flush=True)

    threads = [
        threading.Thread(target=driver, daemon=True),
        threading.Thread(target=parse_driver, daemon=True),
        threading.Thread(target=writer, daemon=True),
        threading.Thread(target=uploader, daemon=True),
    ]
    for t in threads:
        t.start()

    # wait for writer (it consumes until FIN from parser)
    threads[2].join()
    flush_all()
    upload_q.put(FIN)
    threads[3].join()

    total = stats["records"]
    print("\n=== DONE ===")
    print("Records indexed : %,d" % total)
    print("Core shards     : %,d" % stats["core"])
    print("Corrupt skipped : %,d" % stats["corrupt"])
    print("Elapsed         : %.1f min" % ((time.time() - start_time) / 60))
    print("Releases used   : %d" % len(set(pool.tags.values())))
    print("Repo            : https://github.com/%s/releases" % GITHUB_REPO)


if __name__ == "__main__":
    main()
