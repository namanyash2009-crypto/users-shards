"""
parts_builder.py  —  split the 117 GB users_data.zip into N self-contained,
searchable PARTS (one part per hosting account).  Zero localhost dependency.

Run this on a cloud runner (Colab / Kaggle / any Linux box).  It streams the
source once, never materialises the whole thing in RAM, and writes one part at
a time to ./out/part-XXX/ .  After each part is written you upload that folder
to whichever account you like; then delete it.  A tiny routing file ties the
parts together for the API.

What each part contains
-----------------------
  m-<bucket>.bin   mobile index  -> full record JSON
  e-<bucket>.bin   email  index  -> mobile
  n-<bucket>.bin   name   index  -> mobile  (name + fname)
  meta.json

Every *.bin is:
  sorted by key, split into fixed BLOCK-sized blocks, each block gzip'd,
  preceded by a small uncompressed directory of (firstKey, gzOffset, gzLen).
The API binary-searches the directory over HTTP Range, then fetches + gunzips
exactly one block.  Query payload = a few hundred KB, not megabytes.

Config (env vars)
-----------------
  SOURCE          url to the zip  OR  local path to a .zip / .json
  PARTS           number of parts ( = number of accounts ), default 10
  EST_TOTAL       estimated total records, default 1710000000
  MOB_BUCKETS     mobile buckets per part,  default 64
  MAIL_BUCKETS    email  buckets per part,  default 64
  NAME_BUCKETS    name   buckets per part,  default 64
  LIMIT           stop after N records (testing), default 0 = no limit
  OUT             output dir, default ./out
  INDEX_ALT       1 = also index the alt phone (default 1)
"""
import os
import re
import io
import sys
import json
import time
import gzip
import zlib
import shutil
import struct
import hashlib
import zipfile
import subprocess
import urllib.request

# ----------------------------------------------------------------- config
SOURCE = os.environ.get("SOURCE", "").strip()
PARTS = int(os.environ.get("PARTS", "10"))
EST_TOTAL = int(os.environ.get("EST_TOTAL", "1710000000"))
MOB_BUCKETS = int(os.environ.get("MOB_BUCKETS", "64"))
MAIL_BUCKETS = int(os.environ.get("MAIL_BUCKETS", "64"))
NAME_BUCKETS = int(os.environ.get("NAME_BUCKETS", "64"))
LIMIT = int(os.environ.get("LIMIT", "0"))
OUT = os.environ.get("OUT", "./out")
INDEX_ALT = os.environ.get("INDEX_ALT", "1") != "0"
GZIP_STAGE = os.environ.get("GZIP_STAGE", "1") != "0"
# Optional: run a shell command after each part is finalised (upload it).
# Placeholders: {part} = 000,001,...  {dir} = path to the part folder.
# UPLOAD_CMD      -> one template used for every part
# UPLOAD_CMDS     -> JSON array of templates, one per part (different accounts)
UPLOAD_CMD = os.environ.get("UPLOAD_CMD", "").strip()
UPLOAD_CMDS = []
if os.environ.get("UPLOAD_CMDS"):
    UPLOAD_CMDS = json.loads(os.environ["UPLOAD_CMDS"])
elif UPLOAD_CMD:
    UPLOAD_CMDS = [UPLOAD_CMD]
DELETE_AFTER_UPLOAD = os.environ.get("DELETE_AFTER_UPLOAD", "0") == "1"

BLOCK = int(os.environ.get("BLOCK", "2048"))       # entries per gzip block
PART_RECORDS = max(1, (EST_TOTAL + PARTS - 1) // PARTS)
HEADER_OFFSET = 93

F_MOBILE, F_EMAIL, F_NAME = 0, 1, 2
FIELD_BUCKETS = {F_MOBILE: MOB_BUCKETS, F_EMAIL: MAIL_BUCKETS, F_NAME: NAME_BUCKETS}
FIELD_PREFIX = {F_MOBILE: "m", F_EMAIL: "e", F_NAME: "n"}

# ----------------------------------------------------------------- helpers

def md5hex(s):
    return hashlib.md5(s.encode("utf-8", "ignore")).hexdigest()


def bucket_of(field, key):
    return int(md5hex(key)[:8], 16) % FIELD_BUCKETS[field]


def canon_mobile(v):
    d = re.sub(r"\D", "", str(v or ""))
    if len(d) > 10:
        d = d[-10:]
    return d


def canon_email(v):
    s = str(v or "").strip().lower()
    return s if ("@" in s and len(s) >= 5) else ""


def canon_name(v):
    return " ".join(str(v or "").lower().split())


# ----------------------------------------------------------------- source

def iter_source_bytes():
    """Yield decompressed bytes of the JSON array (the zip payload)."""
    src = SOURCE
    if not src:
        sys.exit("Set SOURCE to the zip url / local path.")

    # local file?
    if os.path.exists(src):
        if src.lower().endswith(".json"):
            with open(src, "rb") as f:
                while True:
                    c = f.read(1 << 20)
                    if not c:
                        break
                    yield c
            return
        # local zip -> use zipfile (handles the header for us)
        with zipfile.ZipFile(src) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                while True:
                    c = f.read(1 << 20)
                    if not c:
                        break
                    yield c
        return

    # remote zip: read the local file header to find the payload offset, inflate
    hreq = urllib.request.Request(src, headers={"User-Agent": "Mozilla/5.0",
                                                "Range": "bytes=0-4095"})
    with urllib.request.urlopen(hreq, timeout=60) as hr:
        head = hr.read()
    if head[:4] != b"PK\x03\x04":
        sys.exit("Remote source is not a zip (or Range unsupported).")
    fn_len = struct.unpack("<H", head[26:28])[0]
    ex_len = struct.unpack("<H", head[28:30])[0]
    offset = 30 + fn_len + ex_len
    print("payload offset in zip: %d  (entry %r)" % (offset, head[30:30 + fn_len].decode("utf-8", "ignore")))

    req = urllib.request.Request(src, headers={
        "User-Agent": "Mozilla/5.0",
        "Range": "bytes=%d-" % offset,
    })
    d = zlib.decompressobj(-zlib.MAX_WBITS)
    with urllib.request.urlopen(req, timeout=180) as resp:
        while True:
            c = resp.read(4 << 20)
            if not c:
                break
            out = d.decompress(c)
            if out:
                yield out
    tail = d.flush()
    if tail:
        yield tail


def iter_records():
    """Brace-depth scanner over the byte stream -> raw JSON object strings."""
    buf = ""
    pos = 0
    depth = 0
    start = -1
    for chunk in iter_source_bytes():
        buf += chunk.decode("utf-8", "ignore")
        n = len(buf)
        while pos < n:
            ch = buf[pos]
            if depth == 0:
                if ch == "{":
                    start = pos
                    depth = 1
                pos += 1
            else:
                if ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        yield buf[start:pos + 1]
                        start = -1
                pos += 1
        # keep only the current incomplete record; do NOT re-count its '{'
        if start >= 0:
            buf = buf[start:]
            start = 0
            pos = 1
        else:
            buf = ""
            pos = 0


# ----------------------------------------------------------------- part build

class Part:
    """Buffers one part on disk: one staging file per (field, bucket)."""

    def __init__(self, index):
        self.index = index
        self.dir = os.path.join(OUT, "part-%03d" % index)
        self.stage = os.path.join(OUT, "_stage-%03d" % index)
        os.makedirs(self.stage, exist_ok=True)
        self.h = {}          # (field, bucket) -> open file handle
        self.count = 0
        self.bucket_count = {F_MOBILE: [0] * MOB_BUCKETS,
                             F_EMAIL: [0] * MAIL_BUCKETS,
                             F_NAME: [0] * NAME_BUCKETS}

    def handle(self, field, bucket):
        k = (field, bucket)
        f = self.h.get(k)
        if f is None:
            ext = ".txt.gz" if GZIP_STAGE else ".txt"
            path = os.path.join(self.stage, "%s-%04d%s" % (FIELD_PREFIX[field], bucket, ext))
            if GZIP_STAGE:
                f = gzip.open(path, "wt", encoding="utf-8", compresslevel=1)
            else:
                f = open(path, "w", encoding="utf-8")
            self.h[k] = f
        return f

    def add(self, field, key, value):
        f = self.handle(field, bucket_of(field, key))
        f.write(key)
        f.write("\t")
        f.write(value)
        f.write("\n")
        self.bucket_count[field][bucket_of(field, key)] += 1

    def close_handles(self):
        for f in self.h.values():
            f.close()
        self.h.clear()

    # ---- finalise: sort each bucket, block-gzip, write <prefix>-<bucket>.bin
    def finalize(self):
        self.close_handles()
        os.makedirs(self.dir, exist_ok=True)
        total = {F_MOBILE: 0, F_EMAIL: 0, F_NAME: 0}
        for field in (F_MOBILE, F_EMAIL, F_NAME):
            n = FIELD_BUCKETS[field]
            for b in range(n):
                base = os.path.join(self.stage, "%s-%04d" % (FIELD_PREFIX[field], b))
                path = base + (".txt.gz" if GZIP_STAGE else ".txt")
                if not os.path.exists(path):
                    continue
                out = os.path.join(self.dir, "%s-%04d.bin" % (FIELD_PREFIX[field], b))
                cnt = write_bucket(path, out, field)
                os.remove(path)
                total[field] += cnt
        os.rmdir(self.stage)
        with open(os.path.join(self.dir, "meta.json"), "w", encoding="utf-8") as f:
            json.dump({"part": self.index, "records": self.count,
                       "mobile": total[F_MOBILE], "email": total[F_EMAIL],
                       "name": total[F_NAME]}, f)
        return total


def write_bucket(stage_path, out_path, field):
    """Read `key\tvalue` lines, sort by key, write a block-gzip bucket file."""
    entries = []
    opener = gzip.open if stage_path.endswith(".gz") else open
    with opener(stage_path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            k, _, v = line.partition("\t")
            entries.append((k, v))
    entries.sort(key=lambda kv: kv[0].encode("utf-8"))
    n = len(entries)
    if n == 0:
        return 0

    max_key = max(len(k.encode("utf-8")) for k, _ in entries)
    blocks = []          # (firstKeyBytes, gzBytes)
    for i in range(0, n, BLOCK):
        chunk = entries[i:i + BLOCK]
        raw = bytearray()
        for k, v in chunk:
            kb = k.encode("utf-8")
            vb = v.encode("utf-8")
            raw += struct.pack("<H", len(kb)) + kb + struct.pack("<I", len(vb)) + vb
        blocks.append((chunk[0][0].encode("utf-8"), gzip.compress(bytes(raw), 6)))

    header = bytearray()
    header += b"DBS1"
    header += struct.pack("<BBH", field, 1, max_key)
    header += struct.pack("<II", n, len(blocks))
    body = bytearray()
    body_off = 16 + sum(2 + len(fk) + 16 for fk, _ in blocks)
    # directory
    for fk, gz in blocks:
        header += struct.pack("<H", len(fk)) + fk
        header += struct.pack("<Q", body_off)
        header += struct.pack("<I", len(gz))
        header += struct.pack("<I", BLOCK)      # declared capacity (count not exact for tail)
        body_off += len(gz)
    # note: last block may have < BLOCK entries; that's fine, reader stops at eof of block
    with open(out_path, "wb") as o:
        o.write(header)
        for _, gz in blocks:
            o.write(gz)
    return n


def finish_part(part, t0, manifest):
    t = part.finalize()
    print("[%6.1f min] part-%03d done | records %s | mob %s mail %s name %s"
          % ((time.time() - t0) / 60, part.index, f"{part.count:,}",
             f"{t[F_MOBILE]:,}", f"{t[F_EMAIL]:,}", f"{t[F_NAME]:,}"), flush=True)
    manifest.append({"part": part.index, "dir": os.path.basename(part.dir),
                     "records": part.count, "mobile": t[F_MOBILE],
                     "email": t[F_EMAIL], "name": t[F_NAME]})
    if UPLOAD_CMDS:
        tmpl = UPLOAD_CMDS[part.index] if part.index < len(UPLOAD_CMDS) \
            else UPLOAD_CMDS[part.index % len(UPLOAD_CMDS)]
        cmd = tmpl.replace("{part}", "%03d" % part.index).replace("{dir}", part.dir)
        print("    upload: " + cmd, flush=True)
        rc = subprocess.run(cmd, shell=True).returncode
        if rc != 0:
            print("    upload command exited %d (part kept on disk)" % rc, flush=True)
        elif DELETE_AFTER_UPLOAD:
            shutil.rmtree(part.dir, ignore_errors=True)
    return t


# ----------------------------------------------------------------- main

def main():
    t0 = time.time()
    os.makedirs(OUT, exist_ok=True)
    print("source       : %s" % (SOURCE or "<unset>"))
    print("parts        : %d  (~%s records each)" % (PARTS, f"{PART_RECORDS:,}"))
    print("buckets/part : mob=%d mail=%d name=%d   block=%d"
          % (MOB_BUCKETS, MAIL_BUCKETS, NAME_BUCKETS, BLOCK))
    print()

    part = Part(0)
    nparts = 0
    total = 0
    corrupt = 0
    manifest = []
    for raw in iter_records():
        if LIMIT and total >= LIMIT:
            break
        try:
            rec = json.loads(raw)
        except Exception:
            corrupt += 1
            continue
        if not isinstance(rec, dict):
            corrupt += 1
            continue

        mob = canon_mobile(rec.get("mobile"))
        alt = canon_mobile(rec.get("alt")) if INDEX_ALT else ""
        em = canon_email(rec.get("email"))
        nm = canon_name(rec.get("name"))
        fn = canon_name(rec.get("fname"))
        recjson = json.dumps(rec, ensure_ascii=False, separators=(",", ":"))

        if mob:
            part.add(F_MOBILE, mob, recjson)
            if alt and alt != mob:
                # chain, not a duplicate of the full record: alt -> mobile
                part.add(F_MOBILE, alt, mob)
        if mob and em:
            part.add(F_EMAIL, em, mob)
        if mob and nm:
            part.add(F_NAME, nm, mob)
        if mob and fn and fn != nm:
            part.add(F_NAME, fn, mob)

        part.count += 1
        total += 1

        if part.count >= PART_RECORDS:
            finish_part(part, t0, manifest)
            nparts += 1
            part = Part(part.index + 1)

        if total % 200000 == 0:
            el = time.time() - t0
            print("[%6.1f min] %s records | %.0f rec/s | %s"
                  % (el / 60, f"{total:,}", total / el if el else 0,
                     os.path.basename(part.dir)), flush=True)

    if part.count:
        finish_part(part, t0, manifest)
        nparts += 1

    try:
        with open(os.path.join(OUT, "parts.json"), "w", encoding="utf-8") as f:
            json.dump({"parts": manifest}, f, indent=2)
    except OSError:
        pass

    print("\n=== DONE ===")
    print("records : %s" % f"{total:,}")
    print("parts   : %d  (files in %s/part-XXX/)" % (nparts, OUT))
    print("corrupt : %s" % f"{corrupt:,}")
    print("elapsed : %.1f min" % ((time.time() - t0) / 60))
    print("\nNext: upload each out/part-XXX to its own account, then fill routes.json")


if __name__ == "__main__":
    main()
