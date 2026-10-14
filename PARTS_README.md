# Users DB — partitioned search (no localhost, no credit card)

This splits the 117 GB `users_data.zip` into **N self-contained parts**. You host
one part per account, and the EdgeOne function queries them all. Everything runs
in the cloud — nothing depends on your PC.

## Why parts

The DB is **~1.71 billion records** (446 GB uncompressed, 261 bytes/record).

| index        | raw     | gzipped |
|--------------|---------|---------|
| mobile (full record) + alt chain | ~446 GB | ~120 GB |
| email → mobile | ~68 GB | ~18 GB |
| name/fname → mobile | ~171 GB | ~45 GB |
| **total** | | **~185 GB** |

No single free account should hold 185 GB, so we split by **record order** into
N parts. A key property of record-order splitting:

> a record's mobile, email and name all land in the **same part**.

So a lookup only needs:
* **mobile** → fan out to all parts, read one small block, done.
* **email / name** → fan out, get the mobile, then read that **one** part's mobile block.

## Bucket file format (why queries are small)

Each `*.bin` is sorted by key and split into fixed blocks, each block gzip'd,
preceded by a tiny uncompressed directory (`firstKey → gz offset/len`). The API
**binary-searches the directory over HTTP Range** and fetches exactly one block —
a query moves tens of KB, not megabytes, and each part is only ~100 files.

```
DBS1 | field | maxKeyLen | numEntries | numBlocks |
   { klen, firstKey, gzOffset, gzLen, cap } * numBlocks |
   gzip(block) gzip(block) ...
```

## Build (cloud, e.g. Google Colab)

```python
# 1) get the source url (a fresh direct link to users_data.zip)
# 2) install gh if you'll upload to GitHub:
!apt-get -qq install -y gh

import os
os.environ.update(
    SOURCE   = "https://.../users_data.zip",   # direct link, Range-capable
    PARTS    = "10",                            # = number of accounts
    EST_TOTAL= "1710000000",
    MOB_BUCKETS  = "64",
    MAIL_BUCKETS = "64",
    NAME_BUCKETS = "64",
    BLOCK    = "2048",
    OUT      = "./out",
    # upload each part to its own account, then delete it locally:
    UPLOAD_CMDS = '["gh release upload v1 {dir}/* --repo acct1/repo --clobber",'
                  '"gh release upload v1 {dir}/* --repo acct2/repo --clobber",'
                  '"...one entry per part..."]',
    DELETE_AFTER_UPLOAD = "1",
)

!python parts_builder.py
```

Notes:
* staging is gzip'd (`GZIP_STAGE=1`), so peak disk is modest (a few GB/part).
* `UPLOAD_CMDS` is a JSON array, one command per part → different accounts.
* if you don't upload inline, run without `UPLOAD_CMDS` and upload `./out/part-XXX`
  yourself.

### Upload examples

GitHub (one repo/account per part):
```
gh release create v1 --repo acctN/repo
gh release upload v1 ./out/part-00N/* --repo acctN/repo --clobber
```
base URL: `https://github.com/acctN/repo/releases/download/v1`

Internet Archive (one item per part, needs the `ia` CLI + free account):
```
ia upload users-db-part-00N ./out/part-00N/*
```
base URL: `https://archive.org/download/users-db-part-00N`

## Wire the API

Edit `edgeone-users-data/parts-edge.js` and set `DEFAULT_PART_BASES` (or set the
`PART_BASES` env var in the EdgeOne dashboard as a JSON array), in **part order**:

```js
const DEFAULT_PART_BASES = [
  'https://github.com/acct1/repo1/releases/download/v1',
  'https://github.com/acct2/repo2/releases/download/v1',
  // ... one per part, index 0 = part-000 ...
];
```
Also make sure `MOB_BUCKETS` / `MAIL_BUCKETS` / `NAME_BUCKETS` match the build.

## Deploy

Put `parts-edge.js` as the EdgeOne function entry (it exports `handleRequest`,
`onRequest` and `fetch`). Routes:

* `GET /` — friend-facing search page
* `GET /api/search?mobile=…` · `?email=…` · `?name=…`
* `GET /api/info`
* optional `API_KEY` env gates `/api/search`

## Tuning

| knob | effect |
|------|--------|
| `PARTS` | more parts = smaller per-account size, more fan-out per query |
| `*_BUCKETS` | more buckets = smaller bucket files, more files per part |
| `BLOCK` | entries per gzip block (bigger = fewer, larger range fetches) |

## Files

* `parts_builder.py` — streaming partitioner / index builder
* `edgeone-users-data/parts-edge.js` — EdgeOne query handler
* `edgeone-users-data/test-parts.mjs` — end-to-end test (builds parts, serves them
  on two ports, runs the real handler)
