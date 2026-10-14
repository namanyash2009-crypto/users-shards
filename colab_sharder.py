"""
Google Colab Sharding Pipeline (Streams 117GB directly to GitHub Releases)
Run this inside a Google Colab notebook. Zero PC disk, zero PC bandwidth.
"""
import urllib.request
import zlib
import json
import os
import subprocess
from collections import defaultdict

# Upstream direct worker link
UPSTREAM_URL = "https://white-unit-5eae.conipy.workers.dev/dlink-fetch?token=8A3J8BxlX9eiEuBKvj2azBOmnKNb2NmzqscFjmpWRaf9b3GmCdBJMn_8QjQ14LZN04nMNlJhBF1W7a2c9PuAr3h6vdAeUS8GRr2uXcYJkz0DKXogjIvzrKV2MsQHG6udlKBfWk1Dkoz_g04OaccMykZ11R34fPbCzr7psiX9KVhNZbxqNNaKDuUE9ZAtg7Ex2F4G0FiG4n_Ip6-ONBMCkZGxY7vv6kBQi-nG7GUqNu1gwqgTbc9S-S3cXJk8RBDqUovl6A-PPeLxhXF2tTOxSQuZGhVZk8BCDUyUQWJBKF2dbWW48hhlerYGABCeBneHAva1BcVra748FM584idCvMgd3BUGUWik8vinVU5w9KoDcXoODxOcR4TYhGagW65PhaMKZXZzvHTir77g&file_name=users_data.zip"

GITHUB_REPO = "namanyash2009-crypto/users-shards"
RELEASE_TAG = "v1.0"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")

# ZIP local header is 93 bytes for users_data.json
HEADER_OFFSET = 93

def upload_shard(prefix, records):
    filename = f"shard_{prefix}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(records, f, separators=(',', ':'))
    
    # Upload asset to GitHub Release via gh CLI or curl
    cmd = [
        "gh", "release", "upload", RELEASE_TAG, filename,
        "--repo", GITHUB_REPO, "--clobber"
    ]
    subprocess.run(cmd)
    try:
        os.remove(filename)
    except OSError:
        pass

def main():
    print("Starting direct stream from worker URL...")
    req = urllib.request.Request(UPSTREAM_URL, headers={
        "User-Agent": "Mozilla/5.0",
        "Range": f"bytes={HEADER_OFFSET}-"
    })
    
    shards = defaultdict(list)
    d = zlib.decompressobj(-zlib.MAX_WBITS)
    buf = ""
    chunk_size = 1024 * 1024 * 8 # 8 MB chunks
    processed_count = 0
    
    with urllib.request.urlopen(req) as resp:
        while True:
            chunk = resp.read(chunk_size)
            if not chunk:
                break
            
            decomp = d.decompress(chunk).decode("utf-8", errors="ignore")
            buf += decomp
            
            # Extract individual JSON objects
            while True:
                start = buf.find('{')
                if start == -1:
                    buf = ""
                    break
                
                end = buf.find('}', start)
                if end == -1:
                    buf = buf[start:]
                    break
                
                obj_str = buf[start:end+1]
                buf = buf[end+1:]
                
                try:
                    user = json.loads(obj_str)
                    mob = user.get("mobile", "").strip()
                    if len(mob) >= 10:
                        prefix = mob[:4]
                        shards[prefix].append(user)
                        processed_count += 1
                        
                        # Flush large shards
                        if len(shards[prefix]) >= 5000:
                            upload_shard(prefix, shards[prefix])
                            shards[prefix] = []
                except json.JSONDecodeError:
                    continue
            
            if processed_count % 50000 == 0 and processed_count > 0:
                print(f"Processed {processed_count:,} records so far...")
    
    # Flush remaining
    for prefix, records in shards.items():
        if records:
            upload_shard(prefix, records)
            
    print(f"Done! All records partitioned and pushed to {GITHUB_REPO}.")

if __name__ == "__main__":
    main()
