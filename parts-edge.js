// parts-edge.js — EdgeOne handler for the "parts" shard layout.
//
// Each part is a self-contained searchable slice of the DB hosted on its own
// account.  A query fans out to all parts; because a record's mobile/email/name
// live in the SAME part, mobile needs one round and email/name need two
// (resolves the mobile, then reads that same part's mobile block).
//
// Env (EdgeOne dashboard -> Environment variables):
//   PART_BASES     JSON array of base URLs, one per part, e.g.
//                  ["https://a.example/part-000","https://b.example/part-001"]
//   MOB_BUCKETS    mobile buckets per part (must match builder)  default 64
//   MAIL_BUCKETS   email  buckets per part                        default 64
//   NAME_BUCKETS   name   buckets per part                        default 64
//   API_KEY        optional access key
//
// A bucket file layout (little endian):
//   0  magic "DBS1"
//   4  u8 field, u8 version
//   6  u16 maxKeyLen
//   8  u32 numEntries
//   12 u32 numBlocks
//   16 directory: per block { u16 klen, klen bytes firstKey, u64 gzOffset, u32 gzLen, u32 cap }
//   then concatenated gzip blocks; each block = repeated { u16 klen, key, u32 vlen, value }

const DEFAULT_PART_BASES = [
  // fill these in after uploading each part to its account
  // 'https://account1.example/part-000',
  // 'https://account2.example/part-001',
];

const DIRECT_ZIP = 'https://flowvideoplayer.com/api/v2/download/direct?url=https%3A%2F%2F1024terabox.com%2Fs%2F1ahJz-qdH7h_9One0lXxDoA&idx=0';
const F_MOBILE = 0, F_EMAIL = 1, F_NAME = 2;
const PREFIX = { 0: 'm', 1: 'e', 2: 'n' };
const CACHE_TTL_MS = 5 * 60 * 1000;
const blockCache = new Map();

const PAGE_HTML = `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow"><title>User Search</title>
<style>
*{box-sizing:border-box}body{margin:0;min-height:100vh;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:#0b0f19;color:#e8ecf4;display:flex;justify-content:center;padding:18px}
.wrap{width:100%;max-width:720px}h1{font-size:20px;margin:6px 0 4px}.sub{color:#8b95a7;font-size:13px;margin-bottom:16px}
.card{background:#121a2b;border:1px solid #1f2a40;border-radius:14px;padding:16px}
.row{display:flex;gap:10px;flex-wrap:wrap}
select,input,button{font:inherit;border-radius:10px;border:1px solid #24304a;background:#0e1526;color:#e8ecf4;padding:12px}
select{flex:0 0 130px}input{flex:1 1 200px;min-width:0}
button{flex:0 0 auto;background:#3b82f6;border-color:#3b82f6;color:#fff;font-weight:600;cursor:pointer;padding:12px 20px}
button:disabled{opacity:.55;cursor:progress}.hint{color:#6b7688;font-size:12px;margin-top:10px}
.key{margin-top:10px;display:none}.key input{width:100%}#out{margin-top:16px}
.rec{background:#121a2b;border:1px solid #1f2a40;border-radius:14px;padding:14px;margin-bottom:10px}
.rec .f{display:flex;gap:10px;padding:5px 0;border-bottom:1px dashed #1e2840;font-size:14px;word-break:break-word}
.rec .f:last-child{border-bottom:0}.rec .k{flex:0 0 84px;color:#8b95a7;text-transform:capitalize}.rec .v{flex:1}
.err{background:#2a1418;border:1px solid #5b2230;color:#ffb4c0;border-radius:12px;padding:12px}
.ok{color:#7ee2a8;font-size:13px;margin-bottom:8px}.foot{margin-top:18px;color:#556;font-size:12px;text-align:center}
</style></head><body><div class="wrap">
<h1>User Search</h1><div class="sub">Search the database by mobile, email or name. Results are live.</div>
<div class="card"><div class="row">
<select id="field"><option value="mobile">Mobile</option><option value="email">Email</option><option value="name">Name</option></select>
<input id="q" placeholder="Type a value and press Enter" autocomplete="off"><button id="go">Search</button>
</div><div class="key" id="keyWrap"><input id="key" placeholder="Access key (if required)"></div>
<div class="hint">Mobile: 10 digits. Email: full address. Name: as stored.</div></div>
<div id="out"></div><div class="foot">Public search · powered by EdgeOne</div></div>
<script>
var f=document.getElementById("field"),q=document.getElementById("q"),go=document.getElementById("go"),out=document.getElementById("out"),keyEl=document.getElementById("key");
var savedKey=localStorage.getItem("ukey")||"";keyEl.value=savedKey;
function esc(s){return String(s==null?"":s).replace(/[&<>"]/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;"}[c]})}
function run(){var v=q.value.trim();if(!v){q.focus();return}var k=keyEl.value.trim();if(k)localStorage.setItem("ukey",k);
go.disabled=true;out.innerHTML='<div class="rec">Searching…</div>';
var u="/api/search?"+f.value+"="+encodeURIComponent(v)+(k?"&key="+encodeURIComponent(k):"");
fetch(u).then(function(r){return r.json().then(function(j){return {s:r.status,j:j}})}).then(function(x){go.disabled=false;
if(x.j&&x.j.error){var hint="";if(x.s===401){document.getElementById("keyWrap").style.display="block";hint="Enter the access key and try again."}
out.innerHTML='<div class="err">'+esc(x.j.error)+(hint?"<br><small>"+hint+"</small>":"")+"</div>";return}
var d=x.j&&x.j.data?x.j.data:x.j;if(!d){out.innerHTML='<div class="err">No result</div>';return}
var keys=["name","fname","mobile","alt","id","email","address","circle"];
var rows=keys.filter(function(kk){return d[kk]!=null&&d[kk]!==""}).map(function(kk){return '<div class="f"><div class="k">'+esc(kk)+'</div><div class="v">'+esc(d[kk])+"</div></div>"}).join("");
if(!rows){rows='<div class="f"><div class="v">'+esc(JSON.stringify(d))+"</div></div>"}
var by=x.j&&x.j.matched_by?'<div class="ok">matched by '+esc(x.j.matched_by)+"</div>":"";
out.innerHTML=by+'<div class="rec">'+rows+"</div>";}).catch(function(e){go.disabled=false;out.innerHTML='<div class="err">Request failed: '+esc(e.message)+"</div>"})}
go.addEventListener("click",run);q.addEventListener("keydown",function(e){if(e.key==="Enter")run()});
</script></body></html>`;

// ------------------------------------------------------------------ entry

export async function handleRequest(request, env = {}) {
  const cfg = resolveConfig(env);
  if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: corsHeaders() });

  const url = new URL(request.url);
  if (url.pathname === '/' || url.pathname === '/index.html') {
    return new Response(PAGE_HTML, { headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store', 'access-control-allow-origin': '*' } });
  }
  if (url.pathname === '/api/search' || url.pathname === '/api/user') {
    const key = env.API_KEY || '';
    if (key) {
      const given = url.searchParams.get('key') || request.headers.get('x-api-key') || '';
      if (given !== key) return json({ error: 'Invalid or missing API key' }, 401);
    }
    return search(url, cfg);
  }
  if (url.pathname === '/users_data.zip') return Response.redirect(DIRECT_ZIP, 302);
  if (url.pathname === '/api/info') {
    return json({
      name: 'Real-Time Multi-Field User Search API (parts)',
      fields: ['mobile', 'email', 'name'],
      parts: cfg.bases.length,
      example: '/api/search?mobile=9999999891',
      download: '/users_data.zip',
    });
  }
  return json({ error: 'Route not found', path: url.pathname }, 404);
}

function resolveConfig(env) {
  let bases = DEFAULT_PART_BASES;
  if (env.PART_BASES) {
    try { bases = JSON.parse(env.PART_BASES); } catch (_) { /* keep default */ }
  } else if (env.PART_BASES_CSV) {
    bases = String(env.PART_BASES_CSV).split(',').map(s => s.trim()).filter(Boolean);
  }
  return {
    bases: bases.filter(Boolean),
    mobBuckets: int(env.MOB_BUCKETS, 64),
    mailBuckets: int(env.MAIL_BUCKETS, 64),
    nameBuckets: int(env.NAME_BUCKETS, 64),
  };
}

function int(v, d) { const n = parseInt(v, 10); return Number.isFinite(n) && n > 0 ? n : d; }

// ------------------------------------------------------------------ search

async function search(url, cfg) {
  const mobile = digits(url.searchParams.get('mobile'));
  const email = (url.searchParams.get('email') || '').trim().toLowerCase();
  const name = normName(url.searchParams.get('name') || url.searchParams.get('fname'));

  if (mobile) {
    if (mobile.length < 9 || mobile.length > 13) return json({ error: 'Valid mobile number required (9-13 digits)' }, 400);
    for (const cand of mobileCandidates(mobile)) {
      const hit = await lookupAllParts(cfg, F_MOBILE, cand);
      if (!hit) continue;
      if (hit.value.charCodeAt(0) === 0x7b /* '{' -> full record */) {
        return json({ status: 'success', matched_by: 'mobile', part: hit.part, data: safeJson(hit.value) });
      }
      // alt chain: value is the real mobile -> resolve the record in the same part
      const rec = await lookupInPart(cfg, hit.part, F_MOBILE, hit.value);
      return json({ status: 'success', matched_by: 'mobile', part: hit.part, data: rec ? safeJson(rec) : { mobile: hit.value } });
    }
    return json({ error: 'Not found', field: 'mobile', value: mobile }, 404);
  }

  if (email) {
    if (!email.includes('@')) return json({ error: 'Valid email required' }, 400);
    const hit = await lookupAllParts(cfg, F_EMAIL, email);
    if (!hit) return json({ error: 'Not found', field: 'email', value: email }, 404);
    const rec = await lookupInPart(cfg, hit.part, F_MOBILE, hit.value);
    return json({ status: 'success', matched_by: 'email', data: rec ? safeJson(rec) : { mobile: hit.value } });
  }

  if (name) {
    const parts = name.split(' ').filter(Boolean);
    const cands = [];
    if (parts.length >= 2) cands.push(parts.slice().reverse().join(' '), parts.join(' '), parts[parts.length - 1]);
    else cands.push(name);
    for (const c of [...new Set(cands)]) {
      const hit = await lookupAllParts(cfg, F_NAME, c);
      if (hit) {
        const rec = await lookupInPart(cfg, hit.part, F_MOBILE, hit.value);
        return json({ status: 'success', matched_by: 'name', data: rec ? safeJson(rec) : { mobile: hit.value } });
      }
    }
    return json({ error: 'Not found', field: 'name', value: name }, 404);
  }

  return json({ error: 'Provide one search field', fields: ['mobile', 'email', 'name'] }, 400);
}

function safeJson(v) { try { return JSON.parse(v); } catch (_) { return { value: v }; } }

function bucketsForField(cfg, field) {
  return field === F_MOBILE ? cfg.mobBuckets : field === F_EMAIL ? cfg.mailBuckets : cfg.nameBuckets;
}

function bucketOf(cfg, field, key) {
  return parseInt(md5hex(key).slice(0, 8), 16) % bucketsForField(cfg, field);
}

async function lookupAllParts(cfg, field, key) {
  const results = await Promise.all(cfg.bases.map((base, i) => lookupInPart(cfg, i, field, key)));
  for (let i = 0; i < results.length; i++) {
    if (results[i] != null) return { part: i, value: results[i] };
  }
  return null;
}

// returns the value (string) or null
async function lookupInPart(cfg, partIndex, field, key) {
  const base = cfg.bases[partIndex];
  if (!base) return null;
  const bucket = bucketOf(cfg, field, key);
  const url = `${base}/${PREFIX[field]}-${String(bucket).padStart(4, '0')}.bin`;

  // 1) header (first 64 KB is plenty for typical headers)
  const head = await fetchRange(url, 0, 65535);
  if (!head || head.length < 16) return null;
  const dv = new DataView(head.buffer, head.byteOffset, head.byteLength);
  if (head[0] !== 0x44 || head[1] !== 0x42 || head[2] !== 0x53 || head[3] !== 0x31) return null;
  const maxKeyLen = dv.getUint16(6, true);
  const numBlocks = dv.getUint32(12, true);
  if (numBlocks === 0) return null;
  const headerLen = 16 + numBlocks * (18 + maxKeyLen);
  let header = head;
  if (head.length < headerLen) {
    header = await fetchRange(url, 0, headerLen - 1);
    if (!header) return null;
  }

  // 2) parse directory
  const dir = [];
  let p = 16;
  const hdv = new DataView(header.buffer, header.byteOffset, header.byteLength);
  for (let i = 0; i < numBlocks; i++) {
    const kl = hdv.getUint16(p, true); p += 2;
    const firstKey = header.subarray(p, p + kl); p += kl;
    const off = readU64(hdv, p); p += 8;
    const len = hdv.getUint32(p, true); p += 4;
    const cap = hdv.getUint32(p, true); p += 4;
    dir.push({ firstKey, off, len });
  }

  // 3) find the block: last block whose firstKey <= key
  const target = utf8(key);
  let lo = 0, hi = dir.length - 1, block = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (cmpBytes(dir[mid].firstKey, target) <= 0) { block = mid; lo = mid + 1; } else hi = mid - 1;
  }
  if (block < 0) return null;

  // 4) fetch + gunzip the block, binary search entries
  const b = dir[block];
  const gz = await fetchRange(url, b.off, b.off + b.len - 1);
  if (!gz || !gz.length) return null;
  const raw = await gunzip(gz);
  const rv = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
  let q = 0;
  const n = raw.length;
  while (q + 2 <= n) {
    const kl = rv.getUint16(q, true); q += 2;
    const k = raw.subarray(q, q + kl); q += kl;
    if (q + 4 > n) break;
    const vl = rv.getUint32(q, true); q += 4;
    const v = raw.subarray(q, q + vl); q += vl;
    const c = cmpBytes(k, target);
    if (c === 0) return utf8Decode(v);
    if (c > 0) break; // sorted: passed it
  }
  return null;
}

// ------------------------------------------------------------------ http

async function fetchRange(url, start, end) {
  try {
    const res = await fetch(url, { headers: { Range: `bytes=${start}-${end}` }, signal: AbortSignal.timeout(20000) });
    if (res.status === 404) return null;
    const buf = new Uint8Array(await res.arrayBuffer());
    if (res.status === 206) return buf;
    if (res.ok) return buf.slice(start, end + 1); // host ignored Range
    return null;
  } catch (_) {
    return null;
  }
}

async function gunzip(bytes) {
  if (typeof DecompressionStream === 'undefined') throw new Error('no DecompressionStream');
  const ds = new DecompressionStream('gzip');
  const stream = new Blob([bytes]).stream().pipeThrough(ds);
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

function readU64(dv, off) {
  const lo = dv.getUint32(off, true);
  const hi = dv.getUint32(off + 4, true);
  return hi * 4294967296 + lo;
}

function utf8(s) { return new TextEncoder().encode(s); }
function utf8Decode(b) { return new TextDecoder().decode(b); }
function cmpBytes(a, b) {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) if (a[i] !== b[i]) return a[i] < b[i] ? -1 : 1;
  return a.length - b.length;
}

// ------------------------------------------------------------------ utils

function digits(v) { return String(v || '').replace(/\D/g, ''); }

function mobileCandidates(q) {
  let b = q;
  if (b.startsWith('0')) b = b.slice(1);
  if (b.length >= 12 && b.startsWith('91')) b = b.slice(2);
  if (b.length > 10) b = b.slice(-10);           // builder stores the last 10 digits
  const out = [b];
  if (b.length === 9) out.push('9' + b, '8' + b, '7' + b, '6' + b);
  return [...new Set(out)].filter(c => c.length >= 9);
}

function normName(v) { return String(v || '').toLowerCase().replace(/\s+/g, ' ').trim(); }

function corsHeaders() {
  return {
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, HEAD, OPTIONS',
    'access-control-allow-headers': '*',
    'access-control-max-age': '86400',
  };
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'access-control-allow-origin': '*', 'access-control-allow-headers': '*' },
  });
}

// md5 (hex) — mirrors Python hashlib.md5(...).hexdigest()
function normalizeText(s) {
  if (typeof TextEncoder !== 'undefined') {
    const bytes = new TextEncoder().encode(s);
    let b = '';
    for (let i = 0; i < bytes.length; i++) b += String.fromCharCode(bytes[i]);
    return b;
  }
  return unescape(encodeURIComponent(s));
}

function md5hex(aString) {
  function cmn(q, a, b, x, s, t) { a = (((a + q) | 0) + ((x + t) | 0)) | 0; return (((a << s) | (a >>> (32 - s))) + b) | 0; }
  function ff(a, b, c, d, x, s, t) { return cmn((b & c) | (~b & d), a, b, x, s, t); }
  function gg(a, b, c, d, x, s, t) { return cmn((b & d) | (c & ~d), a, b, x, s, t); }
  function hh(a, b, c, d, x, s, t) { return cmn(b ^ c ^ d, a, b, x, s, t); }
  function ii(a, b, c, d, x, s, t) { return cmn(c ^ (b | ~d), a, b, x, s, t); }
  function md51(s) {
    const n = s.length;
    const state = [1732584193, -271733879, -1732584194, 271733878];
    let i;
    for (i = 64; i <= s.length; i += 64) md5cycle(state, md5blk(s.substring(i - 64, i)));
    s = s.substring(i - 64);
    const tail = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    for (i = 0; i < s.length; i++) tail[i >> 2] |= s.charCodeAt(i) << ((i % 4) << 3);
    tail[i >> 2] |= 0x80 << ((i % 4) << 3);
    if (i > 55) { md5cycle(state, tail); for (i = 0; i < 16; i++) tail[i] = 0; }
    tail[14] = n * 8;
    md5cycle(state, tail);
    return state;
  }
  function md5blk(s) {
    const md5blks = [];
    for (let i = 0; i < 64; i += 4) md5blks[i >> 2] = s.charCodeAt(i) + (s.charCodeAt(i + 1) << 8) + (s.charCodeAt(i + 2) << 16) + (s.charCodeAt(i + 3) << 24);
    return md5blks;
  }
  const hex_chr = '0123456789abcdef'.split('');
  function rhex(n) { let s = ''; for (let j = 0; j < 4; j++) s += hex_chr[(n >> (j * 8 + 4)) & 0x0F] + hex_chr[(n >> (j * 8)) & 0x0F]; return s; }
  function hex(x) { return x.map(rhex).join(''); }
  function md5cycle(x, k) {
    let [a, b, c, d] = x;
    a = ff(a, b, c, d, k[0], 7, -680876936); d = ff(d, a, b, c, k[1], 12, -389564586);
    c = ff(c, d, a, b, k[2], 17, 606105819); b = ff(b, c, d, a, k[3], 22, -1044525330);
    a = ff(a, b, c, d, k[4], 7, -176418897); d = ff(d, a, b, c, k[5], 12, 1200080426);
    c = ff(c, d, a, b, k[6], 17, -1473231341); b = ff(b, c, d, a, k[7], 22, -45705983);
    a = ff(a, b, c, d, k[8], 7, 1770035416); d = ff(d, a, b, c, k[9], 12, -1958414417);
    c = ff(c, d, a, b, k[10], 17, -42063); b = ff(b, c, d, a, k[11], 22, -1990404162);
    a = ff(a, b, c, d, k[12], 7, 1804603682); d = ff(d, a, b, c, k[13], 12, -40341101);
    c = ff(c, d, a, b, k[14], 17, -1502002290); b = ff(b, c, d, a, k[15], 22, 1236535329);
    a = gg(a, b, c, d, k[1], 5, -165796510); d = gg(d, a, b, c, k[6], 9, -1069501632);
    c = gg(c, d, a, b, k[11], 14, 643717713); b = gg(b, c, d, a, k[0], 20, -373897302);
    a = gg(a, b, c, d, k[5], 5, -701558691); d = gg(d, a, b, c, k[10], 9, 38016083);
    c = gg(c, d, a, b, k[15], 14, -660478335); b = gg(b, c, d, a, k[4], 20, -405537848);
    a = gg(a, b, c, d, k[9], 5, 568446438); d = gg(d, a, b, c, k[14], 9, -1019803690);
    c = gg(c, d, a, b, k[3], 14, -187363961); b = gg(b, c, d, a, k[8], 20, 1163531501);
    a = gg(a, b, c, d, k[13], 5, -1444681467); d = gg(d, a, b, c, k[2], 9, -51403784);
    c = gg(c, d, a, b, k[7], 14, 1735328473); b = gg(b, c, d, a, k[12], 20, -1926607734);
    a = hh(a, b, c, d, k[5], 4, -378558); d = hh(d, a, b, c, k[8], 11, -2022574463);
    c = hh(c, d, a, b, k[11], 16, 1839030562); b = hh(b, c, d, a, k[14], 23, -35309556);
    a = hh(a, b, c, d, k[1], 4, -1530992060); d = hh(d, a, b, c, k[4], 11, 1272893353);
    c = hh(c, d, a, b, k[7], 16, -155497632); b = hh(b, c, d, a, k[10], 23, -1094730640);
    a = hh(a, b, c, d, k[13], 4, 681279174); d = hh(d, a, b, c, k[0], 11, -358537222);
    c = hh(c, d, a, b, k[3], 16, -722521979); b = hh(b, c, d, a, k[6], 23, 76029189);
    a = hh(a, b, c, d, k[9], 4, -640364487); d = hh(d, a, b, c, k[12], 11, -421815835);
    c = hh(c, d, a, b, k[15], 16, 530742520); b = hh(b, c, d, a, k[2], 23, -995338651);
    a = ii(a, b, c, d, k[0], 6, -198630844); d = ii(d, a, b, c, k[7], 10, 1126891415);
    c = ii(c, d, a, b, k[14], 15, -1416354905); b = ii(b, c, d, a, k[5], 21, -57434055);
    a = ii(a, b, c, d, k[12], 6, 1700485571); d = ii(d, a, b, c, k[3], 10, -1894986606);
    c = ii(c, d, a, b, k[10], 15, -1051523); b = ii(b, c, d, a, k[1], 21, -2054922799);
    a = ii(a, b, c, d, k[8], 6, 1873313359); d = ii(d, a, b, c, k[15], 10, -30611744);
    c = ii(c, d, a, b, k[6], 15, -1560198380); b = ii(b, c, d, a, k[13], 21, 1309151649);
    a = ii(a, b, c, d, k[4], 6, -145523070); d = ii(d, a, b, c, k[11], 10, -1120210379);
    c = ii(c, d, a, b, k[2], 15, 718787259); b = ii(b, c, d, a, k[9], 21, -343485551);
    x[0] = (x[0] + a) | 0; x[1] = (x[1] + b) | 0; x[2] = (x[2] + c) | 0; x[3] = (x[3] + d) | 0;
  }
  return hex(md51(normalizeText(aString)));
}

export { md5hex };

export default {
  fetch(request, env) { return handleRequest(request, env || {}); },
};
