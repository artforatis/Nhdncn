const fs = require("fs");
const path = require("path");

const RPCS = (process.env.RPCS || "https://rpc.elysium.kinetiq.xyz").split(",").map((s) => s.trim()).filter(Boolean);
const MIN_TX = Number(process.env.MIN_TX || 3);
const OUT = process.env.OUT || "elysium";
const CONC = Number(process.env.CONC || 12);
const SYSTEM = new Set(["0x00000000000000000000000000000000000a4b05", "0x0000000000000000000000000000000000000000"]);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const mode = process.argv[2] || "shard";

let id = 1, rr = 0;
async function call(method, params) {
  for (let i = 0; i < 40; i++) {
    const url = RPCS[rr++ % RPCS.length];
    try {
      const ctl = new AbortController();
      const t = setTimeout(() => ctl.abort(), 30000);
      const r = await fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: id++, method, params }), signal: ctl.signal });
      clearTimeout(t);
      if (r.status === 429 || r.status >= 500) throw new Error("http " + r.status);
      const j = await r.json();
      if (j.error) throw new Error(JSON.stringify(j.error));
      return j.result;
    } catch (e) {
      await sleep(Math.min(30000, 300 * 2 ** Math.min(i, 7)) + Math.random() * 500);
    }
  }
  throw new Error("RPC failed: " + method + " " + JSON.stringify(params));
}

async function target() {
  if (process.env.TO_BLOCK) return Number(process.env.TO_BLOCK);
  return parseInt(await call("eth_blockNumber", []), 16);
}

async function shard() {
  const T = Number(process.env.TARGET);
  const S = Number(process.env.SHARD), N = Number(process.env.SHARDS);
  const from = Math.floor((T + 1) * S / N), to = Math.floor((T + 1) * (S + 1) / N) - 1;
  console.log("Shard " + S + " of " + N + ": blocks " + from + " to " + to);
  const counts = new Map();
  let next = from, done = 0, txs = 0;
  const t0 = Date.now();
  let lastLog = t0;
  async function worker() {
    while (true) {
      const n = next++;
      if (n > to) return;
      const b = await call("eth_getBlockByNumber", ["0x" + n.toString(16), true]);
      if (b) for (const tx of b.transactions || []) {
        const type = parseInt(tx.type || "0x0", 16);
        if (type >= 0x64) continue;
        const f = (tx.from || "").toLowerCase();
        if (!f || SYSTEM.has(f)) continue;
        counts.set(f, (counts.get(f) || 0) + 1);
        txs++;
      }
      done++;
      if (Date.now() - lastLog > 60000) {
        lastLog = Date.now();
        const rate = done / ((Date.now() - t0) / 1000);
        console.log("Progress " + done + "/" + (to - from + 1) + " blocks, " + rate.toFixed(0) + " blocks/s, about " + Math.round((to - from + 1 - done) / rate / 60) + " min left");
      }
    }
  }
  await Promise.all(Array.from({ length: CONC }, worker));
  fs.mkdirSync(OUT, { recursive: true });
  const lines = [JSON.stringify({ shard: S, from, to, txs })];
  for (const [a, c] of counts) lines.push(a + "," + c);
  fs.writeFileSync(path.join(OUT, "part-" + S + ".txt"), lines.join("\n") + "\n");
  console.log("Shard done: " + counts.size + " wallets, " + txs + " transactions");
}

function merge() {
  const dir = process.env.PARTS || "parts";
  const counts = new Map();
  let txs = 0, parts = 0;
  const files = [];
  (function walk(d) { for (const f of fs.readdirSync(d)) { const p = path.join(d, f); if (fs.statSync(p).isDirectory()) walk(p); else if (/^part-\d+\.txt$/.test(f)) files.push(p); } })(dir);
  for (const f of files) {
    const lines = fs.readFileSync(f, "utf8").split("\n");
    txs += JSON.parse(lines[0]).txs;
    parts++;
    for (let i = 1; i < lines.length; i++) {
      const l = lines[i];
      if (!l) continue;
      const k = l.indexOf(",");
      const a = l.slice(0, k);
      counts.set(a, (counts.get(a) || 0) + Number(l.slice(k + 1)));
    }
  }
  const list = [...counts].filter(([, c]) => c >= MIN_TX).sort((a, b) => b[1] - a[1]);
  const buckets = [[3, 4], [5, 9], [10, 19], [20, 49], [50, 99], [100, 499], [500, Infinity]];
  const summary = {
    network: "Elysium mainnet",
    snapshotBlock: Number(process.env.TARGET),
    minTx: MIN_TX,
    shardsMerged: parts,
    eligibleWallets: list.length,
    allSendingWallets: counts.size,
    countedTransactions: txs,
    distribution: buckets.map(([lo, hi]) => ({ range: hi === Infinity ? lo + "+" : lo + "-" + hi, wallets: list.filter(([, c]) => c >= lo && c <= hi).length })),
    finishedAt: new Date().toISOString()
  };
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, "summary.json"), JSON.stringify(summary, null, 1));
  fs.writeFileSync(path.join(OUT, "wallets.csv"), "address,transactions\n" + list.map(([a, c]) => a + "," + c).join("\n") + "\n");
  console.log(JSON.stringify(summary, null, 1));
}

(async () => {
  if (mode === "target") {
    const t = await target();
    console.log("Snapshot block: " + t);
    if (process.env.GITHUB_OUTPUT) fs.appendFileSync(process.env.GITHUB_OUTPUT, "target=" + t + "\n");
  } else if (mode === "merge") merge();
  else await shard();
})().catch((e) => { console.error(e); process.exit(1); });
