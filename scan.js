const fs = require("fs");
const path = require("path");

const RPC = process.env.RPC || "https://rpc.elysium.kinetiq.xyz";
const MIN_TX = Number(process.env.MIN_TX || 3);
const OUT = process.env.OUT || "elysium";
const TO_BLOCK = process.env.TO_BLOCK ? Number(process.env.TO_BLOCK) : 0;
const BATCH = Number(process.env.BATCH || 20);
const PARALLEL = Number(process.env.PARALLEL || 6);
const TIME_LIMIT = Number(process.env.TIME_LIMIT_MIN || 320) * 60000;
const SYSTEM = new Set(["0x00000000000000000000000000000000000a4b05", "0x0000000000000000000000000000000000000000"]);
const started = Date.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let id = 1;
async function rpc(body, tries = 8) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(RPC, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
      if (r.status === 429 || r.status >= 500) throw new Error("http " + r.status);
      const j = await r.json();
      return j;
    } catch (e) {
      await sleep(Math.min(30000, 500 * 2 ** i));
    }
  }
  throw new Error("RPC failed after retries");
}

let batchOk = true;
async function blocks(from, to) {
  const nums = [];
  for (let n = from; n <= to; n++) nums.push(n);
  if (batchOk) {
    const body = nums.map((n) => ({ jsonrpc: "2.0", id: id++, method: "eth_getBlockByNumber", params: ["0x" + n.toString(16), true] }));
    const res = await rpc(body);
    if (Array.isArray(res) && res.every((x) => x && x.result !== undefined)) {
      const byId = new Map(res.map((x) => [x.id, x.result]));
      return body.map((b) => byId.get(b.id));
    }
    batchOk = false;
    console.log("Batch calls not supported, switching to single calls");
  }
  const out = [];
  for (const n of nums) {
    const j = await rpc({ jsonrpc: "2.0", id: id++, method: "eth_getBlockByNumber", params: ["0x" + n.toString(16), true] });
    if (!j || j.error) throw new Error("block " + n + " " + JSON.stringify(j && j.error));
    out.push(j.result);
  }
  return out;
}

function loadState() {
  const f = path.join(OUT, "state.txt");
  const s = { next: 0, target: 0, counts: new Map(), txs: 0 };
  if (!fs.existsSync(f)) return s;
  const lines = fs.readFileSync(f, "utf8").split("\n");
  const head = JSON.parse(lines[0]);
  s.next = head.next; s.target = head.target; s.txs = head.txs || 0;
  for (let i = 1; i < lines.length; i++) {
    const l = lines[i];
    if (!l) continue;
    const k = l.indexOf(",");
    s.counts.set(l.slice(0, k), Number(l.slice(k + 1)));
  }
  return s;
}

function saveState(s) {
  fs.mkdirSync(OUT, { recursive: true });
  const parts = [JSON.stringify({ next: s.next, target: s.target, txs: s.txs, saved: new Date().toISOString() })];
  for (const [a, c] of s.counts) parts.push(a + "," + c);
  fs.writeFileSync(path.join(OUT, "state.txt"), parts.join("\n") + "\n");
}

function finish(s) {
  const list = [...s.counts].filter(([, c]) => c >= MIN_TX).sort((a, b) => b[1] - a[1]);
  const buckets = [[3, 4], [5, 9], [10, 19], [20, 49], [50, 99], [100, 499], [500, Infinity]];
  const dist = buckets.map(([lo, hi]) => ({ range: hi === Infinity ? lo + "+" : lo + "-" + hi, wallets: list.filter(([, c]) => c >= lo && c <= hi).length }));
  const summary = {
    network: "Elysium mainnet",
    rpc: RPC,
    snapshotBlock: s.target,
    minTx: MIN_TX,
    eligibleWallets: list.length,
    allSendingWallets: s.counts.size,
    countedTransactions: s.txs,
    distribution: dist,
    finishedAt: new Date().toISOString()
  };
  fs.writeFileSync(path.join(OUT, "summary.json"), JSON.stringify(summary, null, 1));
  fs.writeFileSync(path.join(OUT, "wallets.csv"), "address,transactions\n" + list.map(([a, c]) => a + "," + c).join("\n") + "\n");
  console.log(JSON.stringify(summary, null, 1));
}

(async () => {
  const s = loadState();
  if (!s.target) {
    if (TO_BLOCK) s.target = TO_BLOCK;
    else {
      const j = await rpc({ jsonrpc: "2.0", id: id++, method: "eth_blockNumber", params: [] });
      s.target = parseInt(j.result, 16);
    }
    console.log("Snapshot block:", s.target);
  }
  let lastSave = Date.now();
  while (s.next <= s.target) {
    if (Date.now() - started > TIME_LIMIT) {
      saveState(s);
      console.log("Time limit reached at block " + s.next + ". Run the workflow again to continue.");
      fs.writeFileSync(path.join(OUT, "progress.txt"), "Scanned up to block " + (s.next - 1) + " of " + s.target + "\n");
      return;
    }
    const jobs = [];
    for (let p = 0; p < PARALLEL && s.next <= s.target; p++) {
      const from = s.next;
      const to = Math.min(s.target, from + BATCH - 1);
      s.next = to + 1;
      jobs.push(blocks(from, to));
    }
    const res = await Promise.all(jobs);
    for (const list of res) {
      for (const b of list) {
        if (!b) throw new Error("missing block");
        for (const tx of b.transactions || []) {
          const type = parseInt(tx.type || "0x0", 16);
          if (type >= 0x64) continue;
          const from = (tx.from || "").toLowerCase();
          if (!from || SYSTEM.has(from)) continue;
          s.counts.set(from, (s.counts.get(from) || 0) + 1);
          s.txs++;
        }
      }
    }
    if (Date.now() - lastSave > 600000) {
      saveState(s);
      lastSave = Date.now();
      console.log("Progress: block " + (s.next - 1) + " / " + s.target + ", wallets so far " + s.counts.size);
    }
  }
  saveState(s);
  fs.writeFileSync(path.join(OUT, "progress.txt"), "Done. Scanned all blocks up to " + s.target + "\n");
  finish(s);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
