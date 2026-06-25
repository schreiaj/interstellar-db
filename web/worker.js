// Web Worker: owns the WASM indexer and the IndexedDB store, off the main thread.
//
// Protocol: main posts { id, op, args }; worker replies { id, ok, result } or
// { id, ok: false, error }. The WASM side only computes keys / scan ranges and
// decodes candidates — all persistence is IndexedDB (see idb.js).

import init, { Indexer } from './pkg/interstellar_wasm.js';
import { openDB, putBatch, scanRanges, countAll, clearAll } from './idb.js';

let indexer = null;
let db = null;
let params = { maxRange: 1000, epochDuration: 3600 };

async function ensureReady() {
  if (!indexer) {
    await init(); // instantiate the wasm module (installs the panic hook)
    indexer = new Indexer(params.maxRange, params.epochDuration);
    db = await openDB();
  }
}

// Exact filters applied after the coarse Morton range scan — mirrors the native
// `SpatioTemporalStore::collect`: drop candidates outside the wall-clock window
// or outside the true sphere (ranges only bound the cube), then sort in time.
function filterAndSort(candidates, cx, cy, cz, radius, startSecs, endSecs) {
  const r2 = radius * radius;
  const out = [];
  for (const { key, value } of candidates) {
    const [x, y, z, secs, nanos] = indexer.decodeKey(key); // [x, y, z, secs, nanos]
    if (secs < startSecs || secs > endSecs) continue;
    const dx = x - cx, dy = y - cy, dz = z - cz;
    if (dx * dx + dy * dy + dz * dz > r2) continue;
    out.push({ x, y, z, secs, nanos, label: value?.label });
  }
  out.sort((a, b) => a.secs - b.secs || a.nanos - b.nanos);
  return out;
}

const ops = {
  async config({ maxRange, epochDuration }) {
    params = { maxRange, epochDuration };
    if (indexer) indexer = new Indexer(maxRange, epochDuration);
    return { ok: true };
  },

  // records: [{ x, y, z, secs, nanos?, label? }]
  async store({ records }) {
    const entries = records.map((r) => ({
      key: indexer.generateKey(r.x, r.y, r.z, r.secs, r.nanos ?? 0),
      value: { label: r.label ?? null },
    }));
    await putBatch(db, entries);
    return { stored: entries.length };
  },

  async queryEpoch({ cx, cy, cz, radius, secs }) {
    const ranges = indexer.epochRanges(cx, cy, cz, radius, secs);
    const candidates = await scanRanges(db, ranges);
    const results = filterAndSort(candidates, cx, cy, cz, radius, 0, Number.MAX_SAFE_INTEGER);
    return { results, scanned: candidates.length, ranges: ranges.length };
  },

  async queryWindow({ cx, cy, cz, radius, startSecs, endSecs }) {
    const ranges = indexer.windowRanges(cx, cy, cz, radius, startSecs, endSecs);
    const candidates = await scanRanges(db, ranges);
    const results = filterAndSort(candidates, cx, cy, cz, radius, startSecs, endSecs);
    return { results, scanned: candidates.length, ranges: ranges.length };
  },

  async count() {
    return { count: await countAll(db) };
  },

  async clear() {
    await clearAll(db);
    return { cleared: true };
  },
};

self.onmessage = async (e) => {
  const { id, op, args } = e.data;
  try {
    await ensureReady();
    const fn = ops[op];
    if (!fn) throw new Error(`unknown op: ${op}`);
    const result = await fn(args ?? {});
    self.postMessage({ id, ok: true, result });
  } catch (err) {
    self.postMessage({ id, ok: false, error: String(err?.stack ?? err) });
  }
};
