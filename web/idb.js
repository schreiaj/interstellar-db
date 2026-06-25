// Minimal IndexedDB KV layer for the spatio-temporal store.
//
// The only thing that makes this work as a spatial store: IndexedDB accepts
// binary keys (ArrayBuffer / typed arrays) and orders them by *byte comparison*
// — the exact lexicographic order the 20-byte Morton+time keys are designed for.
// That lets a Morton range become a single `IDBKeyRange.bound(start, end)` scan.

const DB_NAME = 'interstellar';
const STORE = 'observations';

function promisify(req) {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function txDone(tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}

export function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, 1);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE)) {
        // Out-of-line keys: we supply the 20-byte key explicitly on every put.
        db.createObjectStore(STORE);
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

// entries: [{ key: Uint8Array(20), value: any }]
export function putBatch(db, entries) {
  const tx = db.transaction(STORE, 'readwrite');
  const store = tx.objectStore(STORE);
  for (const { key, value } of entries) store.put(value, key);
  return txDone(tx);
}

// ranges: Array<[Uint8Array, Uint8Array]> (inclusive start, inclusive end).
// Returns [{ key: Uint8Array, value: any }] across every range.
export async function scanRanges(db, ranges) {
  const tx = db.transaction(STORE, 'readonly');
  const store = tx.objectStore(STORE);
  const out = [];
  // All requests are issued synchronously below (before any await), so they
  // share this one read transaction.
  await Promise.all(
    ranges.map(async ([start, end]) => {
      const range = IDBKeyRange.bound(start, end, false, false);
      const [keys, values] = await Promise.all([
        promisify(store.getAllKeys(range)),
        promisify(store.getAll(range)),
      ]);
      for (let i = 0; i < keys.length; i++) {
        // Binary keys come back as ArrayBuffer; normalize to a byte view.
        out.push({ key: new Uint8Array(keys[i]), value: values[i] });
      }
    }),
  );
  return out;
}

export function countAll(db) {
  const tx = db.transaction(STORE, 'readonly');
  return promisify(tx.objectStore(STORE).count());
}

export function clearAll(db) {
  const tx = db.transaction(STORE, 'readwrite');
  tx.objectStore(STORE).clear();
  return txDone(tx);
}
