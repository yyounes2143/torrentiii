import EventEmitter from 'events';
import { Readable } from 'stream';
import WebTorrent from 'webtorrent';

const MAX_BUFFERED_PIECES = 120;   // High-watermark: stop downloading
const LOW_WATERMARK_PIECES = 30;  // Low-watermark: resume downloading

export class SlidingWindowStore extends EventEmitter {
  chunkLength: number;
  length: number;
  totalPieces: number;
  _chunks: Map<number, Buffer>;
  uploadHead: number;
  _waiters: Map<number, Function[]>;
  _torrent: any;
  jobId: string;

  constructor(chunkLength: number, storeOpts: any, jobId: string) {
    super();
    this.chunkLength = chunkLength;
    this.length = storeOpts.length || 0;
    this.totalPieces = Math.ceil(this.length / chunkLength);
    this._chunks = new Map();
    this.uploadHead = 0;
    this._waiters = new Map();
    this.jobId = jobId;
  }

  put(index: number, buffer: Buffer, cb?: (err: Error | null) => void) {
    // Silently drop pieces that are behind the upload frontier
    if (index < this.uploadHead) {
      if (cb) process.nextTick(() => cb(null));
      return;
    }

    // We no longer reject pieces here to avoid crashing WebTorrent's scheduler.
    // Instead, we strictly control piece selection in createSequentialReadable.
    this._chunks.set(index, Buffer.from(buffer)); // defensive copy

    if (cb) process.nextTick(() => cb(null));

    // Notify any waiter that was blocked on this piece.
    const waiters = this._waiters.get(index);
    if (waiters) {
      this._waiters.delete(index);
      for (const resolve of waiters) resolve();
    }

    this.emit('piece', index);

    // Emit pressure event when we hit the high watermark
    if (this._chunks.size >= MAX_BUFFERED_PIECES) {
      this.emit('pressure');
    }
  }

  get(index: number, opts: any, cb: (err: Error | null, buf?: Buffer) => void) {
    if (typeof opts === 'function') { cb = opts; opts = {}; }
    const buf = this._chunks.get(index);
    if (!buf) {
      if (cb) process.nextTick(() => cb(new Error(`piece ${index} not in store`)));
      return;
    }
    const offset = opts?.offset || 0;
    const length = opts?.length || (buf.length - offset);
    if (cb) process.nextTick(() => cb(null, buf.slice(offset, offset + length)));
  }

  prune(index: number) {
    this._chunks.delete(index);
    if (this._chunks.size <= LOW_WATERMARK_PIECES) {
      this.emit('drain');
    }
  }

  waitForPiece(index: number) {
    if (this._chunks.has(index)) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const list = this._waiters.get(index) || [];
      list.push(resolve);
      this._waiters.set(index, list);
    });
  }

  close(cb?: (err: Error | null) => void) {
    this._chunks.clear();
    this._waiters.clear();
    if (cb) process.nextTick(() => cb(null));
  }

  destroy(cb?: (err: Error | null) => void) {
    this.close(cb);
  }
}

export class BackpressureController {
  torrent: WebTorrent.Torrent;
  store: SlidingWindowStore;
  _throttled: boolean;

  constructor(torrent: WebTorrent.Torrent, store: SlidingWindowStore) {
    this.torrent = torrent;
    this.store = store;
    this._throttled = false;

    store.on('pressure', () => this._throttle());
    store.on('drain',    () => this._unthrottle());
  }

  _throttle() {
    if (this._throttled) return;
    this._throttled = true;
    console.log(`[backpressure ${this.store.jobId}] 🔴 Window full – waiting to drain, but torrent NOT paused to avoid deadlock`);
  }

  _unthrottle() {
    if (!this._throttled) return;
    this._throttled = false;
    console.log(`[backpressure ${this.store.jobId}] 🟢 Window drained`);
  }

  onNewWire(wire: any) {
    // nothing needed here if pause() handles it
  }
}

import fs from 'fs';

export function createSequentialReadable(file: WebTorrent.TorrentFile, store: SlidingWindowStore) {
  fs.appendFileSync('debug.log', `[DEBUG] createSequentialReadable called. Store type: ${store ? store.constructor.name : 'null'}, Keys: ${Object.keys(store || {})}\n`);
  const pieceLength = store.chunkLength;
  const fileStart   = file.offset;
  const fileEnd     = fileStart + file.length - 1;
  const firstPiece  = Math.floor(fileStart / pieceLength);
  const lastPiece   = Math.floor(fileEnd   / pieceLength);

  let currentPiece = firstPiece;

  const readable = new Readable({
    highWaterMark: pieceLength * 4,
    read() {
      (async () => {
        if (currentPiece > lastPiece) {
          this.push(null);
          return;
        }

        store.uploadHead = currentPiece;

        try {
          const torrent = store._torrent;
          if (torrent) {
             const endPiece = Math.min(lastPiece, currentPiece + 250);
             if ((torrent as any).select) {
                (torrent as any).select(currentPiece, endPiece, false);
             }
             if ((torrent as any).critical) {
                (torrent as any).critical(currentPiece, currentPiece + 2);
             }
          }
        } catch (_) {}

        await store.waitForPiece(currentPiece);

        const buf = store._chunks.get(currentPiece);
        if (!buf) {
          this.destroy(new Error(`piece ${currentPiece} vanished from store`));
          return;
        }

        let sliceStart = 0;
        let sliceEnd   = buf.length;

        if (currentPiece === firstPiece) {
          sliceStart = fileStart % pieceLength;
        }
        if (currentPiece === lastPiece) {
          sliceEnd = ((fileEnd % pieceLength) + 1) || buf.length;
        }

        const chunk = buf.slice(sliceStart, sliceEnd);
        
        // DO NOT prune the last piece of this file since the next file
        // likely needs it if they share a piece boundary.
        if (currentPiece !== lastPiece) {
          store.prune(currentPiece);
        }

        currentPiece++;
        this.push(chunk);

      })().catch(err => this.destroy(err));
    }
  });

  return readable;
}
