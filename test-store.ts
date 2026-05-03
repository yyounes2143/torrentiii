import WebTorrent from "webtorrent";

class PrunableMemoryStore {
  chunkLength: number;
  length: number;
  chunks: Map<number, Buffer>;
  closed: boolean;
  
  constructor(chunkLength: number, storeOpts: { length: number }) {
    this.chunkLength = chunkLength;
    this.length = storeOpts.length;
    this.chunks = new Map();
    this.closed = false;
  }
  put(index: number, buf: Buffer, cb?: (err?: Error | null) => void) {
    if (this.closed) { if (cb) cb(new Error("closed")); return; }
    this.chunks.set(index, buf);
    if (cb) cb(null);
  }
  get(index: number, opts?: any, cb?: (err: Error | null, buf?: Buffer) => void) {
    if (typeof opts === 'function') {
      cb = opts;
      opts = null;
    }
    if (this.closed) { if (cb) cb(new Error("closed")); return; }
    let buf = this.chunks.get(index);
    if (!buf) { if (cb) cb(new Error("Chunk not found")); return; }
    const offset = opts?.offset || 0;
    const length = opts?.length || (buf.length - offset);
    if (cb) cb(null, buf.slice(offset, offset + length));
  }
  close(cb?: (err?: Error | null) => void) {
    this.closed = true;
    this.chunks.clear();
    if (cb) cb(null);
  }
  destroy(cb?: (err?: Error | null) => void) {
    this.closed = true;
    this.chunks.clear();
    if (cb) cb(null);
  }
  pruneUpTo(byteOffset: number) {
    const maxIndexToPrune = Math.floor(byteOffset / this.chunkLength) - 1;
    for (const key of this.chunks.keys()) {
      if (key <= maxIndexToPrune) {
        this.chunks.delete(key);
      }
    }
  }
}

const client = new WebTorrent();

const magnet = "magnet:?xt=urn:btih:08ada5a7a6183aae1e09d831df6748d566095a10&dn=Sintel&tr=udp%3A%2F%2Fexplodie.org%3A6969&tr=udp%3A%2F%2Ftracker.coppersurfer.tk%3A6969&tr=udp%3A%2F%2Ftracker.empire-js.us%3A1337&tr=udp%3A%2F%2Ftracker.leechers-paradise.org%3A6969&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=wss%3A%2F%2Ftracker.btorrent.xyz&tr=wss%3A%2F%2Ftracker.fastcast.nz&tr=wss%3A%2F%2Ftracker.openwebtorrent.com";

client.add(magnet, { store: function (chunkLength: any, storeOpts: any) { return new PrunableMemoryStore(chunkLength, storeOpts) as any; } }, (torrent) => {
  console.log("Torrent ready!");
  torrent.on("download", () => {
    console.log("Downloaded:", torrent.downloaded);
  });
  torrent.on("done", () => {
    console.log("Torrent done!");
    client.destroy();
  });
});
