import express from "express";
import { createServer } from "http";
import { Server } from "socket.io";
import { createServer as createViteServer } from "vite";
import path from "path";
import fs from "fs";
import { Transform } from "stream";
import { google } from "googleapis";
import WebTorrent from "webtorrent";
import { initializeApp } from "firebase/app";
import {
  getFirestore,
  collection,
  doc,
  setDoc,
  getDoc,
  updateDoc,
  addDoc,
  getDocs,
  query,
  where,
  serverTimestamp,
  deleteDoc,
} from "firebase/firestore";
import session from "express-session";
import cookieParser from "cookie-parser";
import dotenv from "dotenv";
import { fileURLToPath } from "url";
import axios from "axios";
import multer from "multer";
import { Storage } from "megajs";
import { SlidingWindowStore, BackpressureController, createSequentialReadable } from "./torrent-engine.js";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const firebaseConfig = JSON.parse(
  fs.readFileSync(path.join(__dirname, "firebase-applet-config.json"), "utf8")
);
process.env.GOOGLE_CLOUD_PROJECT = firebaseConfig.projectId;

process.on("uncaughtException", (err) => {
  console.error("Uncaught Exception:", err);
});
process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Rejection at:", promise, "reason:", reason);
});

const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp, firebaseConfig.firestoreDatabaseId);

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: { origin: "*" },
  // Increase ping timeouts for large uploads
  pingTimeout: 120000,
  pingInterval: 25000,
});

const upload = multer({ dest: "uploads/" });
const PORT = 3000;
const DOWNLOAD_DIR = path.join(process.cwd(), "downloads");
if (!fs.existsSync(DOWNLOAD_DIR)) {
  fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });
}

// ─── TRACKERS (extensive list for maximum speed) ────────────────────────────
const ANNOUNCE_LIST = [
  "http://tracker.opentrackr.org:1337/announce",
  "https://tracker.nanoha.org:443/announce",
  "https://tracker.tamersunion.org:443/announce",
  "http://tracker.openbittorrent.com:80/announce",
  "wss://tracker.openwebtorrent.com",
  "wss://tracker.btorrent.xyz",
  "wss://tracker.webtorrent.dev",
  "udp://tracker.opentrackr.org:1337/announce",
  "udp://9.rarbg.com:2810/announce",
  "udp://tracker.openbittorrent.com:6969/announce",
  "udp://open.demonii.com:1337/announce",
  "udp://exodus.desync.com:6969/announce",
  "udp://tracker.torrent.eu.org:451/announce",
  "udp://tracker.tiny-vps.com:6969/announce"
];


// Maps jobId -> WebTorrent.Torrent (active downloads)
const activeJobs = new Map<string, WebTorrent.Torrent>();
// Set of jobIds that should stop all activity
const cancelledJobs = new Set<string>();
// Per-job throttle: last time we wrote to Firestore
const lastUpdateTime = new Map<string, number>();

const THROTTLE_MS = 2000; // Write progress at most every 2 seconds

// Cache for user's Torrent folder id to save API calls
const torrentFolderCache = new Map<string, string>();

// ─── MEMORY STORES ───────────────────────────────────────────
const memoryStores = new Map<string, any>(); 

function createStoreFactory(jobId: string) {
  return function(chunkLength: number, storeOpts: any) {
    const store = new SlidingWindowStore(chunkLength, storeOpts, jobId);
    // Add internal ref for WebTorrent
    (store as any)._torrent = null;
    memoryStores.set(jobId, store);
    return store;
  };
}

// ─── HELPER: Update Firestore + emit socket (throttled for progress updates) ─
async function updateJob(jobId: string, data: any, force = false) {
  try {
    // For progress-only updates, throttle to avoid Firestore rate limits
    const isProgressUpdate =
      ("downloadProgress" in data || "uploadProgress" in data) &&
      !("status" in data);

    if (isProgressUpdate && !force) {
      const now = Date.now();
      const last = lastUpdateTime.get(jobId) ?? 0;
      if (now - last < THROTTLE_MS) {
        // Still emit via socket for live UI (cheap), skip Firestore write
        io.emit("jobUpdate", { id: jobId, ...data });
        return;
      }
      lastUpdateTime.set(jobId, now);
    }

    await updateDoc(doc(db, "jobs", jobId), {
      ...data,
      updatedAt: serverTimestamp(),
    });
    io.emit("jobUpdate", { id: jobId, ...data });
  } catch (error: any) {
    // If doc doesn't exist yet, try setDoc (race condition on job creation)
    if (error?.code === "not-found") {
      try {
        await setDoc(
          doc(db, "jobs", jobId),
          { ...data, updatedAt: serverTimestamp() },
          { merge: true }
        );
        io.emit("jobUpdate", { id: jobId, ...data });
      } catch (_) {}
    } else {
      console.error(`Error updating job ${jobId}:`, error?.message);
    }
  }
}

// ─── OAUTH2 HELPER ──────────────────────────────────────────────────────────
function getOAuthClient(redirectUri: string) {
  return new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    redirectUri
  );
}

// ─── REFRESH GOOGLE TOKENS ──────────────────────────────────────────────────
async function getValidTokens(userId: string, storedTokens: any) {
  const authClient = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET
  );
  authClient.setCredentials(storedTokens);

  // Force refresh if expiry is within 5 minutes or missing
  const expiryDate = storedTokens.expiry_date;
  const needsRefresh = !expiryDate || Date.now() > expiryDate - 5 * 60 * 1000;

  if (needsRefresh && storedTokens.refresh_token) {
    try {
      const { credentials } = await authClient.refreshAccessToken();
      // Persist refreshed tokens
      await updateDoc(doc(db, "users", userId), {
        tokens: credentials,
        updatedAt: serverTimestamp(),
      });
      return credentials;
    } catch (err) {
      console.error("Token refresh failed:", err);
    }
  }
  return storedTokens;
}

// ─── STREAM DIRECTLY TO DRIVE (Resumable + Chunked for maximum speed) ─────────
async function streamToDrive(
  jobId: string,
  file: WebTorrent.TorrentFile,
  userId: string,
  tokens: any,
  isLast: boolean = true
) {
  if (cancelledJobs.has(jobId)) throw new Error("Job Cancelled");

  const fileSize = file.length;
  const validTokens = await getValidTokens(userId, tokens);

  const jobDoc = await getDoc(doc(db, "jobs", jobId));
  const jobData = jobDoc.data();
  const folderId = jobData?.targetFolderId || await getTorrentFolderId(userId, validTokens);

  console.log(`[${jobId}] Streaming "${file.name}" (${fileSize} bytes) → Drive...`);

  // ── Create resumable upload session ────────────────────────────────────────
  const sessionRes = await axios.post(
    `https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable`,
    { name: file.name, parents: folderId ? [folderId] : undefined },
    {
      headers: {
        Authorization: `Bearer ${validTokens.access_token}`,
        "Content-Type": "application/json; charset=UTF-8",
        "X-Upload-Content-Type": "application/octet-stream",
        "X-Upload-Content-Length": fileSize,
      },
      timeout: 30000,
    }
  );
  const uploadUrl: string = sessionRes.headers.location;

  // ── Stream data sequentially ─────────────────────────────
  const torrent = activeJobs.get(jobId);
  const store = memoryStores.get(jobId);
  if (!store || !torrent) throw new Error("No store or torrent found for job");
  
  const readStream = createSequentialReadable(file, store);
  let uploadedBytes = 0;
  let lastEmitTime = 0;
  let speedWindowStart = Date.now();
  let speedWindowBytes = 0;

  const monitorStream = new Transform({
    transform(chunk: Buffer, encoding: string, callback: Function) {
      if (cancelledJobs.has(jobId)) {
        readStream.destroy();
        callback(new Error("Job Cancelled"));
        return;
      }
      uploadedBytes += chunk.length;
      speedWindowBytes += chunk.length;

      const now = Date.now();
      if (now - lastEmitTime > 400) {
        lastEmitTime = now;

        const elapsed = (now - speedWindowStart) / 1000;
        const uploadSpeed = elapsed > 0.1 ? speedWindowBytes / elapsed : 0;
        if (now - speedWindowStart > 2000) {
          speedWindowStart = now;
          speedWindowBytes = 0;
        }

        const progress = Math.min((uploadedBytes / fileSize) * 100, 100);
        io.emit("jobUpdate", { id: jobId, uploadProgress: progress, uploaded: uploadedBytes, uploadSpeed });

        const lastTs = lastUpdateTime.get(jobId) ?? 0;
        if (now - lastTs > THROTTLE_MS) {
          lastUpdateTime.set(jobId, now);
          updateDoc(doc(db, "jobs", jobId), { uploadProgress: progress, uploaded: uploadedBytes }).catch(() => {});
        }
      }
      callback(null, chunk);
    }
  });

  try {
    const res = await axios.put(uploadUrl, readStream.pipe(monitorStream), {
      headers: {
        "Content-Type": "application/octet-stream",
        "Content-Length": fileSize,
      },
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      timeout: 0, // Disable timeout for large streams
    });
    
    if (res.status < 200 || res.status >= 300) {
      throw new Error(`Drive upload failed: ${res.status}`);
    }
    const driveFileId = res.data?.id ?? null;

    if (isLast) {
      await updateJob(
        jobId,
        { status: "completed", uploadProgress: 100, uploaded: fileSize, googleDriveFileId: driveFileId },
        true
      );
    }
  } catch (err: any) {
    readStream.destroy();
    if (axios.isCancel(err)) throw new Error("Job Cancelled");
    throw err;
  }
}

async function getTorrentFolderId(userId: string, tokens: any): Promise<string | null> {
  if (torrentFolderCache.has(userId)) {
    return torrentFolderCache.get(userId)!;
  }
  try {
    const searchRes = await axios.get("https://www.googleapis.com/drive/v3/files", {
      headers: { Authorization: `Bearer ${tokens.access_token}` },
      params: {
        q: "name='Torrent' and mimeType='application/vnd.google-apps.folder' and trashed=false",
        fields: "files(id, name)",
        spaces: "drive",
      },
      timeout: 15000,
    });

    if (searchRes.data.files && searchRes.data.files.length > 0) {
      const folderId = searchRes.data.files[0].id;
      torrentFolderCache.set(userId, folderId);
      return folderId;
    }

    const createRes = await axios.post(
      "https://www.googleapis.com/drive/v3/files",
      {
        name: "Torrent",
        mimeType: "application/vnd.google-apps.folder",
      },
      {
        headers: {
          Authorization: `Bearer ${tokens.access_token}`,
          "Content-Type": "application/json",
        },
        timeout: 15000,
      }
    );

    const folderId = createRes.data.id;
    torrentFolderCache.set(userId, folderId);
    return folderId;
  } catch (error: any) {
    console.error(`[User ${userId}] Failed to ensure Torrent folder:`, error?.response?.data || error?.message);
    return null;
  }
}

// ─── GOOGLE DRIVE UPLOAD (Resumable) ────────────────────────────────────────
async function uploadToDrive(
  jobId: string,
  filePath: string,
  fileName: string,
  userId: string,
  tokens: any,
  isLast: boolean = true
) {
  if (cancelledJobs.has(jobId)) throw new Error("Job Cancelled");

  // Always try to get valid (possibly refreshed) tokens
  const validTokens = await getValidTokens(userId, tokens);

  const stats = fs.statSync(filePath);
  const fileSize = stats.size;

  // Check for existing resumable session
  const jobDoc = await getDoc(doc(db, "jobs", jobId));
  const jobData = jobDoc.data();
  let uploadUrl: string | null = jobData?.resumableUploadUrl || null;
  let startByte = 0;

  if (uploadUrl) {
    if (cancelledJobs.has(jobId)) throw new Error("Job Cancelled");
    try {
      const statusRes = await axios.put(uploadUrl, null, {
        headers: { "Content-Range": `bytes */${fileSize}` },
        validateStatus: (s) => s === 308 || s === 200 || s === 201,
        timeout: 15000,
      });
      if (statusRes.status === 308) {
        const range = statusRes.headers.range;
        startByte = range ? parseInt(range.split("-")[1]) + 1 : 0;
      } else if (statusRes.status === 200 || statusRes.status === 201) {
        if (isLast)
          await updateJob(
            jobId,
            { status: "completed", uploadProgress: 100 },
            true
          );
        if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
        return;
      }
    } catch {
      console.log(`[${jobId}] Resumable session expired, starting new upload.`);
      uploadUrl = null;
    }
  }

  if (!uploadUrl) {
    if (cancelledJobs.has(jobId)) throw new Error("Job Cancelled");
    
    // Prefer target folder set by the user, fallback to the default "Torrent" folder
    const folderId = jobData?.targetFolderId || await getTorrentFolderId(userId, validTokens);
    const fileMetadata: any = { name: fileName };
    if (folderId) {
      fileMetadata.parents = [folderId];
    }
    
    const res = await axios.post(
      "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable",
      fileMetadata,
      {
        headers: {
          Authorization: `Bearer ${validTokens.access_token}`,
          "Content-Type": "application/json; charset=UTF-8",
          "X-Upload-Content-Type": "application/octet-stream",
          "X-Upload-Content-Length": fileSize,
        },
        timeout: 30000,
      }
    );
    uploadUrl = res.headers.location as string;
    await updateDoc(doc(db, "jobs", jobId), { resumableUploadUrl: uploadUrl });
  }

  if (cancelledJobs.has(jobId)) throw new Error("Job Cancelled");

  // Chunked Upload to prevent Axios hanging indefinitely on huge streams
  const CHUNK_SIZE = 8 * 1024 * 1024; // 8MB chunks
  let currentByte = startByte;
  let lastEmit = Date.now();

  const fd = fs.openSync(filePath, "r");
  try {
    while (currentByte < fileSize) {
      if (cancelledJobs.has(jobId)) throw new Error("Job Cancelled");

      const chunkSize = Math.min(CHUNK_SIZE, fileSize - currentByte);
      const buffer = Buffer.alloc(chunkSize);
      fs.readSync(fd, buffer, 0, chunkSize, currentByte);

      const endByte = currentByte + chunkSize - 1;

      try {
        const uploadRes = await axios.put(uploadUrl, buffer, {
          headers: {
            "Content-Range": `bytes ${currentByte}-${endByte}/${fileSize}`,
            "Content-Type": "application/octet-stream",
          },
          maxContentLength: Infinity,
          maxBodyLength: Infinity,
          timeout: 60000, // 60 seconds timeout per chunk
          validateStatus: (status) => status === 200 || status === 201 || status === 308,
        });

        if (uploadRes.status === 308) {
          // Chunk uploaded successfully
          currentByte += chunkSize;
        } else if (uploadRes.status === 200 || uploadRes.status === 201) {
          // File completed
          currentByte += chunkSize;
          if (isLast) {
            await updateJob(
              jobId,
              {
                status: "completed",
                uploadProgress: 100,
                googleDriveFileId: uploadRes.data.id,
                resumableUploadUrl: null,
              },
              true
            );
          }
          break;
        } else {
          throw new Error("توقف الرفع بسبب حالة غير متوقعة: " + uploadRes.status);
        }

        // Progress Update
        const now = Date.now();
        if (now - lastEmit > 1500) {
          const progress = (currentByte / fileSize) * 100;
          io.emit("jobUpdate", {
            id: jobId,
            uploadProgress: progress,
            uploaded: currentByte,
          });
          if (now - (lastUpdateTime.get(jobId) ?? 0) > THROTTLE_MS) {
            lastUpdateTime.set(jobId, now);
            updateDoc(doc(db, "jobs", jobId), {
              uploadProgress: progress,
              uploaded: currentByte,
            }).catch(() => {});
          }
          lastEmit = now;
        }

      } catch (err: any) {
        if (cancelledJobs.has(jobId)) throw err;
        console.log(`[${jobId}] Chunk error. Querying current status...`, err.message);

        // Recover from network timeout or failure by checking actual uploaded bytes
        try {
          const statusRes = await axios.put(uploadUrl, null, {
            headers: { "Content-Range": `bytes */${fileSize}` },
            validateStatus: (s) => s === 308 || s === 200 || s === 201,
            timeout: 15000,
          });
          if (statusRes.status === 308) {
            const range = statusRes.headers.range;
            currentByte = range ? parseInt(range.split("-")[1]) + 1 : 0;
          } else if (statusRes.status === 200 || statusRes.status === 201) {
            if (isLast) {
              await updateJob(
                jobId,
                {
                  status: "completed",
                  uploadProgress: 100,
                  googleDriveFileId: statusRes.data.id,
                  resumableUploadUrl: null,
                },
                true
              );
            }
            break;
          } else {
             throw new Error("Status API returned unexpected code");
          }
        } catch (statusErr) {
          console.error(`[${jobId}] Failed to verify drive status:`, statusErr);
          throw new Error("فشل في استكمال الرفع لتعطل الشبكة");
        }
      }
    }
  } finally {
    fs.closeSync(fd);
    if (currentByte >= fileSize && fs.existsSync(filePath)) {
      try {
        fs.unlinkSync(filePath);
      } catch (_) {}
    }
  }
}

// ─── MEGA.NZ UPLOAD ─────────────────────────────────────────────────────────
async function streamToMega(
  jobId: string,
  file: WebTorrent.TorrentFile,
  megaConfig: any,
  isLast: boolean = true
) {
  if (cancelledJobs.has(jobId)) throw new Error("Job Cancelled");

  const storage = await new Storage({
    email: megaConfig.email,
    password: megaConfig.password,
  }).ready;

  if (cancelledJobs.has(jobId)) throw new Error("Job Cancelled");
  
  console.log(`[${jobId}] Streaming file ${file.name} directly to Mega...`);

  let uploadedBytes = 0;
  let lastEmitTime = 0;
  let speedWindowStart = Date.now();
  let speedWindowBytes = 0;

  const torrent = activeJobs.get(jobId);
  const store = memoryStores.get(jobId);
  if (!store || !torrent) throw new Error("No store or torrent found for job");
  
  const readStream = createSequentialReadable(file, store);

  const monitorStream = new Transform({
    transform(chunk: Buffer, encoding: string, callback: Function) {
      if (cancelledJobs.has(jobId)) {
        readStream.destroy();
        callback(new Error("Job Cancelled"));
        return;
      }
      uploadedBytes += chunk.length;
      speedWindowBytes += chunk.length;

      const now = Date.now();
      if (now - lastEmitTime > 400) {
        lastEmitTime = now;

        const elapsed = (now - speedWindowStart) / 1000;
        const uploadSpeed = elapsed > 0.1 ? speedWindowBytes / elapsed : 0;
        if (now - speedWindowStart > 2000) {
          speedWindowStart = now;
          speedWindowBytes = 0;
        }

        const progress = (uploadedBytes / file.length) * 100;
        io.emit("jobUpdate", { id: jobId, uploadProgress: progress, uploaded: uploadedBytes, uploadSpeed });
        
        const lastTs = lastUpdateTime.get(jobId) ?? 0;
        if (now - lastTs > THROTTLE_MS) {
          lastUpdateTime.set(jobId, now);
          updateDoc(doc(db, "jobs", jobId), { uploadProgress: progress, uploaded: uploadedBytes }).catch(() => {});
        }
      }
      callback(null, chunk);
    }
  });

  try {
    const uploadStream = readStream.pipe(monitorStream);

    const megaFile = await (storage as any)
      .upload({ name: file.name, size: file.length }, uploadStream)
      .complete;

    if (cancelledJobs.has(jobId)) {
      throw new Error("Job Cancelled");
    }

    if (isLast) {
      await updateJob(
        jobId,
        {
          status: "completed",
          uploadProgress: 100,
          uploaded: file.length,
          megaFileLink: await (megaFile as any).link(),
        },
        true
      );
    }
  } catch (err: any) {
    readStream.destroy();
    throw err;
  }
}

// ─── MEGA.NZ UPLOAD ─────────────────────────────────────────────────────────
async function uploadToMega(
  jobId: string,
  filePath: string,
  fileName: string,
  megaConfig: any,
  isLast: boolean = true
) {
  if (cancelledJobs.has(jobId)) throw new Error("Job Cancelled");

  const storage = await new Storage({
    email: megaConfig.email,
    password: megaConfig.password,
  }).ready;

  if (cancelledJobs.has(jobId)) throw new Error("Job Cancelled");

  const stats = fs.statSync(filePath);
  const fileSize = stats.size;
  let uploadedBytes = 0;
  let lastEmit = Date.now();

  const fileStream = fs.createReadStream(filePath);
  fileStream.on("data", (chunk: Buffer) => {
    if (cancelledJobs.has(jobId)) {
      fileStream.destroy();
      return;
    }
    uploadedBytes += chunk.length;
    const now = Date.now();
    if (now - lastEmit > 1500) {
      const progress = (uploadedBytes / fileSize) * 100;
      io.emit("jobUpdate", { id: jobId, uploadProgress: progress });
      lastEmit = now;
    }
  });

  const megaFile = await (storage as any)
    .upload({ name: fileName, size: fileSize }, fileStream)
    .complete;

  if (cancelledJobs.has(jobId)) {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    throw new Error("Job Cancelled");
  }

  if (isLast) {
    await updateJob(
      jobId,
      {
        status: "completed",
        uploadProgress: 100,
        megaFileLink: await (megaFile as any).link(),
      },
      true
    );
  }
  if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
}

// ─── CORE TORRENT HANDLER ────────────────────────────────────────────────────
async function handleTorrent(
  jobId: string,
  torrentSource: string | Buffer,
  userId: string,
  selectedIndices?: number[],
  streamMode: boolean = true
) {
  cancelledJobs.delete(jobId);

  const jobDir = path.join(DOWNLOAD_DIR, jobId);
  if (!streamMode && !fs.existsSync(jobDir)) {
    fs.mkdirSync(jobDir, { recursive: true });
  }

  const clientOpts: any = {
    maxConns: 100,
  };

  const jobClient = new WebTorrent(clientOpts);

  jobClient.on("error", (err: Error) => {
    console.error(`[${jobId}] WebTorrent client error:`, err.message);
  });

  const torrentOpts: any = {
    announce: ANNOUNCE_LIST,
    strategy: "sequential",
  };

  if (streamMode) {
    torrentOpts.store = createStoreFactory(jobId);
    torrentOpts.storeCacheSlots = 0;
  } else {
    torrentOpts.path = jobDir;
  }

  jobClient.add(
    torrentSource,
    torrentOpts,
    (torrent: WebTorrent.Torrent) => {
      activeJobs.set(jobId, torrent);
      
      if (streamMode) {
        const store = memoryStores.get(jobId);
        if (store) {
          store._torrent = torrent;
          const bpController = new BackpressureController(torrent, store);
          torrent.on("wire", (wire: any) => bpController.onNewWire(wire));
        }
      }
      
      attachTorrentListeners(jobId, torrent, userId, selectedIndices, jobDir, streamMode);
    }
  );
}

function attachTorrentListeners(
  jobId: string,
  torrent: WebTorrent.Torrent,
  userId: string,
  selectedIndices: number[] | undefined,
  jobDir: string,
  streamMode: boolean = true
) {
  // Apply file selection
  torrent.files.forEach((file, index) => {
    if (streamMode) {
      // In stream mode, do NOT select the entire file. We will dynamically select
      // pieces in createSequentialReadable. Otherwise, WebTorrent downloads out-of-order randomly.
      file.deselect();
    } else if (selectedIndices && selectedIndices.length > 0 && !selectedIndices.includes(index)) {
      file.deselect();
    } else {
      file.select();
    }
  });

  updateJob(
    jobId,
    {
      name: torrent.name,
      status: streamMode ? "streaming" : "downloading",
      totalSize: torrent.length,
      downloadProgress: torrent.progress * 100,
      streamMode,
    },
    true
  );

  // ── Helper: fetch user storage settings ──────────────────────────────────
  const getUserStorage = async () => {
    const userDoc = await getDoc(doc(db, "users", userId));
    const userData = userDoc.data();
    return {
      tokens: userData?.tokens,
      megaConfig: userData?.mega,
      preferredStorage: (userData?.preferredStorage ?? "drive") as "drive" | "mega",
    };
  };

  // ── STREAM MODE: simultaneous download + upload ───────────────────────────
  if (streamMode) {
    const startStreaming = async () => {
      if (cancelledJobs.has(jobId)) return;
      console.log(`[${jobId}] Stream mode: downloading + uploading simultaneously...`);

      const { tokens, megaConfig, preferredStorage } = await getUserStorage();

      if (preferredStorage === "drive" && !tokens) {
        await updateJob(jobId, { status: "error", error: "لم يتم العثور على صلاحيات Google Drive" }, true);
        return;
      }
      if (preferredStorage === "mega" && !megaConfig) {
        await updateJob(jobId, { status: "error", error: "لم يتم العثور على بيانات Mega.nz" }, true);
        return;
      }

      // ── Don't set downloadProgress: 100 — let torrent events handle it ──
      await updateJob(jobId, { status: "streaming", uploadProgress: 0 }, true);

      try {
        const filesToUpload =
          selectedIndices && selectedIndices.length > 0
            ? torrent.files.filter((_, i) => selectedIndices.includes(i))
            : torrent.files;

        for (let i = 0; i < filesToUpload.length; i++) {
          if (cancelledJobs.has(jobId)) throw new Error("Cancelled");
          const file = filesToUpload[i];
          const isLast = i === filesToUpload.length - 1;

          // Crucial: Tell WebTorrent to actually download this file!
          if (!streamMode) {
            file.select();
          }

          if (preferredStorage === "drive") {
            await streamToDrive(jobId, file, userId, tokens, isLast);
          } else {
            await streamToMega(jobId, file, megaConfig, isLast);
          }
        }

        try { torrent.client.destroy(); } catch (_) {}
        activeJobs.delete(jobId);
        lastUpdateTime.delete(jobId);
        memoryStores.delete(jobId); // Clean up memory store reference
        if (fs.existsSync(jobDir)) fs.rmSync(jobDir, { recursive: true, force: true });
      } catch (err: any) {
        if (err.message === "Cancelled" || cancelledJobs.has(jobId)) return;
        console.error(`[${jobId}] Stream upload error:`, err.message);
        await updateJob(jobId, { status: "error", error: "فشل في البث المباشر: " + err.message }, true);
      }
    };

    startStreaming();
  }

  // ── LOCAL MODE: download fully first, then upload ─────────────────────────
  if (!streamMode) {
    torrent.on("done", async () => {
      if (cancelledJobs.has(jobId)) return;
      console.log(`[${jobId}] Local mode: download done, starting upload...`);

      const { tokens, megaConfig, preferredStorage } = await getUserStorage();

      if (preferredStorage === "drive" && !tokens) {
        await updateJob(jobId, { status: "error", error: "لم يتم العثور على صلاحيات Google Drive" }, true);
        return;
      }
      if (preferredStorage === "mega" && !megaConfig) {
        await updateJob(jobId, { status: "error", error: "لم يتم العثور على بيانات Mega.nz" }, true);
        return;
      }

      await updateJob(jobId, { status: "uploading", uploadProgress: 0, downloadProgress: 100 }, true);

      try {
        const filesToUpload =
          selectedIndices && selectedIndices.length > 0
            ? torrent.files.filter((_, i) => selectedIndices.includes(i))
            : torrent.files;

        for (let i = 0; i < filesToUpload.length; i++) {
          if (cancelledJobs.has(jobId)) throw new Error("Cancelled");
          const file = filesToUpload[i];
          const filePath = path.join(jobDir, file.path);
          const isLast = i === filesToUpload.length - 1;

          if (preferredStorage === "drive") {
            await uploadToDrive(jobId, filePath, file.name, userId, tokens, isLast);
          } else {
            await uploadToMega(jobId, filePath, file.name, megaConfig, isLast);
          }
        }

        try { torrent.client.destroy(); } catch (_) {}
        activeJobs.delete(jobId);
        lastUpdateTime.delete(jobId);
        if (fs.existsSync(jobDir)) fs.rmSync(jobDir, { recursive: true, force: true });
      } catch (err: any) {
        if (err.message === "Cancelled" || cancelledJobs.has(jobId)) return;
        console.error(`[${jobId}] Local upload error:`, err.message);
        await updateJob(jobId, { status: "error", error: "فشل في الرفع: " + err.message }, true);
      }
    });
  }

  // ── Real-time progress (both modes) ───────────────────────────────────────
  torrent.on("download", () => {
    if (cancelledJobs.has(jobId)) {
      try { torrent.client.destroy(); } catch (_) {}
      return;
    }
    io.emit("jobUpdate", {
      id: jobId,
      downloadProgress: torrent.progress * 100,
      downloadSpeed: torrent.downloadSpeed,
      downloaded: torrent.downloaded,
    });
  });

  torrent.on("error", (err: any) => {
    if (cancelledJobs.has(jobId)) return;
    console.error(`[${jobId}] Torrent error:`, err.message ?? err);
    updateJob(jobId, { status: "error", error: err.message ?? "خطأ في التورنت" }, true);
    activeJobs.delete(jobId);
  });
}

// ─── SESSION MIDDLEWARE ──────────────────────────────────────────────────────
app.use(cookieParser());
app.use(
  session({
    secret: process.env.SESSION_SECRET ?? "torrent-drive-secret",
    resave: false,
    saveUninitialized: false,
    cookie: {
      secure: process.env.NODE_ENV === "production",
      sameSite: process.env.NODE_ENV === "production" ? "none" : "lax",
      httpOnly: true,
      maxAge: 7 * 24 * 60 * 60 * 1000, // 7 days
    },
  })
);
app.use(express.json({ limit: "10mb" }));

// ─── AUTH ROUTES ─────────────────────────────────────────────────────────────
app.get("/api/auth/url", (req, res) => {
  try {
    const origin = (req.query.origin as string) || "http://localhost:3000";
    const redirectUri = `${origin}/auth/callback`;
    const oauthClient = getOAuthClient(redirectUri);
    const url = oauthClient.generateAuthUrl({
      access_type: "offline",
      scope: [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/userinfo.email",
        "https://www.googleapis.com/auth/userinfo.profile",
      ],
      prompt: "consent",
      state: redirectUri,
    });
    res.json({ url });
  } catch (error: any) {
    console.error("Error generating Auth URL:", error);
    res.status(500).json({ error: error.message || "Failed to generate Auth URL" });
  }
});

app.get("/auth/callback", async (req, res) => {
  const { code, state } = req.query;
  const redirectUri =
    (state as string) || "http://localhost:3000/auth/callback";
  try {
    const oauthClient = getOAuthClient(redirectUri);
    const { tokens } = await oauthClient.getToken(code as string);
    oauthClient.setCredentials(tokens);
    const oauth2 = google.oauth2({ version: "v2", auth: oauthClient });
    const userInfo = await oauth2.userinfo.get();
    const userId = userInfo.data.id!;

    await setDoc(
      doc(db, "users", userId),
      {
        tokens,
        email: userInfo.data.email,
        preferredStorage: "drive",
        updatedAt: serverTimestamp(),
      },
      { merge: true }
    );

    (req as any).session.userId = userId;
    res.send(`
      <html>
        <body style="font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;background:#020617;color:white">
          <script>
            if (window.opener) {
              window.opener.postMessage({ type: 'OAUTH_AUTH_SUCCESS', userId: '${userId}' }, '*');
              window.close();
            } else {
              window.location.href = '/?userId=${userId}';
            }
          </script>
          <div style="text-align:center">
            <h2 style="color:#3b82f6">تم الاتصال بنجاح!</h2>
            <p>سيتم إغلاق هذه النافذة تلقائياً.</p>
          </div>
        </body>
      </html>
    `);
  } catch (error) {
    console.error("OAuth callback error:", error);
    res.status(500).send("Authentication failed");
  }
});

app.post("/api/auth/mega", async (req, res) => {
  const { email, password, userId } = req.body;
  if (!email || !password || !userId)
    return res.status(400).json({ error: "Missing data" });
  try {
    const storage = await new Storage({ email, password }).ready;
    if (storage) {
      await setDoc(
        doc(db, "users", userId),
        {
          mega: { email, password },
          email,
          preferredStorage: "mega",
          updatedAt: serverTimestamp(),
        },
        { merge: true }
      );
      res.json({ success: true, userId });
    }
  } catch {
    res.status(401).json({ error: "بيانات Mega غير صحيحة" });
  }
});

app.post("/api/settings/storage", async (req, res) => {
  const { userId, preferredStorage } = req.body;
  if (!userId || !preferredStorage)
    return res.status(400).json({ error: "Missing data" });
  try {
    await updateDoc(doc(db, "users", userId), { preferredStorage });
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: "Failed to update storage settings" });
  }
});

// ─── METADATA CLIENT (Persists DHT) ─────────────────────────────────────────
// Using a dedicated persistent client for metadata drastically speeds up
// magnet link resolution because its DHT node stays bootstrapped.
const metadataClient = new WebTorrent({ maxConns: 30 });
metadataClient.on("error", () => {});

// ─── TORRENT INFO (magnet) ───────────────────────────────────────────────────
app.post("/api/torrent/info", async (req, res) => {
  const { magnet } = req.body;
  if (!magnet) return res.status(400).json({ error: "Missing magnet" });

  let responded = false;

  const timeoutId = setTimeout(() => {
    if (!responded) {
      responded = true;
      res.status(408).json({ error: "فشل في جلب معلومات التورنت (انتهى الوقت). تأكد من وجود Peers نشطة." });
      
      // Attempt to clean up from metadataClient if it timed out
      try {
        const match = magnet.match(/btih:([a-fA-F0-9]{40}|[a-zA-Z2-7]{32})/i);
        const hash = match ? match[1] : null;
        if (hash) {
          const t = metadataClient.get(hash);
          if (t) t.destroy();
        }
      } catch (_) {}
    }
  }, 45000); // 45 seconds timeout for better peer discovery

  try {
    metadataClient.add(magnet, { announce: ANNOUNCE_LIST } as any, (torrent) => {
      if (!responded) {
        responded = true;
        clearTimeout(timeoutId);
        const files = torrent.files.map((f, i) => ({
          name: f.name,
          size: f.length,
          index: i,
        }));
        const info = { name: torrent.name, size: torrent.length, files };
        
        let bufferBase64 = "";
        if (torrent.torrentFile) {
          bufferBase64 = torrent.torrentFile.toString("base64");
        }
        
        torrent.destroy(); // Free up memory immediately
        res.json({ ...info, torrentBuffer: bufferBase64 });
      } else {
        torrent.destroy();
      }
    });

  } catch (err: any) {
    if (!responded) {
      responded = true;
      clearTimeout(timeoutId);
      res.status(400).json({ error: "رابط تورنت غير صالح: " + err.message });
    }
  }
});

// ─── TORRENT INFO (file upload) ──────────────────────────────────────────────
app.post(
  "/api/torrent/info/file",
  upload.single("torrent"),
  async (req, res) => {
    try {
      const file = (req as any).file;
      if (!file) return res.status(400).json({ error: "Missing torrent file" });

      const torrentBuffer = fs.readFileSync(file.path);
      if (fs.existsSync(file.path)) fs.unlinkSync(file.path);

      const tempClient = new WebTorrent();
      let responded = false;

      const cleanup = () => {
        try {
          tempClient.destroy();
        } catch (_) {}
      };

      const timeoutId = setTimeout(() => {
        if (!responded) {
          responded = true;
          cleanup();
          res.status(408).json({ error: "فشل في قراءة ملف التورنت (انتهى الوقت)." });
        }
      }, 25000);

      tempClient.on("error", (err: Error) => {
        if (!responded) {
          responded = true;
          clearTimeout(timeoutId);
          cleanup();
          res.status(400).json({ error: "ملف تورنت غير صالح: " + err.message });
        }
      });

      tempClient.add(
        torrentBuffer,
        { announce: ANNOUNCE_LIST } as any,
        (torrent) => {
          if (!responded) {
            responded = true;
            clearTimeout(timeoutId);
            const files = torrent.files.map((f, i) => ({
              name: f.name,
              size: f.length,
              index: i,
            }));
            const info = { name: torrent.name, size: torrent.length, files };
            // Also return buffer as base64 so client can store it for resume
            const bufferBase64 = torrentBuffer.toString("base64");
            torrent.destroy(() => cleanup());
            res.json({ ...info, torrentBuffer: bufferBase64 });
          }
        }
      );
    } catch (err: any) {
      console.error("Error reading torrent file:", err);
      res.status(500).json({ error: "Internal server error reading torrent file" });
    }
  }
);

// ─── GET DRIVE FOLDERS ────────────────────────────────────────────────────────
app.get("/api/drive/folders", async (req, res) => {
  const { userId, parentId } = req.query;
  if (!userId) return res.status(400).json({ error: "Missing userId" });
  
  try {
    const userDoc = await getDoc(doc(db, "users", userId as string));
    const tokens = userDoc.data()?.tokens;
    if (!tokens) return res.status(401).json({ error: "No Drive tokens" });
    
    const validTokens = await getValidTokens(userId as string, tokens);
    const queryStr = parentId 
        ? `'${parentId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`
        : `'root' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`;
        
    const response = await axios.get("https://www.googleapis.com/drive/v3/files", {
      headers: { Authorization: `Bearer ${validTokens.access_token}` },
      params: { 
        q: queryStr, 
        fields: "files(id, name)", 
        orderBy: "name",
        supportsAllDrives: true,
        includeItemsFromAllDrives: true
      },
    });
    
    res.json(response.data.files || []);
  } catch (error: any) {
    res.status(500).json({ error: error?.message || "Failed to fetch folders" });
  }
});

// ─── CREATE JOB (magnet) ─────────────────────────────────────────────────────
app.post("/api/jobs", async (req, res) => {
  const { magnet, userId, selectedIndices, targetFolderId, streamMode } = req.body;
  if (!magnet || !userId) return res.status(400).json({ error: "Missing data" });

  const isStreamMode = streamMode !== false; // default true

  try {
    const jobRef = await addDoc(collection(db, "jobs"), {
      userId,
      magnet,
      sourceType: "magnet",
      status: "queued",
      downloadProgress: 0,
      uploadProgress: 0,
      createdAt: serverTimestamp(),
      selectedIndices: selectedIndices ?? [],
      targetFolderId: targetFolderId || null,
      streamMode: isStreamMode,
    });

    const jobId = jobRef.id;
    handleTorrent(jobId, magnet, userId, selectedIndices, isStreamMode).catch((err) => {
      console.error("handleTorrent (magnet) failed:", err);
      updateJob(jobId, { status: "error", error: err.message }, true);
    });

    res.json({ id: jobId });
  } catch (error: any) {
    console.error("Create job error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// ─── CREATE JOB (file upload) ────────────────────────────────────────────────
app.post("/api/jobs/file", upload.single("torrent"), async (req, res) => {
  const file = (req as any).file;
  const { userId, targetFolderId } = req.body;
  if (!file || !userId) return res.status(400).json({ error: "Missing data" });

  try {
    const selectedIndices = req.body.selectedIndices
      ? JSON.parse(req.body.selectedIndices)
      : [];

    const isStreamMode = req.body.streamMode !== "false"; // default true

    const torrentBuffer = fs.readFileSync(file.path);
    if (fs.existsSync(file.path)) fs.unlinkSync(file.path);

    const torrentBase64 = torrentBuffer.toString("base64");

    const jobRef = await addDoc(collection(db, "jobs"), {
      userId,
      sourceType: "file",
      torrentBase64,
      status: "queued",
      downloadProgress: 0,
      uploadProgress: 0,
      createdAt: serverTimestamp(),
      selectedIndices,
      targetFolderId: targetFolderId || null,
      streamMode: isStreamMode,
    });

    const jobId = jobRef.id;
    handleTorrent(jobId, torrentBuffer, userId, selectedIndices, isStreamMode).catch((err) => {
      console.error("handleTorrent (file) failed:", err);
      updateJob(jobId, { status: "error", error: err.message }, true);
    });

    res.json({ id: jobId });
  } catch (error: any) {
    console.error("Create job file error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// ─── PAUSE JOB ───────────────────────────────────────────────────────────────
app.post("/api/jobs/:jobId/pause", async (req, res) => {
  const { jobId } = req.params;
  try {
    const torrent = activeJobs.get(jobId);
    if (torrent) {
      torrent.pause(); // Graceful pause - keeps peers connected
    }
    await updateDoc(doc(db, "jobs", jobId), { status: "paused" });
    io.emit("jobUpdate", { id: jobId, status: "paused" });
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: "Failed to pause job" });
  }
});

// ─── RESUME JOB ──────────────────────────────────────────────────────────────
app.post("/api/jobs/:jobId/resume", async (req, res) => {
  const { jobId } = req.params;
  try {
    const jobDoc = await getDoc(doc(db, "jobs", jobId));
    if (!jobDoc.exists()) return res.status(404).json({ error: "Not found" });
    const jobData = jobDoc.data();

    // If torrent is still in memory (just paused), resume it
    const torrent = activeJobs.get(jobId);
    if (torrent) {
      torrent.resume();
      await updateDoc(doc(db, "jobs", jobId), { status: "downloading" });
      io.emit("jobUpdate", { id: jobId, status: "downloading" });
      return res.json({ success: true });
    }

    // Otherwise restart download
    await updateDoc(doc(db, "jobs", jobId), { status: "downloading" });
    io.emit("jobUpdate", { id: jobId, status: "downloading" });

    if (jobData.sourceType === "file" && jobData.torrentBase64) {
      const buffer = Buffer.from(jobData.torrentBase64, "base64");
      handleTorrent(jobId, buffer, jobData.userId, jobData.selectedIndices, jobData.streamMode ?? true).catch(
        (err) => console.error("Resume (file) failed:", err)
      );
    } else if (jobData.magnet) {
      handleTorrent(jobId, jobData.magnet, jobData.userId, jobData.selectedIndices, jobData.streamMode ?? true).catch(
        (err) => console.error("Resume (magnet) failed:", err)
      );
    } else {
      return res.status(400).json({ error: "لا يمكن استئناف هذه المهمة - مصدر التورنت مفقود" });
    }

    res.json({ success: true });
  } catch (error: any) {
    console.error("Resume error:", error);
    res.status(500).json({ error: "Failed to resume job" });
  }
});

// ─── DELETE JOB ──────────────────────────────────────────────────────────────
app.delete("/api/jobs/:jobId", async (req, res) => {
  const { jobId } = req.params;
  try {
    // Mark as cancelled FIRST to stop any running activity
    cancelledJobs.add(jobId);
    lastUpdateTime.delete(jobId);

    const torrent = activeJobs.get(jobId);
    if (torrent) {
      try {
        torrent.client.destroy();
      } catch (_) {}
      activeJobs.delete(jobId);
    }

    await deleteDoc(doc(db, "jobs", jobId));

    const jobDir = path.join(DOWNLOAD_DIR, jobId);
    if (fs.existsSync(jobDir)) {
      fs.rmSync(jobDir, { recursive: true, force: true });
    }

    // Notify all clients that this job was deleted (so UI can remove it)
    io.emit("jobDeleted", { id: jobId });

    // Clean up cancelled set after 1 hour
    setTimeout(() => cancelledJobs.delete(jobId), 60 * 60 * 1000);

    res.json({ success: true });
  } catch (error: any) {
    console.error("Delete job error:", error);
    res.status(500).json({ error: "Failed to delete job" });
  }
});

// ─── GET JOBS ─────────────────────────────────────────────────────────────────
app.get("/api/jobs", async (req, res) => {
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: "Missing userId" });
  try {
    const snapshot = await getDocs(
      query(collection(db, "jobs"), where("userId", "==", userId))
    );
    let jobs = snapshot.docs.map((d) => {
      const data = d.data();
      // Strip large base64 buffer from response (not needed in UI)
      const { torrentBase64, ...rest } = data;
      return { id: d.id, ...rest };
    });
    jobs.sort((a: any, b: any) => {
      const tA = a.createdAt?.toMillis ? a.createdAt.toMillis() : 0;
      const tB = b.createdAt?.toMillis ? b.createdAt.toMillis() : 0;
      return tB - tA;
    });
    res.json(jobs.slice(0, 50));
  } catch (error) {
    console.error("Fetch jobs error:", error);
    res.status(500).json({ error: "Failed to fetch jobs" });
  }
});

// ─── VITE / STATIC ───────────────────────────────────────────────────────────
if (process.env.NODE_ENV !== "production") {
  const vite = await createViteServer({
    server: { middlewareMode: true },
    appType: "spa",
  });
  app.use(vite.middlewares);
} else {
  const distPath = path.join(__dirname, "dist");
  app.use(express.static(distPath));
  app.get("*", (_req, res) => {
    res.sendFile(path.join(distPath, "index.html"));
  });
}

// ─── START SERVER + RESUME ACTIVE JOBS ───────────────────────────────────────
httpServer.listen(PORT, "0.0.0.0", async () => {
  console.log(`✅ Server running on http://localhost:${PORT}`);

  try {
    const snapshot = await getDocs(query(collection(db, "jobs")));

    const activeStatuses = ["downloading", "uploading", "streaming", "queued"];
    
    for (const jobDoc of snapshot.docs) {
      const job = jobDoc.data();
      if (!activeStatuses.includes(job.status)) continue;
      const jobId = jobDoc.id;
      const jobStreamMode = job.streamMode ?? true;
      console.log(`↩️  Resuming job: ${jobId} (${job.status}, streamMode=${jobStreamMode})`);

      if (job.sourceType === "file" && job.torrentBase64) {
        const buffer = Buffer.from(job.torrentBase64, "base64");
        handleTorrent(jobId, buffer, job.userId, job.selectedIndices, jobStreamMode).catch(
          (err) => console.error(`Resume failed for ${jobId}:`, err)
        );
      } else if (job.magnet) {
        handleTorrent(jobId, job.magnet, job.userId, job.selectedIndices, jobStreamMode).catch(
          (err) => console.error(`Resume failed for ${jobId}:`, err)
        );
      } else {
        await updateDoc(doc(db, "jobs", jobId), {
          status: "error",
          error: "تعذر الاستئناف - مصدر التورنت مفقود",
        }).catch(() => {});
      }
    }
  } catch (err) {
    console.error("Startup resume error:", err);
  }
});
