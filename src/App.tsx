import React, { useState, useEffect, useCallback, useRef } from "react";
import { io, Socket } from "socket.io-client";
import {
  Download,
  Upload,
  Play,
  CheckCircle2,
  AlertCircle,
  LogOut,
  FolderOpen,
  Chrome,
  Settings,
  HardDrive,
  Cloud,
  FileUp,
  ShieldCheck,
  ExternalLink,
  Trash2,
  X,
  FileBox,
  Pause,
  PlayCircle,
  RefreshCw,
  Zap,
  Wifi,
  HardDriveDownload,
  ArrowUpFromLine,
  Timer,
  Radio,
} from "lucide-react";
import { motion, AnimatePresence } from "motion/react";

// ─── TYPES ────────────────────────────────────────────────────────────────────
interface Job {
  id: string;
  name?: string;
  status: "queued" | "downloading" | "uploading" | "streaming" | "completed" | "error" | "paused";
  downloadProgress: number;
  uploadProgress: number;
  totalSize?: number;
  downloaded?: number;
  uploaded?: number;
  downloadSpeed?: number;
  uploadSpeed?: number;
  error?: string;
  megaFileLink?: string;
  googleDriveFileId?: string;
  streamMode?: boolean;
}

interface TorrentInfo {
  name: string;
  size: number;
  files: { name: string; size: number; index: number }[];
  torrentBuffer?: string; // base64, returned for .torrent files
}

// ─── APP ──────────────────────────────────────────────────────────────────────
export default function App() {
  const [toast, setToast] = useState<{ message: string; type: "error" | "success" } | null>(null);

  const showToast = useCallback((message: string, type: "error" | "success" = "error") => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 5000);
  }, []);

  const [userId, setUserId] = useState<string | null>(
    localStorage.getItem("userId")
  );
  const [magnet, setMagnet] = useState("");
  const [jobs, setJobs] = useState<Job[]>([]);
  const [socket, setSocket] = useState<Socket | null>(null);
  const [loading, setLoading] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [preferredStorage, setPreferredStorage] = useState<"drive" | "mega">("drive");
  const [megaCreds, setMegaCreds] = useState({ email: "", password: "" });
  const [megaError, setMegaError] = useState("");
  const [previewData, setPreviewData] = useState<TorrentInfo | null>(null);
  const [selectedIndices, setSelectedIndices] = useState<number[]>([]);
  const [selectedTorrentFile, setSelectedTorrentFile] = useState<File | null>(null);
  const [pendingTorrentBuffer, setPendingTorrentBuffer] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const [targetFolderId, setTargetFolderId] = useState<string | null>(null);
  const [targetFolderName, setTargetFolderName] = useState<string>("مجلد التورنت الافتراضي");
  const [showFolderPicker, setShowFolderPicker] = useState(false);
  const [driveFolders, setDriveFolders] = useState<{id: string, name: string}[]>([]);
  const [loadingFolders, setLoadingFolders] = useState(false);
  const [currentParentId, setCurrentParentId] = useState<string | null>(null);
  const [folderHistory, setFolderHistory] = useState<{id: string | null, name: string}[]>([]);
  const [streamMode, setStreamMode] = useState<boolean>(true); // true = stream directly, false = download first

  // Track deleted job IDs to prevent socket from re-adding them
  const deletedJobIds = useRef<Set<string>>(new Set());
  // Track jobs being deleted (show loading state)
  const [deletingIds, setDeletingIds] = useState<Set<string>>(new Set());
  // Track jobs being paused/resumed
  const [togglingIds, setTogglingIds] = useState<Set<string>>(new Set());

  // ── INIT FROM URL ──────────────────────────────────────────────────────────
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const urlUserId = params.get('userId');
    if (urlUserId) {
      setUserId(urlUserId);
      localStorage.setItem('userId', urlUserId);
      setPreferredStorage("drive");
      // Clean up URL
      window.history.replaceState({}, document.title, window.location.pathname);
    }
  }, []);

  // ── SOCKET.IO SETUP ────────────────────────────────────────────────────────
  useEffect(() => {
    const newSocket = io({
      reconnectionAttempts: Infinity,
      reconnectionDelay: 1000,
      reconnectionDelayMax: 5000,
    });
    setSocket(newSocket);

    // Live progress update
    newSocket.on("jobUpdate", (updatedJob: Partial<Job> & { id: string }) => {
      // Ignore updates for deleted jobs
      if (deletedJobIds.current.has(updatedJob.id)) return;

      setJobs((prev) => {
        const index = prev.findIndex((j) => j.id === updatedJob.id);
        if (index === -1) {
          // New job from another session/tab - add it only if it has a name
          if (updatedJob.name) return [updatedJob as Job, ...prev];
          return prev;
        }
        const next = [...prev];
        next[index] = { ...next[index], ...updatedJob };
        return next;
      });
    });

    // Server confirms job deletion → remove from UI definitively
    newSocket.on("jobDeleted", ({ id }: { id: string }) => {
      deletedJobIds.current.add(id);
      setJobs((prev) => prev.filter((j) => j.id !== id));
      setDeletingIds((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
    });

    return () => {
      newSocket.close();
    };
  }, []);

  // ── FETCH JOBS ─────────────────────────────────────────────────────────────
  const fetchJobs = useCallback(async (uid: string) => {
    try {
      const res = await fetch(`/api/jobs?userId=${uid}`);
      if (res.ok) {
        const data = await res.json();
        setJobs(data);
      }
    } catch (err) {
      console.error("Failed to fetch jobs", err);
    }
  }, []);

  useEffect(() => {
    if (userId) fetchJobs(userId);
  }, [userId, fetchJobs]);

  // ── OAUTH MESSAGE LISTENER ─────────────────────────────────────────────────
  useEffect(() => {
    const handler = (event: MessageEvent) => {
      if (event.data?.type === "OAUTH_AUTH_SUCCESS") {
        const id = event.data.userId;
        setUserId(id);
        localStorage.setItem("userId", id);
        setPreferredStorage("drive");
        setShowSettings(false);
      }
    };
    window.addEventListener("message", handler);
    return () => window.removeEventListener("message", handler);
  }, []);

  // ── AUTH ACTIONS ───────────────────────────────────────────────────────────
  const handleConnectDrive = async () => {
    try {
      const res = await fetch(
        `/api/auth/url?origin=${encodeURIComponent(window.location.origin)}`
      );
      if (!res.ok) {
        const errorText = await res.text();
        throw new Error(`Failed to generate auth url. URL might be unreachable. ${res.status}`);
      }
      const data = await res.json();
      if (data.url) {
        window.open(data.url, "google-auth", "width=600,height=700,noopener");
      }
    } catch (err: any) {
      console.error("Auth error:", err.message);
      showToast("تعذر الاتصال بـ Google Drive", "error");
    }
  };

  const handleMegaLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!megaCreds.email || !megaCreds.password) return;
    setLoading(true);
    setMegaError("");
    try {
      const guestId = userId || "guest_" + Math.random().toString(36).slice(2);
      const res = await fetch("/api/auth/mega", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...megaCreds, userId: guestId }),
      });
      if (!res.ok) {
        const text = await res.text();
        let errorMsg = "خطأ غير معروف";
        try { errorMsg = JSON.parse(text).error || errorMsg; } catch {}
        setMegaError(errorMsg);
        return;
      }
      const data = await res.json();
      setPreferredStorage("mega");
      setUserId(data.userId);
      localStorage.setItem("userId", data.userId);
      setShowSettings(false);
    } catch {
      setMegaError("خطأ في الاتصال بخدمة Mega");
    } finally {
      setLoading(false);
    }
  };

  const handleStorageToggle = async (type: "drive" | "mega") => {
    if (!userId) return;
    setPreferredStorage(type);
    await fetch("/api/settings/storage", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ userId, preferredStorage: type }),
    });
  };

  const handleLogout = () => {
    setUserId(null);
    localStorage.removeItem("userId");
    setJobs([]);
    setPreviewData(null);
    setMagnet("");
  };

  // ── PREVIEW ────────────────────────────────────────────────────────────────
  const handlePreview = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!magnet || !userId) return;
    setLoading(true);
    setSelectedTorrentFile(null);
    setPendingTorrentBuffer(null);
    setPreviewData(null);
    try {
      const res = await fetch("/api/torrent/info", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ magnet }),
      });
      if (!res.ok) {
        const text = await res.text();
        let errorMsg = "خطأ في جلب المعلومات";
        try { errorMsg = JSON.parse(text).error || errorMsg; } catch {}
        showToast(errorMsg, "error");
        return;
      }
      const data = await res.json();
      setPreviewData(data);
      setSelectedIndices(data.files.map((f: any) => f.index));
    } catch {
      showToast("خطأ في الاتصال بالخادم", "error");
    } finally {
      setLoading(false);
    }
  };

  const fetchDriveFolders = async (parentId: string | null = null, folderName: string = 'الرئيسية') => {
    if (!userId || preferredStorage !== 'drive') return;
    setLoadingFolders(true);
    try {
      const parentQuery = parentId ? `?userId=${userId}&parentId=${parentId}` : `?userId=${userId}`;
      const res = await fetch(`/api/drive/folders${parentQuery}`);
      if (!res.ok) {
        showToast("خطأ في جلب المجلدات. تأكد من إعطاء الصلاحيات", "error");
        return;
      }
      const folders = await res.json();
      setDriveFolders(folders);
      setCurrentParentId(parentId);
    } catch {
      showToast("خطأ في الاتصال", "error");
    } finally {
      setLoadingFolders(false);
    }
  };

  const openFolderPicker = () => {
    setShowFolderPicker(true);
    fetchDriveFolders(null, 'الرئيسية');
    setFolderHistory([{id: null, name: 'الرئيسية'}]);
  };

  const navigateToFolder = (folderId: string, folderName: string) => {
    fetchDriveFolders(folderId);
    setFolderHistory((prev) => [...prev, {id: folderId, name: folderName}]);
  };

  const navigateBack = async () => {
    if (folderHistory.length <= 1) return;
    const newHistory = [...folderHistory];
    newHistory.pop(); // remove current
    const previous = newHistory[newHistory.length - 1];
    setFolderHistory(newHistory);
    
    setLoadingFolders(true);
    try {
      const parentQuery = previous.id ? `?userId=${userId}&parentId=${previous.id}` : `?userId=${userId}`;
      const res = await fetch(`/api/drive/folders${parentQuery}`);
      if (!res.ok) throw new Error("Fetch failed");
      const folders = await res.json();
      setDriveFolders(folders);
      setCurrentParentId(previous.id);
    } catch (error) {
      showToast("خطأ في جلب المجلدات", "error");
    } finally {
      setLoadingFolders(false);
    }
  };
  
  const handleSelectFolder = (folderId: string | null, folderName: string) => {
    setTargetFolderId(folderId);
    setTargetFolderName(folderName);
    setShowFolderPicker(false);
  };

  // ─── SUBMIT JOB ─────────────────────────────────────────────────────────────
  const handleSubmit = async () => {
    if (!userId) return;
    setLoading(true);
    try {
      let jobId: string | null = null;

      if (pendingTorrentBuffer) {
        // Use the buffer (available for physical files AND magnet links after info fetch)
        const byteArray = Uint8Array.from(atob(pendingTorrentBuffer), c => c.charCodeAt(0));
        const blob = new Blob([byteArray]);
        const formData = new FormData();
        const fileName = selectedTorrentFile?.name || (previewData?.name ? `${previewData.name}.torrent` : "download.torrent");
        formData.append("torrent", blob, fileName);
        formData.append("userId", userId);
        formData.append("selectedIndices", JSON.stringify(selectedIndices));
        formData.append("streamMode", String(streamMode));
        if (targetFolderId) {
          formData.append("targetFolderId", targetFolderId);
        }

        const res = await fetch("/api/jobs/file", {
          method: "POST",
          body: formData,
        });
        if (!res.ok) {
          const text = await res.text();
          let err: any = { error: "خطأ في إنشاء المهمة" };
          try { err = JSON.parse(text); } catch {}
          showToast(err.error || "خطأ في إنشاء المهمة", "error");
        } else {
          const data = await res.json();
          jobId = data.id;
        }
      } else if (magnet) {
        // Fallback to sending the magnet directly if buffer is missing
        const res = await fetch("/api/jobs", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ magnet, userId, selectedIndices, targetFolderId, streamMode }),
        });
        if (!res.ok) {
          const text = await res.text();
          let err: any = { error: "خطأ في إنشاء المهمة" };
          try { err = JSON.parse(text); } catch {}
          showToast(err.error || "خطأ في إنشاء المهمة", "error");
        } else {
          const data = await res.json();
          jobId = data.id;
        }
      }

      if (jobId) {
        // Optimistic add to UI immediately
        const newJob: Job = {
          id: jobId,
          status: "queued",
          downloadProgress: 0,
          uploadProgress: 0,
          name: previewData?.name || "جاري جلب المعلومات...",
          totalSize: previewData?.size,
        };
        setJobs((prev) => [newJob, ...prev]);

        // Reset form
        setMagnet("");
        setPreviewData(null);
        setSelectedIndices([]);
        setSelectedTorrentFile(null);
        setPendingTorrentBuffer(null);
        if (fileInputRef.current) fileInputRef.current.value = "";
      }
    } catch (err) {
      console.error("Submit error", err);
      showToast("خطأ في الاتصال بالخادم", "error");
    } finally {
      setLoading(false);
    }
  };

  // ── PAUSE / RESUME ─────────────────────────────────────────────────────────
  const handleTogglePause = async (id: string, currentStatus: string) => {
    if (togglingIds.has(id)) return;
    setTogglingIds((prev) => new Set([...prev, id]));

    const isPaused = currentStatus === "paused";
    const endpoint = isPaused ? "resume" : "pause";
    const newStatus = isPaused ? "downloading" : "paused";

    // Optimistic UI update
    setJobs((prev) =>
      prev.map((j) => (j.id === id ? { ...j, status: newStatus as any } : j))
    );

    try {
      await fetch(`/api/jobs/${id}/${endpoint}`, { method: "POST" });
    } catch (err) {
      console.error("Toggle pause error:", err);
      // Revert on failure
      setJobs((prev) =>
        prev.map((j) =>
          j.id === id ? { ...j, status: currentStatus as any } : j
        )
      );
    } finally {
      setTogglingIds((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
    }
  };

  // ── DELETE ─────────────────────────────────────────────────────────────────
  const handleDelete = async (id: string) => {
    if (deletingIds.has(id)) return;

    // Mark as deleted and remove from UI immediately
    deletedJobIds.current.add(id);
    setDeletingIds((prev) => new Set([...prev, id]));
    setJobs((prev) => prev.filter((j) => j.id !== id));

    try {
      const res = await fetch(`/api/jobs/${id}`, { method: "DELETE" });
      if (!res.ok) {
        // Server error - restore job
        deletedJobIds.current.delete(id);
        setDeletingIds((prev) => {
          const next = new Set(prev);
          next.delete(id);
          return next;
        });
        showToast("فشل في حذف المهمة، يرجى المحاولة مجدداً", "error");
        await fetchJobs(userId!);
      } else {
        showToast("تم الحذف بنجاح", "success");
      }
    } catch {
      deletedJobIds.current.delete(id);
      setDeletingIds((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
      showToast("خطأ في الاتصال بالخادم", "error");
      await fetchJobs(userId!);
    }
  };

  // ── FILE SELECTION ─────────────────────────────────────────────────────────
  const toggleFile = (index: number) => {
    setSelectedIndices((prev) =>
      prev.includes(index) ? prev.filter((i) => i !== index) : [...prev, index]
    );
  };

  const toggleAllFiles = () => {
    if (!previewData) return;
    if (selectedIndices.length === previewData.files.length) {
      setSelectedIndices([]);
    } else {
      setSelectedIndices(previewData.files.map((f) => f.index));
    }
  };

  // ── TORRENT FILE UPLOAD ────────────────────────────────────────────────────
  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file || !userId) return;

    setLoading(true);
    setMagnet("");
    setPreviewData(null);
    setPendingTorrentBuffer(null);

    const formData = new FormData();
    formData.append("torrent", file);

    try {
      const res = await fetch("/api/torrent/info/file", {
        method: "POST",
        body: formData,
      });
      if (!res.ok) {
        const text = await res.text();
        let errorMsg = "خطأ في قراءة ملف التورنت";
        try { errorMsg = JSON.parse(text).error || errorMsg; } catch {}
        showToast(errorMsg, "error");
        if (fileInputRef.current) fileInputRef.current.value = "";
        return;
      }
      const data = await res.json();
      setSelectedTorrentFile(file);
      setPendingTorrentBuffer(data.torrentBuffer ?? null);
      setPreviewData(data);
      setSelectedIndices(data.files.map((f: any) => f.index));
    } catch {
      showToast("خطأ في الاتصال بالخادم", "error");
    } finally {
      setLoading(false);
    }
  };

  // ── HELPERS ────────────────────────────────────────────────────────────────
  const formatSize = (bytes?: number) => {
    if (!bytes || bytes === 0) return "---";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let i = 0;
    let b = bytes;
    while (b >= 1024 && i < units.length - 1) {
      b /= 1024;
      i++;
    }
    return `${b.toFixed(1)} ${units[i]}`;
  };

  const formatSpeed = (bps?: number) => {
    if (!bps || bps === 0) return null;
    return formatSize(bps) + "/s";
  };

  const formatETA = (remainingBytes: number, speedBps: number): string | null => {
    if (!speedBps || speedBps <= 0 || !remainingBytes || remainingBytes <= 0) return null;
    const seconds = remainingBytes / speedBps;
    if (seconds > 86400) return `+24 ساعة`;
    if (seconds > 3600) return `~${Math.round(seconds / 3600)} ساعة`;
    if (seconds > 60) return `~${Math.round(seconds / 60)} دقيقة`;
    return `~${Math.round(seconds)} ثانية`;
  };

  const totalDownloadSpeed = jobs
    .filter((j) => j.status === "downloading" || j.status === "streaming")
    .reduce((acc, j) => acc + (j.downloadSpeed ?? 0), 0);

  const totalUploadSpeed = jobs
    .filter((j) => j.status === "uploading" || j.status === "streaming")
    .reduce((acc, j) => acc + (j.uploadSpeed ?? 0), 0);

  const activeCount = jobs.filter((j) =>
    ["downloading", "uploading", "streaming", "queued"].includes(j.status)
  ).length;

  // ── RENDER ─────────────────────────────────────────────────────────────────
  return (
    <div
      className="min-h-screen bg-slate-950 text-slate-100 font-sans flex flex-col selection:bg-blue-500/30"
      dir="rtl"
    >
      {/* ── HEADER ── */}
      <header className="h-16 border-b border-slate-800 flex items-center justify-between px-6 bg-slate-900/50 backdrop-blur-md sticky top-0 z-50">
        <div className="flex items-center gap-3">
          <div
            className="w-10 h-10 bg-blue-600 rounded-lg flex items-center justify-center shadow-lg shadow-blue-500/20 cursor-pointer"
            onClick={() => window.location.reload()}
          >
            <Download className="w-6 h-6 text-white" />
          </div>
          <h1 className="text-xl font-bold tracking-tight flex items-center gap-2">
            كلاود ستريم
            <span className="text-blue-500 text-[10px] font-bold border border-blue-500/30 px-1.5 py-0.5 rounded leading-none">
              PRO
            </span>
          </h1>
        </div>

        <div className="flex items-center gap-3">
          {userId ? (
            <>
              <button
                onClick={() => setShowSettings(!showSettings)}
                className={`flex items-center gap-2 px-3 py-2 rounded-lg transition-colors text-sm font-bold ${
                  showSettings
                    ? "bg-blue-600 text-white"
                    : "bg-slate-800 text-slate-400 hover:text-white"
                }`}
              >
                <Settings className="w-4 h-4" />
                <span className="hidden sm:inline">الإعدادات</span>
              </button>
              <button
                onClick={handleLogout}
                className="bg-slate-800 hover:bg-slate-700 text-slate-300 px-3 py-2 rounded-lg text-sm transition-colors flex items-center gap-2"
              >
                <span className="hidden sm:inline">الخروج</span>
                <LogOut className="w-4 h-4" />
              </button>
            </>
          ) : (
            <div className="flex items-center gap-2">
              <button
                onClick={() => setShowSettings(true)}
                className="bg-red-600 hover:bg-red-500 text-white px-4 py-2 rounded-xl text-sm font-bold transition-all flex items-center gap-2 shadow-lg shadow-red-600/20"
              >
                <ShieldCheck className="w-4 h-4" />
                <span>Mega.nz</span>
              </button>
              <button
                onClick={handleConnectDrive}
                className="bg-blue-600 hover:bg-blue-500 text-white px-4 py-2 rounded-xl text-sm font-bold transition-all flex items-center gap-2 shadow-lg shadow-blue-600/20"
              >
                <Chrome className="w-4 h-4" />
                <span>ربط Drive</span>
              </button>
            </div>
          )}
        </div>
      </header>

      {/* ── WELCOME BANNER ── */}
      {!userId && !showSettings && (
        <div className="bg-blue-600/10 border-b border-blue-500/20 p-8 text-center">
          <h2 className="text-2xl font-black mb-2">مرحباً بك في كلاود ستريم</h2>
          <p className="text-slate-400 max-w-2xl mx-auto">
            ربط حساب Google Drive أو Mega.nz وابدأ في نقل ملفات التورنت إلى
            سحابتك مباشرة بسرعة خارقة.
          </p>
        </div>
      )}

      {/* ── SETTINGS PANEL ── */}
      <AnimatePresence>
        {showSettings && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            className="bg-slate-900/80 border-b border-slate-800 backdrop-blur-xl overflow-hidden"
          >
            <div className="max-w-4xl mx-auto p-8 grid grid-cols-1 md:grid-cols-2 gap-8">
              <div className="space-y-4">
                <h3 className="text-sm font-bold text-slate-400 uppercase tracking-widest flex items-center gap-2">
                  <HardDrive className="w-4 h-4" />
                  وجهة التحميل
                </h3>
                <div className="grid grid-cols-2 gap-3">
                  <button
                    onClick={() => handleStorageToggle("drive")}
                    className={`p-4 rounded-2xl border-2 flex flex-col items-center gap-2 transition-all ${
                      preferredStorage === "drive"
                        ? "border-blue-500 bg-blue-500/10"
                        : "border-slate-800 hover:border-slate-700"
                    }`}
                  >
                    <Cloud className="w-6 h-6" />
                    <span className="text-sm font-bold">Google Drive</span>
                  </button>
                  <button
                    onClick={() => handleStorageToggle("mega")}
                    className={`p-4 rounded-2xl border-2 flex flex-col items-center gap-2 transition-all ${
                      preferredStorage === "mega"
                        ? "border-red-500 bg-red-500/10"
                        : "border-slate-800 hover:border-slate-700"
                    }`}
                  >
                    <ShieldCheck className="w-6 h-6" />
                    <span className="text-sm font-bold">Mega.nz</span>
                  </button>
                </div>
                {userId && (
                  <button
                    onClick={handleConnectDrive}
                    className="w-full flex items-center justify-center gap-2 px-4 py-2.5 rounded-xl bg-blue-600/10 border border-blue-500/30 text-blue-400 hover:bg-blue-600/20 text-sm font-bold transition-all"
                  >
                    <Chrome className="w-4 h-4" />
                    إعادة ربط Google Drive
                  </button>
                )}
              </div>

              <div className="space-y-4">
                <h3 className="text-sm font-bold text-slate-400 uppercase tracking-widest">
                  إعدادات Mega.nz
                </h3>
                <form onSubmit={handleMegaLogin} className="space-y-3">
                  {megaError && (
                    <div className="bg-red-500/10 border border-red-500/20 text-red-400 p-3 rounded-xl text-xs font-bold flex items-center gap-2">
                      <AlertCircle className="w-4 h-4 shrink-0" />
                      <span>{megaError}</span>
                    </div>
                  )}
                  <input
                    type="email"
                    placeholder="البريد الإلكتروني"
                    className="w-full bg-slate-950 border border-slate-800 rounded-xl px-4 py-2.5 text-sm focus:ring-2 focus:ring-red-500 outline-none"
                    value={megaCreds.email}
                    onChange={(e) =>
                      setMegaCreds((p) => ({ ...p, email: e.target.value }))
                    }
                  />
                  <input
                    type="password"
                    placeholder="كلمة المرور"
                    className="w-full bg-slate-950 border border-slate-800 rounded-xl px-4 py-2.5 text-sm focus:ring-2 focus:ring-red-500 outline-none"
                    value={megaCreds.password}
                    onChange={(e) =>
                      setMegaCreds((p) => ({ ...p, password: e.target.value }))
                    }
                  />
                  <button
                    type="submit"
                    disabled={loading}
                    className="w-full bg-red-600 hover:bg-red-500 py-2.5 rounded-xl text-sm font-bold transition-all disabled:opacity-50"
                  >
                    {loading ? "جاري التحقق..." : "حفظ وتسجيل دخول Mega"}
                  </button>
                </form>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* ── MAIN ── */}
      <main className="flex-1 p-6 flex flex-col gap-6 max-w-[1400px] mx-auto w-full">
        {/* Input Section */}
        <section className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          <div className="lg:col-span-8 bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-4">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium text-slate-400">
                أدخل الرابط المغناطيسي (Magnet Link)
              </label>
              {loading && (
                <div className="flex items-center gap-2 text-blue-400 text-xs">
                  <div className="w-3.5 h-3.5 border-2 border-blue-500/30 border-t-blue-500 rounded-full animate-spin" />
                  <span>جاري المعالجة...</span>
                </div>
              )}
            </div>

            <form onSubmit={handlePreview} className="flex gap-3">
              <input
                type="text"
                placeholder="magnet:?xt=urn:btih:..."
                className="flex-1 bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-blue-600 transition-all text-left placeholder:text-slate-600 disabled:opacity-50"
                dir="ltr"
                value={magnet}
                onChange={(e) => setMagnet(e.target.value)}
                disabled={!userId || loading}
              />
              <button
                type="submit"
                disabled={!userId || !magnet || loading}
                className="bg-blue-600 hover:bg-blue-500 disabled:opacity-40 disabled:cursor-not-allowed px-6 py-3 rounded-xl font-bold transition-all flex items-center gap-2 shadow-lg shadow-blue-600/20 whitespace-nowrap active:scale-[0.98]"
              >
                <span>معاينة</span>
                <Play className="w-4 h-4" />
              </button>
            </form>

            {/* Preview Panel */}
            <AnimatePresence>
              {previewData && (
                <motion.div
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -10 }}
                  className="bg-slate-950/60 border border-slate-700 rounded-xl p-4 space-y-4"
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3 min-w-0">
                      <FileBox className="w-5 h-5 text-blue-400 shrink-0" />
                      <div className="min-w-0">
                        <h4 className="font-bold text-sm truncate">
                          {previewData.name}
                        </h4>
                        <span className="text-[11px] text-slate-500">
                          الحجم الكلي: {formatSize(previewData.size)} ·{" "}
                          {previewData.files.length} ملف
                        </span>
                      </div>
                    </div>
                    <button
                      onClick={() => {
                        setPreviewData(null);
                        setSelectedIndices([]);
                        setSelectedTorrentFile(null);
                        setPendingTorrentBuffer(null);
                        if (fileInputRef.current)
                          fileInputRef.current.value = "";
                      }}
                      className="text-slate-500 hover:text-white p-1 rounded transition-colors shrink-0"
                    >
                      <X className="w-4 h-4" />
                    </button>
                  </div>

                  {/* File list */}
                  <div className="max-h-56 overflow-y-auto space-y-1 border-y border-slate-800 py-3">
                    <div className="flex items-center justify-between px-2 mb-2">
                      <button
                        onClick={toggleAllFiles}
                        className="text-[11px] text-blue-400 hover:text-blue-300 font-bold transition-colors"
                      >
                        {selectedIndices.length === previewData.files.length
                          ? "إلغاء تحديد الكل"
                          : "تحديد الكل"}
                      </button>
                    </div>
                    {previewData.files.map((file) => (
                      <div
                        key={file.index}
                        onClick={() => toggleFile(file.index)}
                        className="flex items-center justify-between text-[11px] p-2 hover:bg-slate-900 rounded-lg cursor-pointer group"
                      >
                        <div className="flex items-center gap-3 truncate flex-1">
                          <input
                            type="checkbox"
                            checked={selectedIndices.includes(file.index)}
                            onChange={() => toggleFile(file.index)}
                            onClick={(e) => e.stopPropagation()}
                            className="rounded text-blue-600 focus:ring-0 w-4 h-4 shrink-0 bg-slate-950 border-slate-700 cursor-pointer"
                          />
                          <span className="truncate group-hover:text-blue-400 transition-colors">
                            {file.name}
                          </span>
                        </div>
                        <span className="text-slate-500 font-mono shrink-0 px-2">
                          {formatSize(file.size)}
                        </span>
                      </div>
                    ))}
                  </div>

                  <div className="flex items-center justify-between">
                    <span className="text-xs text-slate-500">
                      {selectedIndices.length}/{previewData.files.length} ملف ·{" "}
                      {formatSize(
                        previewData.files
                          .filter((f) => selectedIndices.includes(f.index))
                          .reduce((acc, f) => acc + f.size, 0)
                      )}
                    </span>
                    <div className="flex items-center gap-3">
                      {/* Stream mode toggle */}
                      <div className="flex items-center gap-1 bg-slate-900 border border-slate-700 rounded-lg p-0.5 text-[10px] font-black">
                        <button
                          onClick={() => setStreamMode(true)}
                          className={`flex items-center gap-1 px-2.5 py-1.5 rounded-md transition-all ${
                            streamMode
                              ? "bg-violet-600 text-white shadow-lg shadow-violet-600/30"
                              : "text-slate-500 hover:text-slate-300"
                          }`}
                          title="يتجاوز حد الـ 2 جيجا - يرفع أثناء التحميل مباشرة"
                        >
                          <Radio className="w-3 h-3" />
                          بث مباشر
                        </button>
                        <button
                          onClick={() => setStreamMode(false)}
                          className={`flex items-center gap-1 px-2.5 py-1.5 rounded-md transition-all ${
                            !streamMode
                              ? "bg-blue-600 text-white shadow-lg shadow-blue-600/30"
                              : "text-slate-500 hover:text-slate-300"
                          }`}
                          title="يحمل كامل الملف أولاً (حد 2 جيجا)"
                        >
                          <HardDriveDownload className="w-3 h-3" />
                          تحميل أولاً
                        </button>
                      </div>
                      <button
                        onClick={handleSubmit}
                        disabled={selectedIndices.length === 0 || loading}
                        className="bg-emerald-600 hover:bg-emerald-500 disabled:opacity-40 disabled:cursor-not-allowed text-white px-6 py-2 rounded-lg text-xs font-black transition-all shadow-lg shadow-emerald-600/20 active:scale-95 flex items-center gap-2"
                      >
                        <Upload className="w-3.5 h-3.5" />
                        بدء التحميل
                      </button>
                    </div>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>

            {/* Divider */}
            <div className="flex items-center gap-4">
              <div className="h-px bg-slate-800 flex-1" />
              <span className="text-xs text-slate-600 font-bold tracking-widest uppercase">
                أو
              </span>
              <div className="h-px bg-slate-800 flex-1" />
            </div>

            {/* File upload button */}
            <div className="flex justify-center gap-4 flex-wrap">
              <button
                onClick={() => fileInputRef.current?.click()}
                disabled={!userId || loading}
                className="flex items-center gap-3 px-6 py-3 rounded-xl border border-slate-800 hover:bg-slate-800 hover:border-slate-700 transition-all text-sm font-bold text-slate-400 hover:text-slate-200 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                <FileUp className="w-5 h-5" />
                <span>رفع ملف .torrent</span>
              </button>
              <input
                type="file"
                accept=".torrent"
                className="hidden"
                ref={fileInputRef}
                onChange={handleFileChange}
              />
              {preferredStorage === 'drive' && (
                <button
                  onClick={openFolderPicker}
                  disabled={!userId || loading}
                  className="flex items-center gap-3 px-6 py-3 rounded-xl border border-blue-500/30 hover:bg-blue-600/10 transition-all text-sm font-bold text-blue-400 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  <FolderOpen className="w-5 h-5" />
                  <span className="truncate max-w-[150px]" dir="ltr">{targetFolderName}</span>
                </button>
              )}
            </div>
            {/* Stream mode hint (shown when no preview) */}
            {!previewData && userId && (
              <div className="flex items-center justify-center gap-6 text-[10px] text-slate-600">
                <div className="flex items-center gap-1.5">
                  <div className={`w-2 h-2 rounded-full ${streamMode ? "bg-violet-500" : "bg-slate-700"}`} />
                  <span>
                    وضع الرفع:{" "}
                    <span className={streamMode ? "text-violet-400 font-bold" : "text-slate-500"}>
                      {streamMode ? "بث مباشر ⚡ (يتجاوز حد 2 جيجا)" : "تحميل محلي أولاً"}
                    </span>
                  </span>
                  <button
                    onClick={() => setStreamMode(!streamMode)}
                    className="underline text-slate-500 hover:text-slate-300 transition-colors"
                  >
                    تغيير
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* Info card */}
          <div className="hidden lg:flex lg:col-span-4 bg-slate-900 border border-slate-800 rounded-2xl p-6 flex-col items-center justify-center gap-4 relative overflow-hidden group">
            <div className="absolute inset-0 bg-blue-600/5 opacity-0 group-hover:opacity-100 transition-opacity" />
            <Cloud
              className={`w-12 h-12 mb-1 group-hover:scale-110 transition-transform ${
                preferredStorage === "drive"
                  ? "text-blue-500/50"
                  : "text-red-500/50"
              }`}
            />
            <div className="text-center">
              <p className="text-sm font-bold text-slate-300">
                الوجهة النشطة:
              </p>
              <p
                className={`text-xl font-black mt-1 ${
                  preferredStorage === "drive"
                    ? "text-blue-500"
                    : "text-red-500"
                }`}
              >
                {preferredStorage === "drive" ? "Google Drive" : "Mega.nz"}
              </p>
            </div>
            {totalDownloadSpeed > 0 && (
              <div className="bg-blue-500/10 border border-blue-500/20 rounded-xl px-4 py-2 text-center">
                <p className="text-[10px] text-slate-400 font-bold uppercase tracking-wider">
                  سرعة التحميل
                </p>
                <p className="text-blue-400 font-black text-lg">
                  ↓ {formatSpeed(totalDownloadSpeed)}
                </p>
              </div>
            )}
            {totalUploadSpeed > 0 && (
              <div className="bg-emerald-500/10 border border-emerald-500/20 rounded-xl px-4 py-2 text-center">
                <p className="text-[10px] text-slate-400 font-bold uppercase tracking-wider">
                  سرعة الرفع
                </p>
                <p className="text-emerald-400 font-black text-lg">
                  ↑ {formatSpeed(totalUploadSpeed)}
                </p>
              </div>
            )}
            <p className="text-[10px] text-slate-600 text-center">
              يمكنك تغيير الوجهة من الإعدادات
            </p>
          </div>
        </section>

        {/* Jobs Table */}
        <section className="flex-1 flex flex-col min-h-0">
          <div className="flex items-center justify-between mb-4 px-1">
            <h2 className="text-base font-bold flex items-center gap-2">
              <div className="w-2 h-2 bg-blue-500 rounded-full animate-pulse" />
              المهام النشطة
            </h2>
            <div className="flex items-center gap-3">
              {totalDownloadSpeed > 0 && (
                <div className="flex items-center gap-1.5 text-[11px] text-blue-400 font-bold bg-blue-500/10 border border-blue-500/20 px-3 py-1 rounded-full">
                  <Zap className="w-3 h-3" />
                  ↓ {formatSpeed(totalDownloadSpeed)}
                </div>
              )}
              {totalUploadSpeed > 0 && (
                <div className="flex items-center gap-1.5 text-[11px] text-emerald-400 font-bold bg-emerald-500/10 border border-emerald-500/20 px-3 py-1 rounded-full">
                  <ArrowUpFromLine className="w-3 h-3" />
                  ↑ {formatSpeed(totalUploadSpeed)}
                </div>
              )}
              <button
                onClick={() => userId && fetchJobs(userId)}
                disabled={!userId}
                className="p-1.5 rounded-lg text-slate-600 hover:text-slate-300 hover:bg-slate-800 transition-colors disabled:opacity-40"
                title="تحديث"
              >
                <RefreshCw className="w-4 h-4" />
              </button>
              <span className="text-xs font-mono text-slate-500">
                {activeCount} نشط
              </span>
            </div>
          </div>

          <div className="flex-1 bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden shadow-2xl flex flex-col">
            {/* Table header */}
            <div className="grid grid-cols-12 gap-3 bg-slate-800/40 px-5 py-3 text-[10px] font-bold uppercase tracking-wider text-slate-500 border-b border-slate-800/50">
              <div className="col-span-12 lg:col-span-4">الملف / المهمة</div>
              <div className="hidden lg:block lg:col-span-3 text-center">
                التحميل P2P
              </div>
              <div className="hidden lg:block lg:col-span-3 text-center">
                الرفع السحابي
              </div>
              <div className="hidden lg:block lg:col-span-2 text-left">
                الإجراءات
              </div>
            </div>

            {/* Scrollable rows */}
            <div className="flex-1 overflow-y-auto divide-y divide-slate-800/50">
              <AnimatePresence mode="popLayout">
                {jobs.length === 0 ? (
                  <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    className="p-16 flex flex-col items-center justify-center text-slate-600 gap-4"
                  >
                    <div className="w-16 h-16 rounded-full bg-slate-950 border border-slate-800 flex items-center justify-center">
                      <FolderOpen className="w-8 h-8" />
                    </div>
                    <p className="text-sm font-medium">
                      {userId
                        ? "لا توجد تحميلات نشطة حالياً"
                        : "قم بالتسجيل أولاً لبدء التحميل"}
                    </p>
                  </motion.div>
                ) : (
                  jobs.map((job) => (
                    <motion.div
                      key={job.id}
                      layout
                      initial={{ opacity: 0, x: -20 }}
                      animate={{ opacity: 1, x: 0 }}
                      exit={{ opacity: 0, x: 20 }}
                      className={`grid grid-cols-12 gap-3 px-5 py-4 items-center hover:bg-slate-800/20 transition-all border-l-4 ${
                        job.status === "completed"
                          ? "border-l-emerald-500/60"
                          : job.status === "error"
                          ? "border-l-red-500/60"
                          : job.status === "downloading"
                          ? "border-l-blue-500/60"
                          : job.status === "streaming"
                          ? "border-l-violet-500/60"
                          : job.status === "uploading"
                          ? "border-l-violet-500/60"
                          : "border-l-transparent"
                      }`}
                    >
                      {/* Name & info */}
                      <div className="col-span-12 lg:col-span-4 flex flex-col gap-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-bold truncate text-slate-200">
                            {job.name || "جاري جلب المعلومات..."}
                          </span>
                          {job.megaFileLink && (
                            <a
                              href={job.megaFileLink}
                              target="_blank"
                              rel="noreferrer"
                              className="text-red-400 hover:text-red-300 shrink-0"
                              title="فتح رابط Mega"
                            >
                              <ExternalLink className="w-3.5 h-3.5" />
                            </a>
                          )}
                          {job.googleDriveFileId && (
                            <a
                              href={`https://drive.google.com/file/d/${job.googleDriveFileId}/view`}
                              target="_blank"
                              rel="noreferrer"
                              className="text-blue-400 hover:text-blue-300 shrink-0"
                              title="فتح في Drive"
                            >
                              <ExternalLink className="w-3.5 h-3.5" />
                            </a>
                          )}
                        </div>
                        <div className="flex items-center gap-2 flex-wrap">
                          <span className="text-[10px] bg-slate-800 px-1.5 py-0.5 rounded text-slate-400 font-mono">
                            {formatSize(job.totalSize)}
                          </span>
                          {job.status === "downloading" &&
                            job.downloadSpeed != null &&
                            job.downloadSpeed > 0 && (
                              <span className="text-[10px] text-blue-400 font-bold flex items-center gap-1 animate-pulse">
                                ↓ {formatSpeed(job.downloadSpeed)}
                              </span>
                            )}
                          {job.status === "streaming" && (
                            <span className="text-[10px] text-violet-400 font-bold flex items-center gap-1">
                              {job.downloadSpeed && job.downloadSpeed > 0 && <span>↓ {formatSpeed(job.downloadSpeed)}</span>}
                              {job.uploadSpeed && job.uploadSpeed > 0 && <span className="text-emerald-400">↑ {formatSpeed(job.uploadSpeed)}</span>}
                            </span>
                          )}
                        </div>
                      </div>

                      {/* Download progress */}
                      <div className="col-span-6 lg:col-span-3 px-2">
                        <div className="flex justify-between text-[10px] mb-1.5 font-mono">
                          <span className="text-blue-400 font-bold">
                            {(job.downloadProgress ?? 0).toFixed(1)}%
                          </span>
                          <span className="text-slate-500 flex items-center gap-1">
                            {job.status === "downloading" || job.status === "streaming" ? (
                              <>
                                {formatSpeed(job.downloadSpeed) && (
                                  <span className="text-blue-400">↓ {formatSpeed(job.downloadSpeed)}</span>
                                )}
                                {job.downloadSpeed && job.totalSize && (
                                  <span className="text-slate-600">
                                    {formatETA(
                                      (job.totalSize ?? 0) * (1 - (job.downloadProgress ?? 0) / 100),
                                      job.downloadSpeed
                                    )}
                                  </span>
                                )}
                              </>
                            ) : (
                              formatSize(job.downloaded)
                            )}
                          </span>
                        </div>
                        <div className="w-full h-1.5 bg-slate-950 rounded-full overflow-hidden">
                          <motion.div
                            animate={{ width: `${job.downloadProgress ?? 0}%` }}
                            transition={{ duration: 0.3 }}
                            className={`h-full rounded-full ${job.status === "streaming" ? "bg-violet-500" : "bg-blue-500"}`}
                          />
                        </div>
                        {job.status === "streaming" && (
                          <div className="flex items-center gap-1 mt-1">
                            <div className="w-1.5 h-1.5 bg-violet-400 rounded-full animate-pulse" />
                            <span className="text-[9px] text-violet-400 font-bold">بث مباشر</span>
                          </div>
                        )}
                      </div>

                      {/* Upload progress */}
                      <div className="col-span-6 lg:col-span-3 px-2">
                        {job.status === "uploading" ||
                        job.status === "streaming" ||
                        job.status === "completed" ? (
                          <>
                            <div className="flex justify-between text-[10px] mb-1.5 font-mono">
                              <span
                                className={`font-bold ${
                                  preferredStorage === "mega"
                                    ? "text-red-400"
                                    : "text-emerald-400"
                                }`}
                              >
                                {(job.uploadProgress ?? 0).toFixed(1)}%
                              </span>
                              <span className="text-slate-500 flex items-center gap-1">
                                {(job.status === "uploading" || job.status === "streaming") && job.uploadSpeed ? (
                                  <>
                                    <span className="text-emerald-400">↑ {formatSpeed(job.uploadSpeed)}</span>
                                    {job.totalSize && (
                                      <span className="text-slate-600">
                                        {formatETA(
                                          (job.totalSize ?? 0) * (1 - (job.uploadProgress ?? 0) / 100),
                                          job.uploadSpeed
                                        )}
                                      </span>
                                    )}
                                  </>
                                ) : (
                                  formatSize(job.uploaded)
                                )}
                              </span>
                            </div>
                            <div className="w-full h-1.5 bg-slate-950 rounded-full overflow-hidden">
                              <motion.div
                                animate={{
                                  width: `${job.uploadProgress ?? 0}%`,
                                }}
                                transition={{ duration: 0.3 }}
                                className={`h-full rounded-full ${
                                  preferredStorage === "mega"
                                    ? "bg-red-500"
                                    : "bg-emerald-500"
                                }`}
                              />
                            </div>
                          </>
                        ) : job.status === "error" ? (
                          <div className="text-[10px] text-red-400 font-bold text-center">
                            خطأ في الرفع
                          </div>
                        ) : (
                          <div className="text-[10px] text-slate-700 italic text-center">
                            في انتظار اكتمال التحميل...
                          </div>
                        )}
                      </div>

                      {/* Actions */}
                      <div className="col-span-12 lg:col-span-2 flex lg:flex-row flex-row-reverse items-center justify-between lg:justify-end gap-2 px-1">
                        <StatusBadge
                          status={job.status}
                          storage={preferredStorage}
                        />
                        <div className="flex items-center gap-1">
                          {(job.status === "downloading" ||
                            job.status === "streaming" ||
                            job.status === "paused" ||
                            job.status === "queued") && (
                            <button
                              onClick={() =>
                                handleTogglePause(job.id, job.status)
                              }
                              disabled={togglingIds.has(job.id)}
                              className="p-2 text-slate-600 hover:text-blue-400 hover:bg-blue-500/10 rounded-lg transition-all disabled:opacity-40"
                              title={
                                job.status === "paused"
                                  ? "استكمال"
                                  : "إيقاف مؤقت"
                              }
                            >
                              {togglingIds.has(job.id) ? (
                                <div className="w-4 h-4 border-2 border-blue-500/30 border-t-blue-500 rounded-full animate-spin" />
                              ) : job.status === "paused" ? (
                                <PlayCircle className="w-4 h-4" />
                              ) : (
                                <Pause className="w-4 h-4" />
                              )}
                            </button>
                          )}
                          <button
                            onClick={() => handleDelete(job.id)}
                            disabled={deletingIds.has(job.id)}
                            className="p-2 text-slate-600 hover:text-red-400 hover:bg-red-500/10 rounded-lg transition-all disabled:opacity-40"
                            title="حذف المهمة"
                          >
                            {deletingIds.has(job.id) ? (
                              <div className="w-4 h-4 border-2 border-red-500/30 border-t-red-500 rounded-full animate-spin" />
                            ) : (
                              <Trash2 className="w-4 h-4" />
                            )}
                          </button>
                        </div>
                      </div>

                      {/* Error message */}
                      {job.error && (
                        <div className="col-span-12 bg-red-500/10 border border-red-500/20 text-red-400 p-2.5 rounded-xl text-[11px] font-bold flex items-center gap-2">
                          <AlertCircle className="w-3.5 h-3.5 shrink-0" />
                          <span>{job.error}</span>
                        </div>
                      )}
                    </motion.div>
                  ))
                )}
              </AnimatePresence>
            </div>
          </div>
        </section>
      </main>

      {/* ── FOOTER ── */}
      <footer className="h-10 bg-slate-900 border-t border-slate-800 px-6 flex items-center justify-between text-[10px] text-slate-500 font-mono shrink-0">
        <div className="flex gap-6">
          <span>
            محرك التحميل:{" "}
            <span className="text-blue-400">WebTorrent Turbo</span>
          </span>
          <span>
            الوجهة:{" "}
            <span
              className={
                preferredStorage === "mega" ? "text-red-400" : "text-emerald-400"
              }
            >
              {preferredStorage === "drive" ? "Google Drive" : "Mega.nz"}
            </span>
          </span>
        </div>
        <div className="hidden sm:flex gap-4">
          <span>
            المهام: <span className="text-slate-300">{jobs.length}</span>
          </span>
          <span>
            نشط: <span className="text-blue-400">{activeCount}</span>
          </span>
        </div>
      </footer>

      {/* Folder Picker Modal */}
      <AnimatePresence>
        {showFolderPicker && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-[100] bg-slate-950/80 backdrop-blur-sm flex items-center justify-center p-4"
          >
            <motion.div
              initial={{ scale: 0.95, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.95, opacity: 0 }}
              className="bg-slate-900 border border-slate-800 rounded-2xl shadow-2xl w-full max-w-lg flex flex-col max-h-[80vh]"
            >
              <div className="p-4 border-b border-slate-800 flex items-center justify-between shrink-0">
                <h3 className="font-bold flex items-center gap-2">
                  <FolderOpen className="w-5 h-5 text-blue-400" />
                  اختر مجلد الحفظ في Google Drive
                </h3>
                <button
                  onClick={() => setShowFolderPicker(false)}
                  className="text-slate-500 hover:text-white p-1"
                >
                  <X className="w-5 h-5" />
                </button>
              </div>

              <div className="flex items-center gap-2 p-3 border-b border-slate-800/50 bg-slate-800/20 text-xs overflow-x-auto whitespace-nowrap hide-scrollbar shrink-0">
                {folderHistory.length > 1 && (
                  <button
                    onClick={navigateBack}
                    className="p-1 px-2 text-slate-400 hover:text-white font-bold bg-slate-800 rounded flex items-center gap-1"
                  >
                    رجوع
                  </button>
                )}
                <div className="flex items-center gap-2 text-slate-400">
                  {folderHistory.map((fh, idx) => (
                    <React.Fragment key={idx}>
                      {idx > 0 && <span>/</span>}
                      <span
                        className={
                          idx === folderHistory.length - 1
                            ? "text-slate-200 font-bold"
                            : ""
                        }
                      >
                        {fh.name}
                      </span>
                    </React.Fragment>
                  ))}
                </div>
              </div>

              <div className="p-2 flex-1 overflow-y-auto">
                {loadingFolders ? (
                  <div className="py-10 flex justify-center">
                    <div className="w-8 h-8 border-4 border-blue-500/30 border-t-blue-500 rounded-full animate-spin" />
                  </div>
                ) : driveFolders.length === 0 ? (
                  <div className="space-y-1">
                    <button
                      onClick={() =>
                        handleSelectFolder(
                          currentParentId,
                          folderHistory[folderHistory.length - 1]?.name ||
                            "المجلد الحالي"
                        )
                      }
                      className="w-full text-right px-4 py-3 rounded-xl bg-blue-600/10 border border-blue-500/20 hover:bg-blue-600/20 text-blue-400 font-bold text-sm flex items-center justify-between mb-2"
                    >
                      <span>حفظ في هذا المجلد</span>
                      <CheckCircle2 className="w-4 h-4" />
                    </button>
                    <div className="py-10 text-center text-slate-500 text-sm">
                      لا توجد مجلدات هنا
                    </div>
                  </div>
                ) : (
                  <div className="space-y-1">
                    <button
                      onClick={() =>
                        handleSelectFolder(
                          currentParentId,
                          folderHistory[folderHistory.length - 1]?.name ||
                            "المجلد الحالي"
                        )
                      }
                      className="w-full text-right px-4 py-3 rounded-xl bg-blue-600/10 border border-blue-500/20 hover:bg-blue-600/20 text-blue-400 font-bold text-sm flex items-center justify-between mb-2"
                    >
                      <span>حفظ في هذا المجلد</span>
                      <CheckCircle2 className="w-4 h-4" />
                    </button>
                    {driveFolders.map((folder) => (
                      <div
                        key={folder.id}
                        className="flex items-center justify-between w-full hover:bg-slate-800/50 p-2 rounded-xl group transition-all"
                      >
                        <button
                          onClick={() =>
                            navigateToFolder(folder.id, folder.name)
                          }
                          className="flex-1 text-right flex items-center gap-3"
                        >
                          <FolderOpen className="w-5 h-5 text-blue-400/70" />
                          <span className="text-sm font-medium text-slate-300 group-hover:text-white">
                            {folder.name}
                          </span>
                        </button>
                        <button
                          onClick={() =>
                            handleSelectFolder(folder.id, folder.name)
                          }
                          className="px-3 py-1.5 opacity-0 group-hover:opacity-100 bg-slate-800 hover:bg-blue-600 text-white text-xs rounded-lg transition-all"
                        >
                          اختيار
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Global Toast */}
      <AnimatePresence>
        {toast && (
          <motion.div
            initial={{ opacity: 0, y: 50, scale: 0.9 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 50, scale: 0.9 }}
            className={`fixed bottom-14 left-1/2 -translate-x-1/2 z-50 px-4 py-3 rounded-xl flex items-center gap-3 font-bold text-sm shadow-2xl backdrop-blur-md ${
              toast.type === "error"
                ? "bg-red-500/90 text-white shadow-red-500/20 border border-red-500/50"
                : "bg-emerald-500/90 text-white shadow-emerald-500/20 border border-emerald-500/50"
            }`}
          >
            {toast.type === "error" ? <AlertCircle className="w-5 h-5 flex-shrink-0" /> : <CheckCircle2 className="w-5 h-5 flex-shrink-0" />}
            <span>{toast.message}</span>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ─── STATUS BADGE ─────────────────────────────────────────────────────────────
function StatusBadge({
  status,
  storage,
}: {
  status: Job["status"];
  storage: string;
}) {
  const styles: Record<string, string> = {
    queued: "bg-slate-800 text-slate-400 border-slate-700",
    paused: "bg-orange-500/10 text-orange-400 border-orange-500/20",
    downloading: "bg-blue-500/10 text-blue-400 border-blue-500/20 animate-pulse",
    streaming: "bg-violet-500/10 text-violet-400 border-violet-500/20 animate-pulse",
    uploading:
      storage === "mega"
        ? "bg-red-500/10 text-red-400 border-red-500/20 animate-pulse"
        : "bg-violet-500/10 text-violet-400 border-violet-500/20 animate-pulse",
    completed:
      storage === "mega"
        ? "bg-red-600 text-white border-transparent"
        : "bg-emerald-600 text-white border-transparent",
    error: "bg-red-500/10 text-red-400 border-red-500/20",
  };

  const labels: Record<string, string> = {
    queued: "في الانتظار",
    paused: "متوقف",
    downloading: "تحميل P2P",
    streaming: "⚡ بث مباشر",
    uploading: storage === "mega" ? "رفع Mega" : "رفع Drive",
    completed: "✓ مكتمل",
    error: "⚠ خطأ",
  };

  return (
    <span
      className={`px-2.5 py-1 rounded-md text-[9px] font-black uppercase tracking-wider border ${
        styles[status] ?? styles.error
      }`}
    >
      {labels[status] ?? status}
    </span>
  );
}
