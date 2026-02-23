import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "./api.js";
import { getLabel } from "./i18n.js";

const getInitialLanguage = () => localStorage.getItem("ui_language") || "en";

const emptySubmissionForm = () => ({
  publishing_site: "",
  target_site_id: "",
  target_site_url: "",
  client_name: "",
  source_type: "",
  doc_url: "",
  docx_file: null,
  anchor: "",
  topic: "",
});

const createSubmissionBlock = (id, defaults = {}) => ({
  id,
  ...emptySubmissionForm(),
  ...defaults,
});

const emptyRejectForm = () => ({
  reason_code: "quality_below_standard",
  other_reason: "",
});

const emptyLoginForm = () => ({
  email: "",
  password: "",
});

const emptyLoginFieldErrors = () => ({
  email: false,
  password: false,
});

const emptyResetRequestForm = () => ({
  email: "",
});

const emptyResetConfirmForm = () => ({
  password: "",
  confirm_password: "",
});

const getResetTokenFromUrl = () => {
  const params = new URLSearchParams(window.location.search);
  const token = (params.get("reset_token") || "").trim();
  return token || "";
};

const getResetModeFromUrl = () => {
  const params = new URLSearchParams(window.location.search);
  return params.get("mode") === "reset-request";
};

const emptyAdminUserForm = () => ({
  email: "",
  password: "",
  role: "client",
  is_active: true,
  client_ids: [],
});

const baseApiUrl = import.meta.env.VITE_API_BASE_URL || "";
const defaultClientPortalHost = "clientsportal.elci.live";
const defaultAdminPortalHost = "adminportal.elci.live";
const ADMIN_SECTIONS = ["admin", "websites", "clients", "pending-jobs"];
const CLIENT_SECTIONS = ["dashboard", "guest-posts", "orders"];
const CLIENT_IDLE_LOGOUT_MS = 24 * 60 * 60 * 1000;
const ADMIN_IDLE_LOGOUT_MS = 2 * 60 * 60 * 1000;

const normalizeHost = (raw) => {
  const value = (raw || "").trim();
  if (!value) return "";
  try {
    const parsed = value.includes("://") ? new URL(value) : new URL(`https://${value}`);
    return (parsed.host || "").trim().toLowerCase();
  } catch {
    return value.replace(/^https?:\/\//i, "").replace(/\/.*$/, "").trim().toLowerCase();
  }
};

const clientPortalHost = normalizeHost(import.meta.env.VITE_CLIENT_PORTAL_HOST) || defaultClientPortalHost;
const adminPortalHost = normalizeHost(import.meta.env.VITE_ADMIN_PORTAL_HOST) || defaultAdminPortalHost;

const getDefaultSectionForRole = (role) => (role === "admin" ? "admin" : "dashboard");

const getAllowedSectionsForRole = (role) => (role === "admin" ? ADMIN_SECTIONS : CLIENT_SECTIONS);

const getStoredSectionForRole = (role) => localStorage.getItem(`active_section_${role}`) || "";

const resolveSectionForRole = (role, section) => {
  const allowed = getAllowedSectionsForRole(role);
  return allowed.includes(section) ? section : getDefaultSectionForRole(role);
};

async function readApiError(response, fallbackMessage) {
  const rawBody = await response.text();
  try {
    const payload = rawBody ? JSON.parse(rawBody) : null;
    return payload?.detail || payload?.error || rawBody || fallbackMessage;
  } catch {
    return rawBody || fallbackMessage;
  }
}

export default function App() {
  const [activeSection, setActiveSection] = useState("guest-posts");
  const [language, setLanguage] = useState(getInitialLanguage());
  const [theme, setTheme] = useState(() => localStorage.getItem("theme") || "system");

  const [authLoading, setAuthLoading] = useState(true);
  const [authSubmitting, setAuthSubmitting] = useState(false);
  const [currentUser, setCurrentUser] = useState(null);
  const [loginForm, setLoginForm] = useState(emptyLoginForm());
  const [loginFieldErrors, setLoginFieldErrors] = useState(emptyLoginFieldErrors());
  const [loginFieldShake, setLoginFieldShake] = useState(emptyLoginFieldErrors());
  const [authError, setAuthError] = useState("");
  const [showResetRequestForm, setShowResetRequestForm] = useState(false);
  const [resetRequestForm, setResetRequestForm] = useState(emptyResetRequestForm());
  const [resetRequestSubmitting, setResetRequestSubmitting] = useState(false);
  const [resetRequestMessage, setResetRequestMessage] = useState("");
  const [resetToken, setResetToken] = useState(() => getResetTokenFromUrl());
  const [resetConfirmForm, setResetConfirmForm] = useState(emptyResetConfirmForm());
  const [resetConfirmSubmitting, setResetConfirmSubmitting] = useState(false);
  const [resetConfirmMessage, setResetConfirmMessage] = useState("");
  const [resetMode, setResetMode] = useState(() => getResetModeFromUrl());

  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(null);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [showSubmissionSuccessModal, setShowSubmissionSuccessModal] = useState(false);
  const [showSubmissionErrorModal, setShowSubmissionErrorModal] = useState(false);
  const [submissionErrorCode, setSubmissionErrorCode] = useState("");
  const [submissionErrorMessage, setSubmissionErrorMessage] = useState("");

  const [clients, setClients] = useState([]);
  const [sites, setSites] = useState([]);
  const nextSubmissionBlockIdRef = useRef(2);
  const [submissionBlocks, setSubmissionBlocks] = useState(() => [createSubmissionBlock(1)]);
  const [adminUsers, setAdminUsers] = useState([]);
  const [adminUserForm, setAdminUserForm] = useState(emptyAdminUserForm());
  const [adminUserEdits, setAdminUserEdits] = useState({});
  const [adminLoading, setAdminLoading] = useState(false);
  const [adminSubmitting, setAdminSubmitting] = useState(false);
  const [adminSavingUserId, setAdminSavingUserId] = useState("");
  const [pendingJobs, setPendingJobs] = useState([]);
  const [pendingLoading, setPendingLoading] = useState(false);
  const [publishingJobId, setPublishingJobId] = useState("");
  const [rejectingJobId, setRejectingJobId] = useState("");
  const [regeneratingImageJobId, setRegeneratingImageJobId] = useState("");
  const [openRejectJobId, setOpenRejectJobId] = useState("");
  const [rejectForms, setRejectForms] = useState({});
  const [siteSuggestionsBlockId, setSiteSuggestionsBlockId] = useState(null);
  const [uploadProgressBlockId, setUploadProgressBlockId] = useState(null);
  const inactivityTimerRef = useRef(null);

  const t = useMemo(() => (key) => getLabel(language, key), [language]);

  const getClientTargetSites = () => {
    const client = clients[0];
    if (!client) return [];
    const explicit = Array.isArray(client.target_sites) ? client.target_sites.filter(Boolean) : [];
    if (explicit.length) return explicit;
    if ((client.primary_domain || "").trim() || (client.backlink_url || "").trim()) {
      return [
        {
          id: "legacy-primary",
          target_site_domain: (client.primary_domain || "").trim() || null,
          target_site_url: (client.backlink_url || "").trim() || null,
          is_primary: true,
        },
      ];
    }
    return [];
  };

  const getDefaultSubmissionTargetSite = () => {
    const rows = getClientTargetSites();
    if (!rows.length) return {};
    const primary = rows.find((row) => row?.is_primary) || rows[0];
    return {
      target_site_id: String(primary.id || ""),
      target_site_url: (primary.target_site_url || "").trim(),
    };
  };

  const resetSubmissionBlocks = () => {
    nextSubmissionBlockIdRef.current = 2;
    setSubmissionBlocks([createSubmissionBlock(1, getDefaultSubmissionTargetSite())]);
    setSiteSuggestionsBlockId(null);
    setUploadProgressBlockId(null);
  };

  const resetClientSubmissionState = () => {
    setClients([]);
    setSites([]);
    setError("");
    setSuccess("");
    nextSubmissionBlockIdRef.current = 2;
    setSubmissionBlocks([createSubmissionBlock(1)]);
    setSiteSuggestionsBlockId(null);
    setUploadProgressBlockId(null);
    setShowSubmissionSuccessModal(false);
    setShowSubmissionErrorModal(false);
    setSubmissionErrorCode("");
    setSubmissionErrorMessage("");
  };

  const setSubmissionBlockField = (blockId, field, value) => {
    setSubmissionBlocks((prev) => prev.map((block) => (block.id === blockId ? { ...block, [field]: value } : block)));
  };

  const addSubmissionBlock = (afterBlockId) => {
    setSubmissionBlocks((prev) => {
      const nextId = nextSubmissionBlockIdRef.current;
      nextSubmissionBlockIdRef.current += 1;
      const nextBlock = createSubmissionBlock(nextId, getDefaultSubmissionTargetSite());
      const insertIndex = prev.findIndex((block) => block.id === afterBlockId);
      if (insertIndex < 0) return [...prev, nextBlock];
      return [...prev.slice(0, insertIndex + 1), nextBlock, ...prev.slice(insertIndex + 1)];
    });
  };

  const removeSubmissionBlock = (blockId) => {
    setSubmissionBlocks((prev) => {
      if (prev.length <= 1) return prev;
      const next = prev.filter((block) => block.id !== blockId);
      return next.length ? next : [createSubmissionBlock(1, getDefaultSubmissionTargetSite())];
    });
    setSiteSuggestionsBlockId((prev) => (prev === blockId ? null : prev));
    setUploadProgressBlockId((prev) => (prev === blockId ? null : prev));
  };

  const openSubmissionErrorSupportModal = ({ statusCode, message, blockIndex }) => {
    const code = statusCode ? `HTTP_${statusCode}` : "CLIENT_SUBMIT_UNKNOWN";
    const prefix = typeof blockIndex === "number" ? `Block ${blockIndex + 1}: ` : "";
    setSubmissionErrorCode(code);
    setSubmissionErrorMessage(`${prefix}${message || t("errorRequestFailed")}`);
    setShowSubmissionErrorModal(true);
  };

  const getSubmissionBlockError = (block, { orders, clientName, requiresTargetSite }) => {
    const publishingSite = (block.publishing_site || "").trim();
    if (!publishingSite) return t("errorTargetRequired");
    if (!clientName) return t("errorClientRequired");
    if (requiresTargetSite && !(block.target_site_id || "").trim()) return t("errorClientTargetSiteRequired");
    const sourceType = (block.source_type || "").trim();
    if (!sourceType) return t("errorFileTypeRequired");
    if (sourceType === "google-doc" && !(block.doc_url || "").trim()) return t("errorGoogleDocRequired");
    if (sourceType === "word-doc" && !block.docx_file) return t("errorDocxRequired");
    if (orders && !(block.anchor || "").trim() && !(block.topic || "").trim()) {
      return t("errorOrderAnchorOrTopicRequired");
    }
    return "";
  };

  const buildSubmissionFormData = (block, { orders, clientName }) => {
    const formData = new FormData();
    formData.append("publishing_site", block.publishing_site.trim());
    formData.append("client_name", clientName);
    const targetSiteId = (block.target_site_id || "").trim();
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(targetSiteId)) {
      formData.append("target_site_id", targetSiteId);
    }
    if ((block.target_site_url || "").trim()) formData.append("target_site_url", block.target_site_url.trim());
    formData.append("request_kind", orders ? "order" : "guest_post");
    formData.append("source_type", block.source_type);
    formData.append("execution_mode", "async");
    if ((block.anchor || "").trim()) formData.append("anchor", block.anchor.trim());
    if ((block.topic || "").trim()) formData.append("topic", block.topic.trim());
    if (block.source_type === "google-doc") {
      formData.append("doc_url", (block.doc_url || "").trim());
    } else if (block.docx_file) {
      formData.append("docx_file", block.docx_file);
    }
    return formData;
  };

  useEffect(() => {
    if (currentUser?.role !== "client") return;
    const targetSites = getClientTargetSites();
    if (!targetSites.length) return;
    const validIds = new Set(targetSites.map((row) => String(row.id || "")).filter(Boolean));
    const primary = targetSites.find((row) => row?.is_primary) || targetSites[0];
    setSubmissionBlocks((prev) => prev.map((block) => {
      const currentId = String(block.target_site_id || "");
      if (currentId && validIds.has(currentId)) {
        const selected = targetSites.find((row) => String(row.id || "") === currentId);
        return selected
          ? { ...block, target_site_url: (selected.target_site_url || "").trim() }
          : block;
      }
      return {
        ...block,
        target_site_id: String(primary.id || ""),
        target_site_url: (primary.target_site_url || "").trim(),
      };
    }));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [clients, currentUser?.role]);

  useEffect(() => {
    if (theme === "system") {
      document.documentElement.removeAttribute("data-theme");
    } else {
      document.documentElement.setAttribute("data-theme", theme);
    }
    localStorage.setItem("theme", theme);
  }, [theme]);

  useEffect(() => {
    if (!currentUser) {
      document.title = "Elci Solutions Portal";
      return;
    }
    document.title = currentUser.role === "admin" ? "Admin Portal | Elci Solutions" : "Clients Portal | Elci Solutions";
  }, [currentUser]);

  const loadAll = async (forUser = currentUser) => {
    try {
      setError("");
      const sitesPath = forUser?.role === "client" ? "/sites?status=active&ready_only=true" : "/sites";
      const [clientsData, sitesData] = await Promise.all([api.get("/clients"), api.get(sitesPath)]);
      setClients((clientsData || []).filter((item) => item.status === "active"));
      setSites((sitesData || []).filter((item) => item.status === "active"));
    } catch (err) {
      setError(err.message);
    }
  };

  const hydrateAdminUserEdits = (users) => {
    const next = {};
    for (const user of users || []) {
      next[user.id] = {
        role: user.role,
        is_active: Boolean(user.is_active),
        password: "",
        client_ids: [...(user.client_ids || [])],
      };
    }
    setAdminUserEdits(next);
  };

  const loadAdminUsers = async (forUser = currentUser) => {
    if (forUser?.role !== "admin") return;
    try {
      setAdminLoading(true);
      const users = await api.get("/admin/users");
      const normalized = users || [];
      setAdminUsers(normalized);
      hydrateAdminUserEdits(normalized);
    } catch (err) {
      setError(err.message);
    } finally {
      setAdminLoading(false);
    }
  };

  const loadPendingJobs = async (forUser = currentUser) => {
    if (forUser?.role !== "admin") return;
    try {
      setPendingLoading(true);
      const items = await api.get("/jobs/pending");
      setPendingJobs(items || []);
    } catch (err) {
      setError(err.message);
    } finally {
      setPendingLoading(false);
    }
  };

  const publishPendingJob = async (jobId) => {
    try {
      setPublishingJobId(jobId);
      setError("");
      setSuccess("");
      await api.post(`/jobs/${jobId}/publish`, {});
      await loadPendingJobs();
      setSuccess(t("adminPublishedSuccess"));
    } catch (err) {
      setError(err.message);
    } finally {
      setPublishingJobId("");
    }
  };

  const getRejectForm = (jobId) => rejectForms[jobId] || emptyRejectForm();

  const setRejectFormField = (jobId, field, value) => {
    setRejectForms((prev) => ({
      ...prev,
      [jobId]: {
        ...getRejectForm(jobId),
        [field]: value,
      },
    }));
  };

  const rejectPendingJob = async (jobId) => {
    try {
      const draft = getRejectForm(jobId);
      if (draft.reason_code === "other" && !(draft.other_reason || "").trim()) {
        setError(t("rejectOtherReasonRequired"));
        return;
      }
      setRejectingJobId(jobId);
      setError("");
      setSuccess("");
      await api.post(`/jobs/${jobId}/reject`, {
        reason_code: draft.reason_code,
        other_reason: draft.reason_code === "other" ? draft.other_reason.trim() : null,
      });
      await loadPendingJobs();
      setOpenRejectJobId("");
      setRejectForms((prev) => {
        const next = { ...prev };
        delete next[jobId];
        return next;
      });
      setSuccess(t("adminRejectedSuccess"));
    } catch (err) {
      setError(err.message);
    } finally {
      setRejectingJobId("");
    }
  };

  const regeneratePendingJobImage = async (jobId) => {
    try {
      setRegeneratingImageJobId(jobId);
      setError("");
      setSuccess("");
      await api.post(`/jobs/${jobId}/regenerate-image`, {});
      await loadPendingJobs();
      setSuccess(t("adminImageRegeneratedSuccess"));
    } catch (err) {
      setError(err.message);
    } finally {
      setRegeneratingImageJobId("");
    }
  };

  const postMultipartWithProgress = (url, formData) => (
    new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open("POST", url, true);
      xhr.withCredentials = true;

      xhr.upload.onprogress = (event) => {
        if (!event.lengthComputable) return;
        const nextProgress = Math.max(0, Math.min(100, Math.round((event.loaded / event.total) * 100)));
        setUploadProgress(nextProgress);
      };

      xhr.onerror = () => {
        const networkError = new Error(t("errorBackendUnreachable"));
        networkError.statusCode = 0;
        reject(networkError);
      };

      xhr.onload = () => {
        const text = xhr.responseText || "";
        let payload = null;
        try {
          payload = text ? JSON.parse(text) : null;
        } catch {
          payload = null;
        }

        if (xhr.status >= 200 && xhr.status < 300) {
          resolve(payload || {});
          return;
        }
        const message = payload?.detail || payload?.error || text || t("errorRequestFailed");
        const requestError = new Error(message);
        requestError.statusCode = xhr.status;
        reject(requestError);
      };

      xhr.send(formData);
    })
  );

  const getDraftReviewUrl = (item) => {
    const jobId = (item?.job_id || "").toString().trim();
    const apiBase = (baseApiUrl || "").trim().replace(/\/+$/, "");
    if (jobId) {
      return apiBase ? `${apiBase}/jobs/${encodeURIComponent(jobId)}/draft-preview` : `/jobs/${encodeURIComponent(jobId)}/draft-preview`;
    }
    return (item?.wp_post_url || "").trim();
  };

  useEffect(() => {
    const bootstrapAuth = async () => {
      setAuthLoading(true);
      setAuthError("");
      try {
        const response = await fetch(`${baseApiUrl}/auth/me`, {
          method: "GET",
          credentials: "include",
        });
        if (response.status === 401) {
          setCurrentUser(null);
          return;
        }
        if (!response.ok) {
          const message = await readApiError(response, t("errorRequestFailed"));
          throw new Error(message);
        }
        const user = await response.json();
        setCurrentUser(user);
        setActiveSection(resolveSectionForRole(user.role, getStoredSectionForRole(user.role)));
        setLoading(true);
        await loadAll(user);
        if (user.role === "admin") {
          await loadAdminUsers(user);
          await loadPendingJobs(user);
        }
      } catch (err) {
        setCurrentUser(null);
        const message = err?.message || "";
        if (message === "Load failed" || message === "Failed to fetch") {
          setAuthError("");
        } else {
          setAuthError(message);
        }
      } finally {
        setLoading(false);
        setAuthLoading(false);
      }
    };
    bootstrapAuth();
  }, []);

  useEffect(() => {
    if (!currentUser) return;
    const allowedSections = getAllowedSectionsForRole(currentUser.role);
    if (!allowedSections.includes(activeSection)) {
      setActiveSection(getDefaultSectionForRole(currentUser.role));
    }
  }, [currentUser, activeSection]);

  useEffect(() => {
    if (!currentUser) return;
    localStorage.setItem(`active_section_${currentUser.role}`, activeSection);
  }, [currentUser, activeSection]);

  useEffect(() => {
    if (!currentUser) return;
    if (!adminPortalHost || !clientPortalHost || adminPortalHost === clientPortalHost) return;
    const currentHost = (window.location.hostname || "").trim().toLowerCase();
    if (!currentHost) return;

    let targetHost = "";
    if (currentUser.role === "admin" && currentHost === clientPortalHost) {
      targetHost = adminPortalHost;
    } else if (currentUser.role !== "admin" && currentHost === adminPortalHost) {
      targetHost = clientPortalHost;
    }
    if (!targetHost || targetHost === currentHost) return;

    const nextUrl = `${window.location.protocol}//${targetHost}${window.location.pathname}${window.location.search}${window.location.hash}`;
    window.location.replace(nextUrl);
  }, [currentUser]);

  useEffect(() => {
    if (!currentUser || currentUser.role !== "admin") return;
    if (activeSection !== "admin") return;
    if (adminUsers.length > 0) return;
    loadAdminUsers(currentUser);
  }, [currentUser, activeSection]);

  useEffect(() => {
    if (!currentUser || currentUser.role !== "admin") return;
    if (activeSection !== "pending-jobs") return;
    loadPendingJobs();
  }, [currentUser, activeSection]);

  useEffect(() => {
    if (!currentUser) {
      if (inactivityTimerRef.current) {
        window.clearTimeout(inactivityTimerRef.current);
        inactivityTimerRef.current = null;
      }
      return;
    }

    const timeoutMs = currentUser.role === "admin" ? ADMIN_IDLE_LOGOUT_MS : CLIENT_IDLE_LOGOUT_MS;

    const clearInactivityTimer = () => {
      if (!inactivityTimerRef.current) return;
      window.clearTimeout(inactivityTimerRef.current);
      inactivityTimerRef.current = null;
    };

    const forceLogoutForInactivity = async () => {
      try {
        await fetch(`${baseApiUrl}/auth/logout`, {
          method: "POST",
          credentials: "include",
        });
      } catch {
        // Best-effort logout; local state is still cleared.
      }

      setCurrentUser(null);
      setAdminUsers([]);
      setAdminUserEdits({});
      setAdminUserForm(emptyAdminUserForm());
      resetClientSubmissionState();
      setAuthError("Logged out due to inactivity.");
    };

    const resetInactivityTimer = () => {
      clearInactivityTimer();
      inactivityTimerRef.current = window.setTimeout(forceLogoutForInactivity, timeoutMs);
    };

    const activityEvents = ["mousemove", "mousedown", "keydown", "scroll", "touchstart"];
    for (const eventName of activityEvents) {
      window.addEventListener(eventName, resetInactivityTimer, { passive: true });
    }

    resetInactivityTimer();

    return () => {
      clearInactivityTimer();
      for (const eventName of activityEvents) {
        window.removeEventListener(eventName, resetInactivityTimer);
      }
    };
  }, [currentUser]);

  const handleLogin = async (event) => {
    event.preventDefault();
    setAuthError("");
    const email = loginForm.email.trim().toLowerCase();
    const password = loginForm.password;
    const nextErrors = {
      email: !email,
      password: !password,
    };
    if (nextErrors.email || nextErrors.password) {
      setLoginFieldErrors(nextErrors);
      setLoginFieldShake(nextErrors);
      setTimeout(() => setLoginFieldShake(emptyLoginFieldErrors()), 350);
      return;
    }

    try {
      setAuthSubmitting(true);
      const response = await fetch(`${baseApiUrl}/auth/login`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });
      if (!response.ok) {
        if (response.status === 401 || response.status === 403) {
          throw new Error(t("errorWrongCredentials"));
        }
        const message = await readApiError(response, t("errorRequestFailed"));
        throw new Error(message || t("errorRequestFailed"));
      }

      const payload = await response.json();
      const user = payload?.user || null;
      if (!user) {
        throw new Error(t("errorRequestFailed"));
      }

      setCurrentUser(user);
      setLoginForm(emptyLoginForm());
      setLoginFieldErrors(emptyLoginFieldErrors());
      setLoginFieldShake(emptyLoginFieldErrors());
      setShowResetRequestForm(false);
      setResetRequestMessage("");
      setResetConfirmMessage("");
      setActiveSection(resolveSectionForRole(user.role, getStoredSectionForRole(user.role)));
      setLoading(true);
      await loadAll(user);
      if (user.role === "admin") {
        await loadAdminUsers(user);
        await loadPendingJobs(user);
      }
    } catch (err) {
      const message = err?.message || "";
      if (message === "Load failed" || message === "Failed to fetch") {
        setAuthError(t("errorBackendUnreachable"));
      } else {
        setAuthError(message);
      }
    } finally {
      setLoading(false);
      setAuthSubmitting(false);
    }
  };

  const requestPasswordReset = async (event) => {
    event.preventDefault();
    setAuthError("");
    setResetRequestMessage("");
    const email = resetRequestForm.email.trim().toLowerCase();
    if (!email) {
      setAuthError(t("errorEmailRequired"));
      return;
    }
    try {
      setResetRequestSubmitting(true);
      const response = await fetch(`${baseApiUrl}/auth/password-reset/request`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email }),
      });
      if (!response.ok) {
        const detail = await readApiError(response, t("errorRequestFailed"));
        throw new Error(detail || t("errorRequestFailed"));
      }
      const payload = await response.json();
      setResetRequestMessage(payload?.message || t("passwordResetSent"));
      setResetRequestForm(emptyResetRequestForm());
    } catch (err) {
      setAuthError(err?.message || t("errorRequestFailed"));
    } finally {
      setResetRequestSubmitting(false);
    }
  };

  const confirmPasswordReset = async (event) => {
    event.preventDefault();
    setAuthError("");
    setResetConfirmMessage("");
    const password = (resetConfirmForm.password || "").trim();
    const confirmPassword = (resetConfirmForm.confirm_password || "").trim();
    if (!password || !confirmPassword) {
      setAuthError(t("errorPasswordRequired"));
      return;
    }
    if (password !== confirmPassword) {
      setAuthError(t("errorPasswordMismatch"));
      return;
    }
    try {
      setResetConfirmSubmitting(true);
      const response = await fetch(`${baseApiUrl}/auth/password-reset/confirm`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token: resetToken, new_password: password }),
      });
      if (!response.ok) {
        const detail = await readApiError(response, t("errorRequestFailed"));
        throw new Error(detail || t("errorRequestFailed"));
      }
      const payload = await response.json();
      setResetConfirmMessage(payload?.message || t("passwordResetDone"));
      setResetToken("");
      setResetMode(false);
      setResetConfirmForm(emptyResetConfirmForm());
      const params = new URLSearchParams(window.location.search);
      params.delete("reset_token");
      params.delete("mode");
      const next = `${window.location.pathname}${params.toString() ? `?${params.toString()}` : ""}`;
      window.history.replaceState({}, "", next);
    } catch (err) {
      setAuthError(err?.message || t("errorRequestFailed"));
    } finally {
      setResetConfirmSubmitting(false);
    }
  };

  const handleLogout = async () => {
    setAuthError("");
    try {
      await fetch(`${baseApiUrl}/auth/logout`, {
        method: "POST",
        credentials: "include",
      });
    } catch {
      // Best-effort logout; local state is still cleared.
    }
    setCurrentUser(null);
    setAdminUsers([]);
    setAdminUserEdits({});
    setAdminUserForm(emptyAdminUserForm());
    resetClientSubmissionState();
  };

  const submitGuestPost = async (event) => {
    event.preventDefault();
    setError("");
    setSuccess("");
    setShowSubmissionErrorModal(false);
    setSubmissionErrorCode("");
    setSubmissionErrorMessage("");

    if (activeSection === "admin") {
      setError(t("errorSelectClientSection"));
      return;
    }
    const resolvedClientName = ((clients[0]?.name) || "").trim();
    const blocks = submissionBlocks;
    const requiresTargetSiteSelection = getClientTargetSites().length > 0;
    for (let index = 0; index < blocks.length; index += 1) {
      const validationError = getSubmissionBlockError(blocks[index], {
        orders: isOrders,
        clientName: resolvedClientName,
        requiresTargetSite: requiresTargetSiteSelection,
      });
      if (validationError) {
        setError(`Block ${index + 1}: ${validationError}`);
        return;
      }
    }

    try {
      setSubmitting(true);
      for (let index = 0; index < blocks.length; index += 1) {
        const block = blocks[index];
        const formData = buildSubmissionFormData(block, {
          orders: isOrders,
          clientName: resolvedClientName,
        });
        try {
          if (block.source_type === "word-doc" && block.docx_file) {
            setUploadProgressBlockId(block.id);
            setUploadProgress(0);
            await postMultipartWithProgress(`${baseApiUrl}/automation/guest-post-webhook`, formData);
            setUploadProgress(100);
          } else {
            setUploadProgressBlockId(null);
            const response = await fetch(`${baseApiUrl}/automation/guest-post-webhook`, {
              method: "POST",
              credentials: "include",
              body: formData,
            });

            if (!response.ok) {
              const detail = await readApiError(response, t("errorRequestFailed"));
              const requestError = new Error(detail || t("errorRequestFailed"));
              requestError.statusCode = response.status;
              throw requestError;
            }
            await response.json().catch(() => ({}));
          }
        } catch (err) {
          setError(`Block ${index + 1}: ${err?.message || t("errorRequestFailed")}`);
          openSubmissionErrorSupportModal({
            statusCode: err?.statusCode,
            message: err?.message,
            blockIndex: index,
          });
          throw err;
        }
      }

      setShowSubmissionSuccessModal(true);
      resetSubmissionBlocks();
    } catch (err) {
      if (!err) {
        setError(t("errorRequestFailed"));
      }
    } finally {
      setSubmitting(false);
      setUploadProgress(null);
      setUploadProgressBlockId(null);
    }
  };

  const setAdminUserEditField = (userId, field, value) => {
    setAdminUserEdits((prev) => ({
      ...prev,
      [userId]: {
        ...(prev[userId] || {}),
        [field]: value,
      },
    }));
  };

  const toggleAdminUserClient = (userId, clientId) => {
    setAdminUserEdits((prev) => {
      const current = prev[userId] || { role: "client", is_active: true, password: "", client_ids: [] };
      const set = new Set(current.client_ids || []);
      if (set.has(clientId)) set.delete(clientId);
      else set.add(clientId);
      return {
        ...prev,
        [userId]: {
          ...current,
          client_ids: Array.from(set),
        },
      };
    });
  };

  const toggleCreateUserClient = (clientId) => {
    setAdminUserForm((prev) => {
      const set = new Set(prev.client_ids || []);
      if (set.has(clientId)) set.delete(clientId);
      else set.add(clientId);
      return {
        ...prev,
        client_ids: Array.from(set),
      };
    });
  };

  const createAdminUser = async (event) => {
    event.preventDefault();
    setError("");
    setSuccess("");
    const email = adminUserForm.email.trim().toLowerCase();
    const password = adminUserForm.password;
    if (!email || !password) {
      setError(t("adminUserCreateRequired"));
      return;
    }

    const payload = {
      email,
      password,
      role: adminUserForm.role,
      is_active: adminUserForm.is_active,
      client_ids: adminUserForm.role === "client" ? adminUserForm.client_ids : [],
    };
    try {
      setAdminSubmitting(true);
      await api.post("/admin/users", payload);
      setAdminUserForm(emptyAdminUserForm());
      await loadAdminUsers(currentUser);
      setSuccess(t("adminUserCreated"));
    } catch (err) {
      setError(err.message);
    } finally {
      setAdminSubmitting(false);
    }
  };

  const saveAdminUser = async (userId) => {
    const draft = adminUserEdits[userId];
    if (!draft) return;
    const payload = {
      role: draft.role,
      is_active: Boolean(draft.is_active),
      client_ids: draft.role === "client" ? draft.client_ids || [] : [],
    };
    if ((draft.password || "").trim()) {
      payload.password = draft.password.trim();
    }
    try {
      setAdminSavingUserId(userId);
      setError("");
      setSuccess("");
      await api.patch(`/admin/users/${userId}`, payload);
      await loadAdminUsers(currentUser);
      setSuccess(t("adminUserSaved"));
    } catch (err) {
      setError(err.message);
    } finally {
      setAdminSavingUserId("");
    }
  };

  if (authLoading) {
    return (
      <div className="auth-shell">
        <div className="auth-loading">{t("loading")}</div>
      </div>
    );
  }

  if (!currentUser) {
    return (
      <AuthGate
        t={t}
        language={language}
        onLanguageChange={(next) => {
          setLanguage(next);
          localStorage.setItem("ui_language", next);
        }}
        theme={theme}
        onThemeChange={setTheme}
        loginForm={loginForm}
        onLoginFormChange={setLoginForm}
        loginFieldErrors={loginFieldErrors}
        loginFieldShake={loginFieldShake}
        onClearLoginFieldError={(field) => {
          setLoginFieldErrors((prev) => ({ ...prev, [field]: false }));
        }}
        onLoginSubmit={handleLogin}
        submittingLogin={authSubmitting}
        error={authError}
        onShowResetRequest={() => {
          const params = new URLSearchParams(window.location.search);
          params.set("mode", "reset-request");
          window.history.replaceState({}, "", `${window.location.pathname}?${params.toString()}`);
          setResetMode(true);
          setShowResetRequestForm(true);
        }}
        showResetRequestForm={showResetRequestForm || resetMode}
        resetMode={resetMode}
        resetRequestForm={resetRequestForm}
        onResetRequestFormChange={setResetRequestForm}
        onResetRequestSubmit={requestPasswordReset}
        submittingResetRequest={resetRequestSubmitting}
        resetRequestMessage={resetRequestMessage}
        resetToken={resetToken}
        resetConfirmForm={resetConfirmForm}
        onResetConfirmFormChange={setResetConfirmForm}
        onResetConfirmSubmit={confirmPasswordReset}
        submittingResetConfirm={resetConfirmSubmitting}
        resetConfirmMessage={resetConfirmMessage}
      />
    );
  }

  const isAdminSection = activeSection === "admin";
  const isWebsitesSection = activeSection === "websites";
  const isClientsSection = activeSection === "clients";
  const isAdminUser = currentUser.role === "admin";
  const isAdminPendingSection = isAdminUser && activeSection === "pending-jobs";
  const isClientDashboardSection = !isAdminUser && activeSection === "dashboard";
  const isOrders = activeSection === "orders";
  const isGuestPostsSection = activeSection === "guest-posts";
  const activeClient = clients[0] || null;
  const resolvedClientName = ((activeClient?.name) || "").trim();
  const clientTargetSites = getClientTargetSites();
  const clientTargetSitesCount = clientTargetSites.length;
  const clientTargetSitePreview = clientTargetSites
    .slice(0, 4)
    .map((row) => {
      const rawUrl = (row?.target_site_url || "").trim();
      const rawDomain = (row?.target_site_domain || "").trim();
      if (rawDomain) return rawDomain;
      if (!rawUrl) return "";
      try {
        const parsed = rawUrl.includes("://") ? new URL(rawUrl) : new URL(`https://${rawUrl}`);
        return (parsed.hostname || "").replace(/^www\./i, "");
      } catch {
        return rawUrl.replace(/^https?:\/\//i, "").replace(/^www\./i, "").replace(/\/.*$/, "");
      }
    })
    .filter(Boolean)
    .join(" • ");
  const adminCount = adminUsers.filter((item) => item.role === "admin").length;
  const clientUserCount = adminUsers.filter((item) => item.role === "client").length;
  const inactiveUserCount = adminUsers.filter((item) => !item.is_active).length;
  const mappedClientUserCount = adminUsers.filter((item) => item.role === "client" && (item.client_ids || []).length > 0).length;
  const unmappedClientUserCount = adminUsers.filter((item) => item.role === "client" && (item.client_ids || []).length === 0).length;
  const activeCoveragePercent = clients.length
    ? Math.round((mappedClientUserCount / Math.max(clientUserCount, 1)) * 100)
    : 0;
  const getFilteredSitesForQuery = (query) => {
    const normalizedQuery = (query || "").trim().toLowerCase();
    return sites.filter((site) => {
      if (!normalizedQuery) return true;
      const url = (site.site_url || "").toLowerCase();
      const name = (site.name || "").toLowerCase();
      return url.includes(normalizedQuery) || name.includes(normalizedQuery);
    });
  };
  const suggestedGuestPostsMonthly = Math.max(4, Math.min(36, sites.length * 2));
  const suggestedOrdersMonthly = Math.max(2, Math.min(18, Math.ceil(Math.max(sites.length, 1) / 2)));
  const weeklyCadenceText = sites.length >= 14
    ? t("clientDashboardCadenceLarge")
    : sites.length >= 6
      ? t("clientDashboardCadenceMedium")
      : t("clientDashboardCadenceSmall");
  const siteDomainSamples = sites
    .map((site) => {
      const raw = (site.site_url || "").trim();
      if (!raw) return "";
      try {
        const url = raw.includes("://") ? new URL(raw) : new URL(`https://${raw}`);
        return (url.hostname || "").replace(/^www\./i, "");
      } catch {
        return raw.replace(/^https?:\/\//i, "").replace(/^www\./i, "").replace(/\/.*$/, "");
      }
    })
    .filter(Boolean);
  const uniqueSiteDomains = Array.from(new Set(siteDomainSamples));
  const siteMixPreview = uniqueSiteDomains.slice(0, 4).join(" • ");
  const readySitesLabel = sites.length > 0 ? t("clientDashboardReadySitesYes") : t("clientDashboardReadySitesNo");

  return (
    <div className="app-shell">
      <Sidebar
        t={t}
        userRole={currentUser.role}
        activeSection={activeSection}
        onSectionChange={setActiveSection}
        pendingJobsCount={pendingJobs.length}
      />

      <div className="app-main">
        <div className="header">
          <div className="title">{isAdminUser ? t("heroAdminPanel") : t("clientsPortal")}</div>
          <div className="inline header-actions">
            <div className="user-chip">
              <span>{`Hey ${
                currentUser.role === "admin"
                  ? (currentUser.full_name || currentUser.email)
                  : (resolvedClientName || t("roleClient"))
              }!`}</span>
            </div>
            <LanguageToggle
              language={language}
              onChange={(next) => {
                setLanguage(next);
                localStorage.setItem("ui_language", next);
              }}
            />
            <ThemeToggle theme={theme} onChange={setTheme} t={t} />
            <button className="btn secondary" type="button" onClick={handleLogout}>
              {t("logout")}
            </button>
          </div>
        </div>

        <div className={`container ${isAdminPendingSection ? "container-wide" : ""}`.trim()}>
          {!isAdminUser && (isGuestPostsSection || isOrders) ? (
            <div className="hero">
              <h1>{isOrders ? t("heroCreateOrder") : t("heroCreateGuestPost")}</h1>
            </div>
          ) : null}

          {isAdminSection ? (
            <div className="stats-grid admin-kpi-grid">
              <div className="stat-card">
                <span className="stat-label">{t("statActiveSites")}</span>
                <strong>{sites.length}</strong>
              </div>
              <div className="stat-card">
                <span className="stat-label">{t("statActiveClients")}</span>
                <strong>{clients.length}</strong>
              </div>
              <div className="stat-card">
                <span className="stat-label">{t("kpiTotalUsers")}</span>
                <strong>{adminUsers.length}</strong>
              </div>
              <div className="stat-card">
                <span className="stat-label">{t("kpiAdmins")}</span>
                <strong>{adminCount}</strong>
              </div>
              <div className="stat-card">
                <span className="stat-label">{t("kpiClientUsers")}</span>
                <strong>{clientUserCount}</strong>
              </div>
              <div className="stat-card">
                <span className="stat-label">{t("kpiInactiveUsers")}</span>
                <strong>{inactiveUserCount}</strong>
              </div>
              <div className="stat-card">
                <span className="stat-label">{t("kpiMappedClientUsers")}</span>
                <strong>{mappedClientUserCount}</strong>
              </div>
              <div className="stat-card">
                <span className="stat-label">{t("kpiUnmappedClientUsers")}</span>
                <strong>{unmappedClientUserCount}</strong>
              </div>
            </div>
          ) : null}

          {loading ? <div className="panel muted-text">{t("loading")}</div> : null}
          {error && error !== "Load failed" && error !== "Failed to fetch" ? <div className="panel error">{error}</div> : null}
          {success ? <div className="panel success">{success}</div> : null}

          {isAdminSection ? (
            <div className="panel form-panel admin-dashboard-panel">
              <div className="admin-summary-grid">
                <div className="panel admin-summary-card">
                  <h3>{t("adminCoverageTitle")}</h3>
                  <p className="muted-text">{t("adminCoverageBody")}</p>
                  <strong className="admin-summary-number">{activeCoveragePercent}%</strong>
                </div>
                <div className="panel admin-summary-card">
                  <h3>{t("adminHealthTitle")}</h3>
                  <p className="muted-text">{t("adminHealthBody")}</p>
                  <strong className="admin-summary-number">
                    {inactiveUserCount === 0 ? t("adminHealthy") : t("adminNeedsAttention")}
                  </strong>
                </div>
              </div>

              {adminLoading ? <p className="muted-text">{t("loading")}</p> : null}
            </div>
          ) : isClientDashboardSection ? (
            <ClientDashboardPanel
              t={t}
              clientName={resolvedClientName}
              siteCount={sites.length}
              targetSiteCount={clientTargetSitesCount}
              targetSitePreview={clientTargetSitePreview}
              readySitesLabel={readySitesLabel}
              suggestedGuestPostsMonthly={suggestedGuestPostsMonthly}
              suggestedOrdersMonthly={suggestedOrdersMonthly}
              weeklyCadenceText={weeklyCadenceText}
              siteMixPreview={siteMixPreview}
              uniqueDomainCount={uniqueSiteDomains.length}
              onOpenGuestPosts={() => setActiveSection("guest-posts")}
              onOpenOrders={() => setActiveSection("orders")}
            />
          ) : isWebsitesSection ? (
            <div className="panel form-panel">
              <h2>{t("navWebsites")}</h2>
              <div className="admin-entity-list">
                {sites.map((site) => (
                  <div key={site.id} className="admin-entity-card">
                    <strong>{site.name}</strong>
                    <span className="muted-text">{site.site_url}</span>
                  </div>
                ))}
              </div>
            </div>
          ) : isClientsSection ? (
            <div className="panel form-panel">
              <h2>{t("navClients")}</h2>
              <div className="admin-entity-list">
                {clients.map((client) => (
                  <div key={client.id} className="admin-entity-card">
                    <strong>{client.name}</strong>
                    <span className="muted-text">{client.email || client.phone_number || "-"}</span>
                  </div>
                ))}
              </div>
            </div>
          ) : isAdminPendingSection ? (
            <div className="panel form-panel pending-panel">
              <h2>{t("navPendingJobs")}</h2>
              {pendingLoading ? <p className="muted-text">{t("loading")}</p> : null}
              {!pendingLoading && pendingJobs.length === 0 ? (
                <p className="muted-text">
                  {t("pendingJobsEmpty")}
                </p>
              ) : null}
              <div className="pending-list-table">
                <div className="pending-list-header">
                  <span>{t("createdByLabel")}</span>
                  <span>{t("targetWebsiteLabel")}</span>
                  <span>{t("contentTitleLabel")}</span>
                  <span>{t("jobTypeLabel")}</span>
                  <span>{t("actionsLabel")}</span>
                </div>
                {pendingJobs.map((item) => {
                  const draftReviewUrl = getDraftReviewUrl(item);
                  const requestKind = item.request_kind === "order" ? "order" : "guest_post";
                  return (
                    <div key={item.job_id} className="pending-item-wrap">
                      <div className="pending-item-row">
                        <span>{item.client_name}</span>
                        <span>{item.site_url || item.site_name}</span>
                        <span>{item.content_title || t("contentTitleFallback")}</span>
                        <span>{requestKind === "order" ? t("jobTypeOrder") : t("jobTypeGuestPost")}</span>
                        <div className="pending-actions">
                          {draftReviewUrl ? (
                            <a className="btn secondary" href={draftReviewUrl} target="_blank" rel="noreferrer">
                              {t("viewDraft")}
                            </a>
                          ) : (
                            <span className="muted-text small-text">{t("draftLinkUnavailable")}</span>
                          )}
                          <button
                            className="btn secondary"
                            type="button"
                            onClick={() => regeneratePendingJobImage(item.job_id)}
                            disabled={!item.wp_post_id || publishingJobId === item.job_id || rejectingJobId === item.job_id || regeneratingImageJobId === item.job_id}
                          >
                            {regeneratingImageJobId === item.job_id ? t("regeneratingImage") : t("regeneratePostImage")}
                          </button>
                          <button
                            className="btn"
                            type="button"
                            onClick={() => publishPendingJob(item.job_id)}
                            disabled={!item.wp_post_id || publishingJobId === item.job_id || rejectingJobId === item.job_id || regeneratingImageJobId === item.job_id}
                          >
                            {publishingJobId === item.job_id ? t("publishing") : t("publish")}
                          </button>
                          <button
                            className="btn danger"
                            type="button"
                            onClick={() => {
                              setOpenRejectJobId((prev) => (prev === item.job_id ? "" : item.job_id));
                              setRejectForms((prev) => ({
                                ...prev,
                                [item.job_id]: prev[item.job_id] || emptyRejectForm(),
                              }));
                            }}
                            disabled={publishingJobId === item.job_id || rejectingJobId === item.job_id || regeneratingImageJobId === item.job_id}
                          >
                            {t("reject")}
                          </button>
                        </div>
                      </div>
                      {openRejectJobId === item.job_id ? (
                        <div className="pending-reject-panel">
                          <label>{t("rejectReasonLabel")}</label>
                          <select
                            value={getRejectForm(item.job_id).reason_code}
                            onChange={(e) => setRejectFormField(item.job_id, "reason_code", e.target.value)}
                          >
                            <option value="quality_below_standard">{t("rejectReasonQuality")}</option>
                            <option value="policy_or_compliance_issue">{t("rejectReasonPolicy")}</option>
                            <option value="seo_or_link_issue">{t("rejectReasonSeo")}</option>
                            <option value="format_or_structure_issue">{t("rejectReasonFormat")}</option>
                            <option value="other">{t("rejectReasonOther")}</option>
                          </select>
                          {getRejectForm(item.job_id).reason_code === "other" ? (
                            <div>
                              <label>{t("rejectOtherLabel")}</label>
                              <textarea
                                rows={3}
                                value={getRejectForm(item.job_id).other_reason}
                                onChange={(e) => setRejectFormField(item.job_id, "other_reason", e.target.value)}
                                placeholder={t("rejectOtherPlaceholder")}
                              />
                            </div>
                          ) : null}
                          <div className="pending-reject-actions">
                            <button
                              className="btn secondary"
                              type="button"
                              onClick={() => setOpenRejectJobId("")}
                              disabled={rejectingJobId === item.job_id}
                            >
                              {t("close")}
                            </button>
                            <button
                              className="btn danger"
                              type="button"
                              onClick={() => rejectPendingJob(item.job_id)}
                              disabled={rejectingJobId === item.job_id || regeneratingImageJobId === item.job_id}
                            >
                              {rejectingJobId === item.job_id ? t("rejecting") : t("confirmReject")}
                            </button>
                          </div>
                        </div>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            </div>
          ) : (
            <div className="panel form-panel">
              <h2>{isOrders ? t("formOrder") : t("formSubmission")}</h2>
              <form className="guest-form" onSubmit={submitGuestPost}>
                <div className="submission-blocks">
                  {submissionBlocks.map((block, blockIndex) => {
                    const blockFilteredSites = getFilteredSitesForQuery(block.publishing_site);
                    const selectedClientTargetSite = clientTargetSites.find((row) => String(row.id || "") === String(block.target_site_id || ""));
                    const showAddControl = blockIndex === submissionBlocks.length - 1;
                    const showRemoveControl = blockIndex > 0;
                    return (
                      <div key={block.id} className="submission-block-wrap">
                        <div className="submission-block panel">
                          <div className="submission-block-header">
                            <h3>{`${t("requestBlockLabel")} ${blockIndex + 1}`}</h3>
                          </div>

                          {clientTargetSitesCount > 0 ? (
                            <div>
                              <label>{t("targetSiteForBacklink")}</label>
                              <select
                                value={block.target_site_id || ""}
                                onChange={(e) => {
                                  const nextId = e.target.value;
                                  const nextTarget = clientTargetSites.find((row) => String(row.id || "") === nextId);
                                  setSubmissionBlocks((prev) => prev.map((item) => (
                                    item.id === block.id
                                      ? {
                                          ...item,
                                          target_site_id: nextId,
                                          target_site_url: (nextTarget?.target_site_url || "").trim(),
                                        }
                                      : item
                                  )));
                                }}
                                required
                              >
                                <option value="">{t("selectTargetSite")}</option>
                                {clientTargetSites.map((row) => {
                                  const optionId = String(row.id || "");
                                  const domainLabel = (row.target_site_domain || "").trim();
                                  const urlLabel = (row.target_site_url || "").trim();
                                  const label = domainLabel || urlLabel || optionId;
                                  return (
                                    <option key={optionId} value={optionId}>
                                      {row.is_primary ? `${label} (${t("primaryLabel")})` : label}
                                    </option>
                                  );
                                })}
                              </select>
                              {selectedClientTargetSite?.target_site_url ? (
                                <p className="muted-text small-text">{selectedClientTargetSite.target_site_url}</p>
                              ) : null}
                            </div>
                          ) : (
                            <div className="panel muted-text small-text">
                              {t("clientTargetSitesMissingHint")}
                            </div>
                          )}

                          <div>
                            <label>{t("targetWebsite")}</label>
                            <div className="site-suggest-wrap">
                              <input
                                value={block.publishing_site}
                                onFocus={() => setSiteSuggestionsBlockId(block.id)}
                                onBlur={() => setTimeout(() => {
                                  setSiteSuggestionsBlockId((prev) => (prev === block.id ? null : prev));
                                }, 120)}
                                onChange={(e) => {
                                  setSubmissionBlockField(block.id, "publishing_site", e.target.value);
                                  setSiteSuggestionsBlockId(block.id);
                                }}
                                placeholder={t("placeholderTargetWebsite")}
                                required
                              />
                              {siteSuggestionsBlockId === block.id && blockFilteredSites.length > 0 ? (
                                <div className="site-suggest-list">
                                  {blockFilteredSites.slice(0, 30).map((site) => (
                                    <button
                                      key={site.id}
                                      type="button"
                                      className="site-suggest-item"
                                      onMouseDown={(event) => {
                                        event.preventDefault();
                                        setSubmissionBlockField(block.id, "publishing_site", site.site_url);
                                        setSiteSuggestionsBlockId(null);
                                      }}
                                    >
                                      <span>{site.site_url}</span>
                                      <span className="muted-text small-text">{site.name}</span>
                                    </button>
                                  ))}
                                </div>
                              ) : null}
                            </div>
                          </div>

                          <div>
                            <label>{t("fileType")}</label>
                            <div className="toggle source-toggle">
                              <button
                                type="button"
                                className={block.source_type === "google-doc" ? "active" : ""}
                                onClick={() => setSubmissionBlockField(block.id, "source_type", "google-doc")}
                              >
                                {t("googleDoc")}
                              </button>
                              <button
                                type="button"
                                className={block.source_type === "word-doc" ? "active" : ""}
                                onClick={() => setSubmissionBlockField(block.id, "source_type", "word-doc")}
                              >
                                {t("docxFile")}
                              </button>
                            </div>
                          </div>

                          {block.source_type === "google-doc" ? (
                            <div>
                              <label>{t("googleDocLink")}</label>
                              <input
                                type="url"
                                value={block.doc_url}
                                onChange={(e) => setSubmissionBlockField(block.id, "doc_url", e.target.value)}
                                placeholder={t("placeholderGoogleDoc")}
                                required
                              />
                            </div>
                          ) : block.source_type === "word-doc" ? (
                            <div>
                              <label>{t("fileUpload")}</label>
                              <input
                                type="file"
                                accept=".doc,.docx"
                                required
                                onChange={(e) => {
                                  const file = e.target.files?.[0] || null;
                                  setSubmissionBlockField(block.id, "docx_file", file);
                                }}
                              />
                            </div>
                          ) : null}

                          {isOrders ? (
                            <>
                              <div>
                                <label>{t("anchor")}</label>
                                <input
                                  type="text"
                                  value={block.anchor}
                                  onChange={(e) => setSubmissionBlockField(block.id, "anchor", e.target.value)}
                                  placeholder={t("placeholderAnchor")}
                                />
                              </div>
                              <div>
                                <label>{t("topic")}</label>
                                <input
                                  type="text"
                                  value={block.topic}
                                  onChange={(e) => setSubmissionBlockField(block.id, "topic", e.target.value)}
                                  placeholder={t("placeholderTopic")}
                                />
                              </div>
                            </>
                          ) : null}

                          {submitting && uploadProgressBlockId === block.id && uploadProgress !== null ? (
                            <div className="upload-meter" aria-live="polite">
                              <div className="upload-meter-row">
                                <span>{t("uploadingFile")}</span>
                                <strong>{uploadProgress}%</strong>
                              </div>
                              <div className="upload-meter-track">
                                <div className="upload-meter-fill" style={{ width: `${uploadProgress}%` }} />
                              </div>
                            </div>
                          ) : null}
                        </div>

                        {(showAddControl || showRemoveControl) ? (
                          <div className="submission-block-controls">
                            {showRemoveControl ? (
                              <button
                                className="btn secondary block-control-btn"
                                type="button"
                                aria-label={t("removeBlock")}
                                onClick={() => removeSubmissionBlock(block.id)}
                                disabled={submitting}
                              >
                                -
                              </button>
                            ) : null}
                            {showAddControl ? (
                              <button
                                className="btn block-control-btn"
                                type="button"
                                aria-label={t("addAnotherBlock")}
                                onClick={() => addSubmissionBlock(block.id)}
                                disabled={submitting}
                              >
                                +
                              </button>
                            ) : null}
                          </div>
                        ) : null}
                      </div>
                    );
                  })}
                </div>

                <button className="btn submit-btn" type="submit" disabled={submitting}>
                  {submitting ? t("submitting") : t("submitForReview")}
                </button>
              </form>
            </div>
          )}
        </div>
      </div>
      <SubmissionSuccessModal
        t={t}
        open={showSubmissionSuccessModal}
        onClose={() => setShowSubmissionSuccessModal(false)}
        onCreateAnother={() => {
          setShowSubmissionSuccessModal(false);
        }}
      />
      <SubmissionErrorModal
        t={t}
        open={showSubmissionErrorModal}
        errorCode={submissionErrorCode}
        errorMessage={submissionErrorMessage}
        onClose={() => setShowSubmissionErrorModal(false)}
      />
    </div>
  );
}

function SubmissionSuccessModal({ t, open, onClose, onCreateAnother }) {
  if (!open) return null;
  return (
    <div className="modal-overlay" role="dialog" aria-modal="true" aria-labelledby="submission-success-title">
      <div className="modal-card panel">
        <h3 id="submission-success-title">{t("submissionSuccessTitle")}</h3>
        <p className="muted-text">{t("submissionSuccessBody")}</p>
        <div className="modal-actions">
          <button className="btn secondary" type="button" onClick={onClose}>
            {t("close")}
          </button>
          <button className="btn" type="button" onClick={onCreateAnother}>
            {t("createAnotherPost")}
          </button>
        </div>
      </div>
    </div>
  );
}

function SubmissionErrorModal({ t, open, errorCode, errorMessage, onClose }) {
  if (!open) return null;
  return (
    <div className="modal-overlay" role="dialog" aria-modal="true" aria-labelledby="submission-error-title">
      <div className="modal-card panel">
        <h3 id="submission-error-title">{t("submissionErrorTitle")}</h3>
        <p className="muted-text">{t("submissionErrorBody")}</p>
        <p className="muted-text">
          <strong>{t("errorCodeLabel")}:</strong> {errorCode || "CLIENT_SUBMIT_UNKNOWN"}
        </p>
        {errorMessage ? <p className="muted-text">{errorMessage}</p> : null}
        <div className="modal-actions">
          <a className="btn secondary" href="mailto:aat@elci.cloud?subject=Portal%20submission%20error%20support">
            {t("contactSupport")}
          </a>
          <button className="btn" type="button" onClick={onClose}>
            {t("close")}
          </button>
        </div>
      </div>
    </div>
  );
}

function ClientDashboardPanel({
  t,
  clientName,
  siteCount,
  targetSiteCount,
  targetSitePreview,
  readySitesLabel,
  suggestedGuestPostsMonthly,
  suggestedOrdersMonthly,
  weeklyCadenceText,
  siteMixPreview,
  uniqueDomainCount,
  onOpenGuestPosts,
  onOpenOrders,
}) {
  return (
    <div className="panel form-panel client-dashboard-panel">
      <div className="client-dashboard-hero">
        <h2>{t("navClientDashboard")}</h2>
        <p className="muted-text">
          {clientName
            ? `${t("clientDashboardWelcomePrefix")} ${clientName}. ${t("clientDashboardWelcomeBody")}`
            : t("clientDashboardWelcomeBody")}
        </p>
      </div>

      <div className="client-dashboard-grid">
        <div className="client-dashboard-card client-dashboard-highlight">
          <span className="stat-label">{t("clientDashboardReadyTargets")}</span>
          <strong>{siteCount}</strong>
          <p className="muted-text">{readySitesLabel}</p>
        </div>

        <div className="client-dashboard-card">
          <h3>{t("clientDashboardTargetSitesTitle")}</h3>
          <p className="muted-text">
            {targetSiteCount > 0
              ? `${targetSiteCount} ${t("clientDashboardTargetSitesCountLabel")}`
              : t("clientDashboardTargetSitesEmpty")}
          </p>
          {targetSitePreview ? <p className="muted-text client-dashboard-sites-preview">{targetSitePreview}</p> : null}
          <p className="muted-text">{t("clientDashboardTargetSitesBody")}</p>
        </div>

        <div className="client-dashboard-card">
          <h3>{t("clientDashboardMomentumTitle")}</h3>
          <p className="muted-text">{t("clientDashboardMomentumBody")}</p>
          <div className="client-dashboard-metrics">
            <div>
              <span className="stat-label">{t("navGuestPosts")}</span>
              <strong>{suggestedGuestPostsMonthly}</strong>
            </div>
            <div>
              <span className="stat-label">{t("navOrders")}</span>
              <strong>{suggestedOrdersMonthly}</strong>
            </div>
          </div>
        </div>

        <div className="client-dashboard-card">
          <h3>{t("clientDashboardCadenceTitle")}</h3>
          <p className="muted-text">{weeklyCadenceText}</p>
          <p className="muted-text">{t("clientDashboardCadenceSupport")}</p>
        </div>

        <div className="client-dashboard-card">
          <h3>{t("clientDashboardBatchTitle")}</h3>
          <p className="muted-text">{t("clientDashboardBatchBody")}</p>
          <ul className="client-dashboard-list">
            <li>{t("clientDashboardBatchTip1")}</li>
            <li>{t("clientDashboardBatchTip2")}</li>
            <li>{t("clientDashboardBatchTip3")}</li>
          </ul>
        </div>

        <div className="client-dashboard-card">
          <h3>{t("clientDashboardMixTitle")}</h3>
          <p className="muted-text">{t("clientDashboardMixBody")}</p>
          <div className="client-dashboard-actions">
            <button className="btn" type="button" onClick={onOpenGuestPosts}>
              {t("clientDashboardCreateGuestPostsCta")}
            </button>
            <button className="btn secondary" type="button" onClick={onOpenOrders}>
              {t("clientDashboardCreateOrdersCta")}
            </button>
          </div>
        </div>

        <div className="client-dashboard-card">
          <h3>{t("clientDashboardCoverageTitle")}</h3>
          <p className="muted-text">
            {uniqueDomainCount > 0
              ? `${uniqueDomainCount} ${t("clientDashboardCoverageDomainsLabel")}`
              : t("clientDashboardCoverageFallback")}
          </p>
          {siteMixPreview ? <p className="muted-text client-dashboard-sites-preview">{siteMixPreview}</p> : null}
          <p className="muted-text">{t("clientDashboardCoverageBody")}</p>
        </div>
      </div>
    </div>
  );
}

function AuthGate({
  t,
  language,
  onLanguageChange,
  theme,
  onThemeChange,
  loginForm,
  onLoginFormChange,
  loginFieldErrors,
  loginFieldShake,
  onClearLoginFieldError,
  onLoginSubmit,
  submittingLogin,
  error,
  onShowResetRequest,
  showResetRequestForm,
  resetMode,
  resetRequestForm,
  onResetRequestFormChange,
  onResetRequestSubmit,
  submittingResetRequest,
  resetRequestMessage,
  resetToken,
  resetConfirmForm,
  onResetConfirmFormChange,
  onResetConfirmSubmit,
  submittingResetConfirm,
  resetConfirmMessage,
}) {
  const inResetConfirmMode = Boolean(resetToken);
  const inResetRequestMode = Boolean(resetMode) && !inResetConfirmMode;

  return (
    <div className="auth-shell">
      <div className="auth-topbar">
        <LanguageToggle language={language} onChange={onLanguageChange} />
        <ThemeToggle theme={theme} onChange={onThemeChange} t={t} />
      </div>

      <div className="auth-card panel">
        <h1>{t("loginTitle")}</h1>
        {inResetConfirmMode ? (
          <form className="auth-form" onSubmit={onResetConfirmSubmit} noValidate>
            <div>
              <label>{t("newPassword")}</label>
              <input
                type="password"
                value={resetConfirmForm.password}
                onChange={(e) => onResetConfirmFormChange((prev) => ({ ...prev, password: e.target.value }))}
                placeholder="••••••••"
                required
              />
            </div>
            <div>
              <label>{t("confirmPassword")}</label>
              <input
                type="password"
                value={resetConfirmForm.confirm_password}
                onChange={(e) => onResetConfirmFormChange((prev) => ({ ...prev, confirm_password: e.target.value }))}
                placeholder="••••••••"
                required
              />
            </div>
            {error && error !== "Load failed" && error !== "Failed to fetch" ? <div className="error">{error}</div> : null}
            {resetConfirmMessage ? <div className="success">{resetConfirmMessage}</div> : null}
            <button className="btn" type="submit" disabled={submittingResetConfirm}>
              {submittingResetConfirm ? t("submitting") : t("resetPassword")}
            </button>
          </form>
        ) : inResetRequestMode ? (
          <form className="auth-form auth-reset-form" onSubmit={onResetRequestSubmit} noValidate>
            <div>
              <label>{t("resetEmailLabel")}</label>
              <input
                type="email"
                value={resetRequestForm.email}
                onChange={(e) => onResetRequestFormChange((prev) => ({ ...prev, email: e.target.value }))}
                placeholder="name@example.com"
                required
              />
            </div>
            {error && error !== "Load failed" && error !== "Failed to fetch" ? <div className="error">{error}</div> : null}
            {resetRequestMessage ? <div className="success">{resetRequestMessage}</div> : null}
            <button className="btn secondary" type="submit" disabled={submittingResetRequest}>
              {submittingResetRequest ? t("submitting") : t("sendResetLink")}
            </button>
          </form>
        ) : (
          <>
            <form className="auth-form" onSubmit={onLoginSubmit} noValidate>
              <div>
                <label>{t("email")}</label>
                <input
                  type="email"
                  value={loginForm.email}
                  className={`${loginFieldErrors.email ? "input-error" : ""} ${loginFieldShake.email ? "input-shake" : ""}`.trim()}
                  onChange={(e) => {
                    onLoginFormChange((prev) => ({ ...prev, email: e.target.value }));
                    if (e.target.value.trim()) onClearLoginFieldError("email");
                  }}
                  placeholder={loginFieldErrors.email ? t("errorEmailRequired") : "name@example.com"}
                />
              </div>
              <div>
                <label>{t("password")}</label>
                <input
                  type="password"
                  value={loginForm.password}
                  className={`${loginFieldErrors.password ? "input-error" : ""} ${loginFieldShake.password ? "input-shake" : ""}`.trim()}
                  onChange={(e) => {
                    onLoginFormChange((prev) => ({ ...prev, password: e.target.value }));
                    if (e.target.value.trim()) onClearLoginFieldError("password");
                  }}
                  placeholder={loginFieldErrors.password ? t("errorPasswordRequired") : "••••••••"}
                />
                <div className="auth-forgot-row">
                  <span className="muted-text small-text">{t("resetPasswordPrefix")}</span>
                  <button className="link-button" type="button" onClick={onShowResetRequest}>
                    {t("hereWord")}
                  </button>
                </div>
              </div>
              {error && error !== "Load failed" && error !== "Failed to fetch" ? <div className="error">{error}</div> : null}
              {resetRequestMessage ? <div className="success">{resetRequestMessage}</div> : null}
              <button className="btn" type="submit" disabled={submittingLogin}>
                {submittingLogin ? t("loggingIn") : t("login")}
              </button>
            </form>
          </>
        )}
      </div>
    </div>
  );
}

function Sidebar({ t, userRole, activeSection, onSectionChange, pendingJobsCount = 0 }) {
  const sections = userRole === "admin"
    ? [
        { id: "admin", label: t("navAdmin") },
        { id: "websites", label: t("navWebsites") },
        { id: "clients", label: t("navClients") },
        { id: "pending-jobs", label: t("navPendingJobs"), badge: pendingJobsCount },
      ]
    : [
        { id: "dashboard", label: t("navClientDashboard") },
        { id: "guest-posts", label: t("navGuestPosts") },
        { id: "orders", label: t("navOrders") },
      ];

  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brand-logo">e</div>
        <div>
          <strong>Elci Solutions</strong>
          <span>Operations Hub</span>
        </div>
      </div>

      <nav className="nav">
        {sections.map((section) => (
          <button
            key={section.id}
            type="button"
            className={`nav-item ${activeSection === section.id ? "active" : ""}`}
            onClick={() => onSectionChange(section.id)}
          >
            <span>{section.label}</span>
            {typeof section.badge === "number" ? <span className="nav-badge">{section.badge}</span> : null}
          </button>
        ))}
      </nav>
    </aside>
  );
}

function LanguageToggle({ language, onChange }) {
  return (
    <div className="inline">
      <span className="icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" role="img" focusable="false">
          <circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" strokeWidth="1.6" />
          <path
            d="M3 12h18M12 3c3 3.2 3 14.8 0 18M12 3c-3 3.2-3 14.8 0 18"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.6"
          />
        </svg>
      </span>
      <div className="toggle">
        {["en", "de"].map((lang) => (
          <button
            key={lang}
            className={language === lang ? "active" : ""}
            onClick={() => onChange(lang)}
            type="button"
          >
            {lang.toUpperCase()}
          </button>
        ))}
      </div>
    </div>
  );
}

function ThemeToggle({ theme, onChange, t }) {
  return (
    <div className="theme-toggle">
      <button
        type="button"
        className={theme === "light" ? "active" : ""}
        onClick={() => onChange("light")}
        aria-label={t("lightTheme")}
      >
        <SunIcon />
      </button>
      <button
        type="button"
        className={theme === "system" ? "active" : ""}
        onClick={() => onChange("system")}
        aria-label={t("systemTheme")}
      >
        <SystemIcon />
      </button>
      <button
        type="button"
        className={theme === "dark" ? "active" : ""}
        onClick={() => onChange("dark")}
        aria-label={t("darkTheme")}
      >
        <MoonIcon />
      </button>
    </div>
  );
}

function SunIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="12" cy="12" r="4.5" fill="none" stroke="currentColor" strokeWidth="1.6" />
      <path
        d="M12 3v2.5M12 18.5V21M4.2 6.2l1.8 1.8M18 18l1.8 1.8M3 12h2.5M18.5 12H21M4.2 17.8l1.8-1.8M18 6l1.8-1.8"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
      />
    </svg>
  );
}

function MoonIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="M14.5 3.6a7.8 7.8 0 1 0 5.9 12.9 9 9 0 1 1-5.9-12.9Z"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function SystemIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <rect x="3" y="4" width="18" height="13" rx="2" fill="none" stroke="currentColor" strokeWidth="1.6" />
      <path
        d="M8 20h8M12 17v3"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
      />
    </svg>
  );
}
