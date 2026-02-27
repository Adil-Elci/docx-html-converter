import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api } from "./api.js";
import { getLabel } from "./i18n.js";

const CREATOR_PHASE_LABELS = [
  "", // unused index 0
  "progressPhase1",
  "progressPhase2",
  "progressPhase3",
  "progressPhase4",
  "progressPhase5",
  "progressPhase6",
  "progressPhase7",
];
const CREATOR_TOTAL_PHASES = 7;

const getInitialLanguage = () => localStorage.getItem("ui_language") || "en";
const getInitialSidebarHidden = () => localStorage.getItem("portal_sidebar_hidden") === "true";

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
  creator_mode: false,
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
const defaultDbUpdaterHost = "updatedb.elci.live";
const ADMIN_SECTIONS = ["admin", "websites", "clients", "pending-jobs", "guest-posts", "orders"];
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
const dbUpdaterHost = normalizeHost(import.meta.env.VITE_DB_UPDATER_HOST) || defaultDbUpdaterHost;

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
  const currentHost = normalizeHost(window.location.hostname || "");
  const isDbUpdaterDomain = Boolean(dbUpdaterHost && currentHost === dbUpdaterHost);
  const [activeSection, setActiveSection] = useState("guest-posts");
  const [language, setLanguage] = useState(getInitialLanguage());
  const [theme, setTheme] = useState(() => localStorage.getItem("theme") || "system");
  const [sidebarHidden, setSidebarHidden] = useState(getInitialSidebarHidden);
  const [systemPrefersDark, setSystemPrefersDark] = useState(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") return true;
    return window.matchMedia("(prefers-color-scheme: dark)").matches;
  });
  const [isNarrowViewport, setIsNarrowViewport] = useState(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") return false;
    return window.matchMedia("(max-width: 1080px)").matches;
  });

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
  const [dbUpdaterFile, setDbUpdaterFile] = useState(null);
  const [dbUpdaterDryRun, setDbUpdaterDryRun] = useState(true);
  const [dbUpdaterDeleteMissingSites, setDbUpdaterDeleteMissingSites] = useState(false);
  const [dbUpdaterForceDeleteMissingSites, setDbUpdaterForceDeleteMissingSites] = useState(false);
  const [dbUpdaterSubmitting, setDbUpdaterSubmitting] = useState(false);
  const [dbUpdaterUploadPercent, setDbUpdaterUploadPercent] = useState(0);
  const [dbUpdaterJobId, setDbUpdaterJobId] = useState("");
  const [dbUpdaterJob, setDbUpdaterJob] = useState(null);
  const [dbUpdaterJobsHistory, setDbUpdaterJobsHistory] = useState([]);
  const [dbUpdaterError, setDbUpdaterError] = useState("");
  const [dbUpdaterSuccess, setDbUpdaterSuccess] = useState("");

  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(null);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [showSubmissionSuccessModal, setShowSubmissionSuccessModal] = useState(false);
  const [showSubmissionErrorModal, setShowSubmissionErrorModal] = useState(false);
  const [submissionErrorCode, setSubmissionErrorCode] = useState("");
  const [submissionErrorMessage, setSubmissionErrorMessage] = useState("");
  const [submissionFieldErrors, setSubmissionFieldErrors] = useState({});
  const [creatorCanceling, setCreatorCanceling] = useState(false);
  const [creatorCancelError, setCreatorCancelError] = useState("");
  const [creatorCancelConfirm, setCreatorCancelConfirm] = useState(false);
  const [imageRegenToast, setImageRegenToast] = useState({ open: false, message: "", closing: false });
  const [creatorJobIds, setCreatorJobIds] = useState([]);
  const [creatorProgress, setCreatorProgress] = useState({});
  const [showCreatorProgress, setShowCreatorProgress] = useState(false);
  const creatorPollRef = useRef(null);

  const [clients, setClients] = useState([]);
  const [sites, setSites] = useState([]);
  const guestBlockIdRef = useRef(2);
  const orderBlockIdRef = useRef(2);
  const [guestSubmissionBlocks, setGuestSubmissionBlocks] = useState(() => [createSubmissionBlock(1)]);
  const [orderSubmissionBlocks, setOrderSubmissionBlocks] = useState(() => [createSubmissionBlock(1)]);
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
  const [clientSuggestionsBlockId, setClientSuggestionsBlockId] = useState(null);
  const [uploadProgressBlockId, setUploadProgressBlockId] = useState(null);
  const inactivityTimerRef = useRef(null);
  const portalRefreshInFlightRef = useRef(false);

  const t = useMemo(() => (key) => getLabel(language, key), [language]);

  const getTargetSitesForClient = (client) => {
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

  const getClientTargetSites = () => getTargetSitesForClient(clients[0]);

  const sortByLabel = (items, getValue) => [...items].sort((a, b) => (
    getValue(a).localeCompare(getValue(b), undefined, { sensitivity: "base" })
  ));

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
    if (isOrders) {
      orderBlockIdRef.current = 2;
      setOrderSubmissionBlocks([createSubmissionBlock(1, getDefaultSubmissionTargetSite())]);
    } else {
      guestBlockIdRef.current = 2;
      setGuestSubmissionBlocks([createSubmissionBlock(1)]);
    }
    setSiteSuggestionsBlockId(null);
    setClientSuggestionsBlockId(null);
    setUploadProgressBlockId(null);
    setSubmissionFieldErrors({});
  };

  const resetClientSubmissionState = () => {
    setClients([]);
    setSites([]);
    setError("");
    setSuccess("");
    guestBlockIdRef.current = 2;
    orderBlockIdRef.current = 2;
    setGuestSubmissionBlocks([createSubmissionBlock(1)]);
    setOrderSubmissionBlocks([createSubmissionBlock(1, getDefaultSubmissionTargetSite())]);
    setSiteSuggestionsBlockId(null);
    setClientSuggestionsBlockId(null);
    setUploadProgressBlockId(null);
    setShowSubmissionSuccessModal(false);
    setShowSubmissionErrorModal(false);
    setSubmissionErrorCode("");
    setSubmissionErrorMessage("");
    setSubmissionFieldErrors({});
    setShowCreatorProgress(false);
    setCreatorJobIds([]);
    setCreatorProgress({});
  };

  const setSubmissionBlockField = (blockId, field, value) => {
    const setter = isOrders ? setOrderSubmissionBlocks : setGuestSubmissionBlocks;
    setter((prev) => prev.map((block) => (block.id === blockId ? { ...block, [field]: value } : block)));
  };

  const addSubmissionBlock = (afterBlockId) => {
    const setter = isOrders ? setOrderSubmissionBlocks : setGuestSubmissionBlocks;
    const idRef = isOrders ? orderBlockIdRef : guestBlockIdRef;
    setter((prev) => {
      const nextId = idRef.current;
      idRef.current += 1;
      const nextBlock = createSubmissionBlock(nextId, isOrders ? getDefaultSubmissionTargetSite() : {});
      const insertIndex = prev.findIndex((block) => block.id === afterBlockId);
      if (insertIndex < 0) return [...prev, nextBlock];
      return [...prev.slice(0, insertIndex + 1), nextBlock, ...prev.slice(insertIndex + 1)];
    });
  };

  const removeSubmissionBlock = (blockId) => {
    const setter = isOrders ? setOrderSubmissionBlocks : setGuestSubmissionBlocks;
    setter((prev) => {
      if (prev.length <= 1) return prev;
      const next = prev.filter((block) => block.id !== blockId);
      return next.length ? next : [createSubmissionBlock(1, isOrders ? getDefaultSubmissionTargetSite() : {})];
    });
    setSiteSuggestionsBlockId((prev) => (prev === blockId ? null : prev));
    setClientSuggestionsBlockId((prev) => (prev === blockId ? null : prev));
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
    const effectiveClientName = ((block.client_name || "").trim() || clientName);
    if (!effectiveClientName) return t("errorClientRequired");
    if (requiresTargetSite && !(block.target_site_id || "").trim() && !(block.target_site_url || "").trim()) {
      return t("errorClientTargetSiteRequired");
    }
    const sourceType = orders ? "" : (block.source_type || "").trim();
    if (!orders && !sourceType) return t("errorFileTypeRequired");
    if (!orders && sourceType === "google-doc" && !(block.doc_url || "").trim()) return t("errorGoogleDocRequired");
    if (sourceType === "word-doc" && !block.docx_file) return t("errorDocxRequired");
    return "";
  };

  const buildSubmissionFormData = (block, { orders, clientName }) => {
    const formData = new FormData();
    const sourceType = orders ? "google-doc" : (block.source_type || "").trim();
    const effectiveClientName = ((block.client_name || "").trim() || clientName);
    formData.append("publishing_site", block.publishing_site.trim());
    formData.append("client_name", effectiveClientName);
    if (orders) {
      const targetSiteId = (block.target_site_id || "").trim();
      if (/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(targetSiteId)) {
        formData.append("target_site_id", targetSiteId);
      }
      if ((block.target_site_url || "").trim()) formData.append("target_site_url", block.target_site_url.trim());
    }
    formData.append("request_kind", orders ? "order" : "guest_post");
    formData.append("source_type", sourceType);
    formData.append("execution_mode", "async");
    if ((block.anchor || "").trim()) formData.append("anchor", block.anchor.trim());
    if ((block.topic || "").trim()) formData.append("topic", block.topic.trim());
    if (orders) formData.append("creator_mode", "true");
    if (!orders && sourceType === "google-doc") {
      formData.append("doc_url", (block.doc_url || "").trim());
    } else if (sourceType === "word-doc" && block.docx_file) {
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
    if (currentUser?.role !== "client") return;
    const fallbackClientName = ((clients[0]?.name) || "").trim();
    if (!fallbackClientName) return;
    setSubmissionBlocks((prev) => prev.map((block) => (
      (block.client_name || "").trim() ? block : { ...block, client_name: fallbackClientName }
    )));
  }, [clients, currentUser?.role]);

  useEffect(() => {
    const effectiveTheme = theme === "system" ? (systemPrefersDark ? "dark" : "light") : theme;
    if (effectiveTheme === "dark") {
      document.documentElement.removeAttribute("data-theme");
    } else {
      document.documentElement.setAttribute("data-theme", "light");
    }
    localStorage.setItem("theme", theme);
  }, [theme, systemPrefersDark]);

  useEffect(() => {
    localStorage.setItem("portal_sidebar_hidden", sidebarHidden ? "true" : "false");
  }, [sidebarHidden]);

  useEffect(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") return undefined;
    const themeMedia = window.matchMedia("(prefers-color-scheme: dark)");
    const widthMedia = window.matchMedia("(max-width: 1080px)");

    const onThemeMediaChange = (event) => setSystemPrefersDark(Boolean(event.matches));
    const onWidthMediaChange = (event) => {
      const narrow = Boolean(event.matches);
      setIsNarrowViewport(narrow);
      if (narrow) setSidebarHidden(true);
    };

    setSystemPrefersDark(themeMedia.matches);
    setIsNarrowViewport(widthMedia.matches);
    if (widthMedia.matches) setSidebarHidden(true);

    if (typeof themeMedia.addEventListener === "function") {
      themeMedia.addEventListener("change", onThemeMediaChange);
      widthMedia.addEventListener("change", onWidthMediaChange);
      return () => {
        themeMedia.removeEventListener("change", onThemeMediaChange);
        widthMedia.removeEventListener("change", onWidthMediaChange);
      };
    }

    themeMedia.addListener(onThemeMediaChange);
    widthMedia.addListener(onWidthMediaChange);
    return () => {
      themeMedia.removeListener(onThemeMediaChange);
      widthMedia.removeListener(onWidthMediaChange);
    };
  }, []);

  useEffect(() => {
    if (isDbUpdaterDomain) {
      document.title = "DB Updater";
      return;
    }
    if (!currentUser) {
      document.title = "Elci Solutions Portal";
      return;
    }
    document.title = currentUser.role === "admin" ? "Admin Portal | Elci Solutions" : "Clients Portal | Elci Solutions";
  }, [currentUser, isDbUpdaterDomain]);

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
      setImageRegenToast({ open: true, message: t("adminImageRegeneratedSuccess"), closing: false });
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
    const currentHostName = (window.location.hostname || "").trim().toLowerCase();
    if (!currentHostName) return;
    if (dbUpdaterHost && currentHostName === dbUpdaterHost) return;

    let targetHost = "";
    if (currentUser.role === "admin" && currentHostName === clientPortalHost) {
      targetHost = adminPortalHost;
    } else if (currentUser.role !== "admin" && currentHostName === adminPortalHost) {
      targetHost = clientPortalHost;
    }
    if (!targetHost || targetHost === currentHostName) return;

    const nextUrl = `${window.location.protocol}//${targetHost}${window.location.pathname}${window.location.search}${window.location.hash}`;
    window.location.replace(nextUrl);
  }, [currentUser]);

  useEffect(() => {
    if (!dbUpdaterJobId) return undefined;
    let intervalId = null;
    let cancelled = false;

    const poll = async () => {
      try {
        const payload = await api.get(`/db-updater/master-site-sync/jobs/${dbUpdaterJobId}`);
        if (cancelled) return;
        setDbUpdaterJob(payload);
        if (payload?.status === "completed") {
          const report = payload?.report || {};
          setDbUpdaterSuccess(
            `Sync completed. Updated: master ${report.master_rows_to_write || 0}, sites ${report.publishing_sites_rows_to_write || 0}, credentials ${report.credentials_rows_to_write || 0}.`
          );
          setDbUpdaterSubmitting(false);
        } else if (payload?.status === "failed") {
          setDbUpdaterError(payload?.error || payload?.message || "Sync failed.");
          setDbUpdaterSubmitting(false);
        }
        if (payload?.status === "completed" || payload?.status === "failed") {
          if (intervalId) window.clearInterval(intervalId);
        }
      } catch (err) {
        if (cancelled) return;
        setDbUpdaterError(err?.message || "Failed to fetch sync status.");
        setDbUpdaterSubmitting(false);
        if (intervalId) window.clearInterval(intervalId);
      }
    };

    poll();
    intervalId = window.setInterval(poll, 1200);
    return () => {
      cancelled = true;
      if (intervalId) window.clearInterval(intervalId);
    };
  }, [dbUpdaterJobId]);

  useEffect(() => {
    if (dbUpdaterDeleteMissingSites) return;
    setDbUpdaterForceDeleteMissingSites(false);
  }, [dbUpdaterDeleteMissingSites]);

  useEffect(() => {
    if (!isDbUpdaterDomain) return undefined;
    let cancelled = false;
    let intervalId = null;

    const loadHistory = async () => {
      try {
        const payload = await api.get("/db-updater/master-site-sync/jobs?limit=12");
        if (cancelled) return;
        setDbUpdaterJobsHistory(Array.isArray(payload?.items) ? payload.items : []);
      } catch {
        if (cancelled) return;
      }
    };

    loadHistory();
    intervalId = window.setInterval(loadHistory, 3000);
    return () => {
      cancelled = true;
      if (intervalId) window.clearInterval(intervalId);
    };
  }, [isDbUpdaterDomain]);

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
    if (!currentUser || isDbUpdaterDomain) return;
    const refreshOnSectionChange = async () => {
      if (portalRefreshInFlightRef.current) return;
      portalRefreshInFlightRef.current = true;
      try {
        await loadAll(currentUser);
        if (currentUser.role === "admin") {
          if (activeSection === "pending-jobs") {
            await loadPendingJobs(currentUser);
          }
          if (activeSection === "admin") {
            await loadAdminUsers(currentUser);
          }
        }
      } finally {
        portalRefreshInFlightRef.current = false;
      }
    };
    refreshOnSectionChange();
  }, [currentUser, activeSection, isDbUpdaterDomain]);

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

  // ── Creator progress polling ──
  const stopCreatorPolling = useCallback(() => {
    if (creatorPollRef.current) {
      clearInterval(creatorPollRef.current);
      creatorPollRef.current = null;
    }
  }, []);

  const closeCreatorProgress = useCallback(() => {
    stopCreatorPolling();
    setShowCreatorProgress(false);
    setCreatorJobIds([]);
    setCreatorProgress({});
    setCreatorCancelError("");
    setCreatorCanceling(false);
    setCreatorCancelConfirm(false);
  }, [stopCreatorPolling]);

  const dismissImageRegenToast = useCallback(() => {
    setImageRegenToast((prev) => {
      if (!prev.open || prev.closing) return prev;
      return { ...prev, closing: true };
    });
  }, []);

  useEffect(() => {
    if (!imageRegenToast.open) return undefined;
    const timer = setTimeout(() => {
      dismissImageRegenToast();
    }, 3000);
    return () => clearTimeout(timer);
  }, [imageRegenToast.open, dismissImageRegenToast]);

  useEffect(() => {
    if (!imageRegenToast.closing) return undefined;
    const timer = setTimeout(() => {
      setImageRegenToast({ open: false, message: "", closing: false });
    }, 160);
    return () => clearTimeout(timer);
  }, [imageRegenToast.closing]);

  useEffect(() => {
    if (!showCreatorProgress || creatorJobIds.length === 0) return;
    let cancelled = false;
    const poll = async () => {
      const updates = {};
      let allDone = true;
      for (const jid of creatorJobIds) {
        try {
          const data = await api.get(`/automation/status?job_id=${encodeURIComponent(jid)}`);
          if (!data?.found) { allDone = false; continue; }
          const phaseEvents = (data.events || []).filter((e) => e.event_type === "creator_phase");
          const last = phaseEvents.length > 0 ? phaseEvents[phaseEvents.length - 1] : null;
          const jobDone = data.job_status === "pending_approval"
            || data.job_status === "succeeded"
            || data.job_status === "failed"
            || data.job_status === "rejected"
            || data.job_status === "canceled";
          updates[jid] = {
            phase: last?.payload?.phase || 0,
            label: last?.payload?.label || "",
            percent: jobDone ? 100 : (last?.payload?.percent || 0),
            done: jobDone,
            failed: data.job_status === "failed",
            canceled: data.job_status === "canceled",
          };
          if (!jobDone) allDone = false;
        } catch {
          allDone = false;
        }
      }
      if (!cancelled) {
        setCreatorProgress((prev) => ({ ...prev, ...updates }));
        if (allDone) stopCreatorPolling();
      }
    };
    poll();
    creatorPollRef.current = setInterval(poll, 3000);
    return () => { cancelled = true; stopCreatorPolling(); };
  }, [showCreatorProgress, creatorJobIds]); // eslint-disable-line react-hooks/exhaustive-deps

  const cancelCreatorJobs = useCallback(async () => {
    if (creatorCanceling || creatorJobIds.length === 0) return;
    setCreatorCanceling(true);
    setCreatorCancelError("");
    try {
      await Promise.all(creatorJobIds.map((jid) => api.post(`/jobs/${jid}/cancel`, {})));
      setCreatorProgress((prev) => {
        const next = { ...prev };
        creatorJobIds.forEach((jid) => {
          const current = next[jid] || {};
          next[jid] = {
            ...current,
            done: true,
            failed: false,
            canceled: true,
            percent: 100,
            label: t("progressCanceled"),
          };
        });
        return next;
      });
      stopCreatorPolling();
    } catch (err) {
      setCreatorCancelError(err?.message || t("errorRequestFailed"));
    } finally {
      setCreatorCanceling(false);
    }
  }, [creatorCanceling, creatorJobIds, stopCreatorPolling, t]);

  const requestCancelCreatorJobs = useCallback(() => {
    if (creatorCanceling) return;
    setCreatorCancelConfirm(true);
    setCreatorCancelError("");
  }, [creatorCanceling]);

  const dismissCancelConfirm = useCallback(() => {
    if (creatorCanceling) return;
    setCreatorCancelConfirm(false);
  }, [creatorCanceling]);

  const submitSubmissionBlock = async (block, blockIndex) => {
    setError("");
    setSuccess("");
    setShowSubmissionErrorModal(false);
    setSubmissionErrorCode("");
    setSubmissionErrorMessage("");
    setSubmissionFieldErrors({});

    if (activeSection === "admin") {
      setError(t("errorSelectClientSection"));
      return;
    }
    const resolvedClientName = ((clients[0]?.name) || "").trim();
    const requiresTargetSiteSelection = isOrders;
    const validationError = getSubmissionBlockError(block, {
      orders: isOrders,
      clientName: resolvedClientName,
      requiresTargetSite: requiresTargetSiteSelection,
    });
    if (validationError) {
      if (validationError === t("errorFileTypeRequired")) {
        const blockId = block.id;
        setSubmissionFieldErrors((prev) => ({
          ...prev,
          [blockId]: {
            ...(prev[blockId] || {}),
            source_type: true,
          },
        }));
        return;
      }
      setError(`Block ${blockIndex + 1}: ${validationError}`);
      return;
    }

    try {
      setSubmitting(true);
      const formData = buildSubmissionFormData(block, {
        orders: isOrders,
        clientName: resolvedClientName,
      });
      const effectiveSourceType = isOrders ? "google-doc" : (block.source_type || "").trim();
      let responseData = null;
      if (effectiveSourceType === "word-doc" && block.docx_file) {
        setUploadProgressBlockId(block.id);
        setUploadProgress(0);
        responseData = await postMultipartWithProgress(`${baseApiUrl}/automation/guest-post-webhook`, formData);
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
        responseData = await response.json().catch(() => ({}));
      }

      if (responseData?.job_id && isOrders) {
        const jobId = responseData.job_id;
        setCreatorJobIds([jobId]);
        setCreatorProgress({
          [jobId]: { phase: 0, label: "", percent: 0, done: false, failed: false, canceled: false },
        });
        setShowCreatorProgress(true);
      } else {
        setShowSubmissionSuccessModal(true);
      }
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

  const submitDbUpdaterFile = async (event) => {
    event.preventDefault();
    if (!dbUpdaterFile) {
      setDbUpdaterError("Please choose a CSV or XLSX file.");
      return;
    }
    try {
      setDbUpdaterSubmitting(true);
      setDbUpdaterError("");
      setDbUpdaterSuccess("");
      setDbUpdaterUploadPercent(0);
      setDbUpdaterJobId("");
      setDbUpdaterJob(null);
      const formData = new FormData();
      formData.append("file", dbUpdaterFile);
      formData.append("dry_run", dbUpdaterDryRun ? "true" : "false");
      formData.append("delete_missing_sites", dbUpdaterDeleteMissingSites ? "true" : "false");
      formData.append("force_delete_missing_sites", dbUpdaterForceDeleteMissingSites ? "true" : "false");
      const payload = await api.upload("/db-updater/master-site-sync/jobs", formData, {
        onProgress: (percent) => setDbUpdaterUploadPercent(percent),
      });
      setDbUpdaterJobId(payload?.job_id || "");
    } catch (err) {
      setDbUpdaterError(err?.message || "Upload failed.");
      setDbUpdaterSubmitting(false);
    }
  };

  if (authLoading) {
    return (
      <div className="auth-shell">
        <div className="auth-loading">{t("loading")}</div>
      </div>
    );
  }

  if (isDbUpdaterDomain) {
    const serverProgress = Number(dbUpdaterJob?.progress_percent || 0);
    const overallProgress = dbUpdaterJobId
      ? Math.max(25, Math.min(100, 25 + Math.round((serverProgress * 75) / 100)))
      : Math.round((dbUpdaterUploadPercent * 25) / 100);
    const stageLabel = dbUpdaterJob?.message || (dbUpdaterSubmitting ? "Uploading file..." : "Ready");

    return (
      <DbUpdaterWorkspace
        file={dbUpdaterFile}
        onFileChange={setDbUpdaterFile}
        dryRun={dbUpdaterDryRun}
        onDryRunChange={setDbUpdaterDryRun}
        deleteMissingSites={dbUpdaterDeleteMissingSites}
        onDeleteMissingSitesChange={setDbUpdaterDeleteMissingSites}
        forceDeleteMissingSites={dbUpdaterForceDeleteMissingSites}
        onForceDeleteMissingSitesChange={setDbUpdaterForceDeleteMissingSites}
        onSubmit={submitDbUpdaterFile}
        submitting={dbUpdaterSubmitting}
        progressPercent={overallProgress}
        uploadPercent={dbUpdaterUploadPercent}
        stageLabel={stageLabel}
        job={dbUpdaterJob}
        error={dbUpdaterError}
        success={dbUpdaterSuccess}
        historyItems={dbUpdaterJobsHistory}
        onLogout={currentUser ? handleLogout : null}
      />
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
    return sortByLabel(sites, (site) => `${site.site_url || ""} ${site.name || ""}`).filter((site) => {
      const authorName = (site.author_name || "").trim();
      if (!authorName) return false;
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
    <div className={`app-shell ${sidebarHidden ? "sidebar-hidden" : ""}`.trim()}>
      <Sidebar
        t={t}
        userRole={currentUser.role}
        activeSection={activeSection}
        onSectionChange={(next) => {
          setActiveSection(next);
          if (isNarrowViewport) setSidebarHidden(true);
        }}
        pendingJobsCount={pendingJobs.length}
      />
      {isNarrowViewport && !sidebarHidden ? (
        <button
          type="button"
          className="sidebar-backdrop"
          aria-label="Close menu"
          onClick={() => setSidebarHidden(true)}
        />
      ) : null}
      <button
        className={`sidebar-edge-toggle ${sidebarHidden ? "collapsed" : ""}`.trim()}
        type="button"
        onClick={() => setSidebarHidden((prev) => !prev)}
        aria-pressed={!sidebarHidden}
        aria-label={sidebarHidden ? "Show side panel" : "Hide side panel"}
        title={sidebarHidden ? "Show side panel" : "Hide side panel"}
      >
        <span aria-hidden="true">{isNarrowViewport ? (sidebarHidden ? "☰" : "×") : (sidebarHidden ? "›" : "‹")}</span>
      </button>

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

        <div className={`container ${isAdminPendingSection ? "container-wide" : ""} ${(isGuestPostsSection || isOrders) ? "request-container" : ""}`.trim()}>
          {(isGuestPostsSection || isOrders) ? (
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
          {error && error !== "Load failed" && error !== "Failed to fetch" ? (
            <div className="validation-error validation-error-banner">{error}</div>
          ) : null}
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
            <div className="panel form-panel request-form-panel">
              <div className="guest-form request-builder-form">
                <div className="submission-blocks">
                  {(isOrders ? orderSubmissionBlocks : guestSubmissionBlocks).map((block, blockIndex) => {
                    const blockFilteredSites = getFilteredSitesForQuery(block.publishing_site);
                    const blockFilteredClients = isAdminUser
                      ? sortByLabel(clients, (client) => (client.name || "").trim() || String(client.id || "")).filter((client) => {
                          const q = (block.client_name || "").trim().toLowerCase();
                          if (!q) return true;
                          const name = ((client.name || "").trim()).toLowerCase();
                          return name.includes(q);
                        })
                      : [];
                    const selectedClient = isAdminUser
                      ? clients.find((client) => (client.name || "").trim() === (block.client_name || "").trim())
                      : null;
                    const availableTargetSites = sortByLabel(
                      isAdminUser ? getTargetSitesForClient(selectedClient) : clientTargetSites,
                      (row) => `${row.target_site_domain || ""} ${row.target_site_url || ""}`,
                    );
                    const showRemoveControl = blockIndex > 0;
                    return (
                      <div key={block.id} className="submission-block-wrap">
                        <div className={`submission-block panel ${isOrders ? "order-block" : ""}`.trim()}>
                          <div className="submission-block-header">
                            <h3>{`${t("requestBlockLabel")} ${blockIndex + 1}`}</h3>
                          </div>

                          {isAdminUser ? (
                            <div className="submission-field submission-field-inline submission-field-client">
                              <label>{t("clientName")}</label>
                              <div className="site-suggest-wrap">
                                <input
                                  type="text"
                                  value={block.client_name || ""}
                                  onFocus={() => setClientSuggestionsBlockId(block.id)}
                                  onBlur={() => setTimeout(() => {
                                    setClientSuggestionsBlockId((prev) => (prev === block.id ? null : prev));
                                  }, 120)}
                                  onChange={(e) => {
                                    const nextClientName = e.target.value;
                                    setSubmissionBlocks((prev) => prev.map((item) => (
                                      item.id === block.id
                                        ? {
                                            ...item,
                                            client_name: nextClientName,
                                            target_site_id: "",
                                            target_site_url: "",
                                          }
                                        : item
                                    )));
                                    setClientSuggestionsBlockId(block.id);
                                  }}
                                  placeholder={t("selectClient")}
                                  required
                                />
                                {clientSuggestionsBlockId === block.id && blockFilteredClients.length > 0 ? (
                                  <div className="site-suggest-list">
                                    {blockFilteredClients.slice(0, 30).map((client) => (
                                      <button
                                        key={client.id}
                                        type="button"
                                        className="site-suggest-item"
                                        onMouseDown={(event) => {
                                          event.preventDefault();
                                          const nextClientName = (client.name || "").trim();
                                          const nextTargetRows = sortByLabel(
                                            getTargetSitesForClient(client),
                                            (row) => `${row.target_site_domain || ""} ${row.target_site_url || ""}`,
                                          );
                                          const nextPrimary = nextTargetRows.find((row) => row?.is_primary) || nextTargetRows[0] || null;
                                          setSubmissionBlocks((prev) => prev.map((item) => (
                                            item.id === block.id
                                              ? {
                                                  ...item,
                                                  client_name: nextClientName,
                                                  target_site_id: nextPrimary ? String(nextPrimary.id || "") : "",
                                                  target_site_url: nextPrimary ? (nextPrimary.target_site_url || "").trim() : "",
                                                }
                                              : item
                                          )));
                                          setClientSuggestionsBlockId(null);
                                        }}
                                      >
                                        <span>{(client.name || "").trim() || client.id}</span>
                                        <span className="muted-text small-text">{client.email || client.id}</span>
                                      </button>
                                    ))}
                                  </div>
                                ) : null}
                              </div>
                            </div>
                          ) : null}

                          {isOrders ? (
                            <div className="submission-field submission-field-site submission-field-target-site">
                              <label>{t("targetSiteForBacklink")}</label>
                              <input
                                type="url"
                                list={`target-site-list-${block.id}`}
                                value={block.target_site_url || ""}
                                onChange={(e) => {
                                  const nextUrl = e.target.value;
                                  const nextTarget = availableTargetSites.find((row) => {
                                    const urlValue = (row.target_site_url || "").trim();
                                    const domainValue = (row.target_site_domain || "").trim();
                                    const domainUrlValue = domainValue ? `https://${domainValue}` : "";
                                    return urlValue === nextUrl || domainUrlValue === nextUrl;
                                  });
                                  const nextId = nextTarget ? String(nextTarget.id || "") : "";
                                  setSubmissionBlocks((prev) => prev.map((item) => (
                                    item.id === block.id
                                      ? {
                                          ...item,
                                          target_site_id: nextId,
                                          target_site_url: nextUrl,
                                        }
                                      : item
                                  )));
                                }}
                                placeholder={t("placeholderTargetWebsite")}
                                required
                              />
                              <datalist id={`target-site-list-${block.id}`}>
                                {availableTargetSites.map((row) => {
                                  const optionId = String(row.id || "");
                                  const domainLabel = (row.target_site_domain || "").trim();
                                  const urlLabel = (row.target_site_url || "").trim();
                                  const label = urlLabel || (domainLabel ? `https://${domainLabel}` : "") || optionId;
                                  return (
                                    <option key={optionId} value={label} />
                                  );
                                })}
                              </datalist>
                            </div>
                          ) : null}

                          <div className={`submission-field submission-field-site ${isOrders ? "submission-field-inline" : ""}`.trim()}>
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

                          {!isOrders ? (
                            <div className="submission-field submission-field-type">
                              <label>{t("fileType")}</label>
                              <div className="toggle source-toggle">
                                <button
                                  type="button"
                                  className={block.source_type === "google-doc" ? "active" : ""}
                                  onClick={() => {
                                    setSubmissionBlockField(block.id, "source_type", "google-doc");
                                    setSubmissionFieldErrors((prev) => ({
                                      ...prev,
                                      [block.id]: {
                                        ...(prev[block.id] || {}),
                                        source_type: false,
                                      },
                                    }));
                                  }}
                                >
                                  {t("googleDoc")}
                                </button>
                                <button
                                  type="button"
                                  className={block.source_type === "word-doc" ? "active" : ""}
                                  onClick={() => {
                                    setSubmissionBlockField(block.id, "source_type", "word-doc");
                                    setSubmissionFieldErrors((prev) => ({
                                      ...prev,
                                      [block.id]: {
                                        ...(prev[block.id] || {}),
                                        source_type: false,
                                      },
                                    }));
                                  }}
                                >
                                  {t("docxFile")}
                                </button>
                              </div>
                              {submissionFieldErrors[block.id]?.source_type ? (
                                <div className="file-type-tooltip" role="alert">
                                  <span className="file-type-tooltip-icon">!</span>
                                  <span>{t("errorFileTypeRequired")}</span>
                                </div>
                              ) : null}
                            </div>
                          ) : null}

                          {!isOrders && block.source_type === "google-doc" ? (
                            <div className="submission-field submission-field-wide">
                              <label>{t("googleDocLink")}</label>
                              <input
                                type="url"
                                value={block.doc_url}
                                onChange={(e) => setSubmissionBlockField(block.id, "doc_url", e.target.value)}
                                placeholder={t("placeholderGoogleDoc")}
                                required
                              />
                            </div>
                          ) : !isOrders && block.source_type === "word-doc" ? (
                            <div className="submission-field submission-field-wide">
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
                              <div className="submission-field submission-field-inline submission-field-anchor">
                                <label>{`${t("anchor")} (${t("optional")})`}</label>
                                <input
                                  type="text"
                                  value={block.anchor}
                                  onChange={(e) => setSubmissionBlockField(block.id, "anchor", e.target.value)}
                                  placeholder={t("placeholderAnchor")}
                                />
                              </div>
                              <div className="submission-field submission-field-inline submission-field-topic">
                                <label>{`${t("topic")} (${t("optional")})`}</label>
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

                          <div className="submission-block-actions">
                            <button
                              className="btn submit-btn"
                              type="button"
                              onClick={() => submitSubmissionBlock(block, blockIndex)}
                              disabled={submitting}
                            >
                              {submitting ? t("submitting") : t("submitForReview")}
                            </button>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
                <div className="submission-block-controls submission-block-controls-global">
                  <button
                    className="btn block-control-btn"
                    type="button"
                    aria-label={t("addAnotherBlock")}
                    onClick={() => {
                      const currentBlocks = isOrders ? orderSubmissionBlocks : guestSubmissionBlocks;
                      addSubmissionBlock(currentBlocks[currentBlocks.length - 1]?.id);
                    }}
                    disabled={submitting}
                  >
                    +
                  </button>
                  {(isOrders ? orderSubmissionBlocks : guestSubmissionBlocks).length > 1 ? (
                    <button
                      className="btn secondary block-control-btn"
                      type="button"
                      aria-label={t("removeBlock")}
                      onClick={() => {
                        const currentBlocks = isOrders ? orderSubmissionBlocks : guestSubmissionBlocks;
                        removeSubmissionBlock(currentBlocks[currentBlocks.length - 1]?.id);
                      }}
                      disabled={submitting}
                    >
                      -
                    </button>
                  ) : null}
                </div>
              </div>
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
      <CreatorProgressModal
        t={t}
        open={showCreatorProgress}
        jobIds={creatorJobIds}
        progress={creatorProgress}
        onClose={closeCreatorProgress}
        onCancel={cancelCreatorJobs}
        canceling={creatorCanceling}
        cancelError={creatorCancelError}
        cancelConfirm={creatorCancelConfirm}
        onRequestCancel={requestCancelCreatorJobs}
        onDismissCancel={dismissCancelConfirm}
      />
      <SubmissionErrorModal
        t={t}
        open={showSubmissionErrorModal}
        errorCode={submissionErrorCode}
        errorMessage={submissionErrorMessage}
        onClose={() => setShowSubmissionErrorModal(false)}
      />
      {imageRegenToast.open ? (
        <div
          className="toast-overlay"
          role="presentation"
          onClick={dismissImageRegenToast}
          onTouchStart={dismissImageRegenToast}
        >
          <div className={`toast-card ${imageRegenToast.closing ? "toast-hide" : ""}`}>
            {imageRegenToast.message}
          </div>
        </div>
      ) : null}
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

function CreatorProgressModal({
  t,
  open,
  jobIds,
  progress,
  onClose,
  onCancel,
  canceling,
  cancelError,
  cancelConfirm,
  onRequestCancel,
  onDismissCancel,
}) {
  if (!open) return null;
  // Aggregate progress across all jobs — use the first job for the step display
  const firstId = jobIds[0];
  const info = progress[firstId] || { phase: 0, label: "", percent: 0, done: false, failed: false };
  const allDone = jobIds.length > 0 && jobIds.every((jid) => progress[jid]?.done);
  const anyFailed = jobIds.some((jid) => progress[jid]?.failed);
  const anyCanceled = jobIds.some((jid) => progress[jid]?.canceled);
  // Compute aggregate percent
  const aggPercent = jobIds.length > 0
    ? Math.round(jobIds.reduce((sum, jid) => sum + (progress[jid]?.percent || 0), 0) / jobIds.length)
    : 0;
  const currentPhase = info.phase || 0;

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true" aria-labelledby="creator-progress-title">
      <div className="progress-modal-card panel">
        <h3 id="creator-progress-title">{t("progressTitle")}</h3>

        <div className="progress-steps">
          {Array.from({ length: CREATOR_TOTAL_PHASES }, (_, i) => {
            const step = i + 1;
            const isCompleted = allDone || step < currentPhase;
            const isActive = !allDone && step === currentPhase;
            const cls = isCompleted ? "completed" : isActive ? "active" : "";
            return (
              <div key={step} className={`progress-step ${cls}`}>
                <div className="progress-step-indicator">
                  <div className="progress-step-dot" />
                  {step < CREATOR_TOTAL_PHASES && <div className="progress-step-line" />}
                </div>
                <div className="progress-step-content">
                  <span className="progress-step-label">{t(CREATOR_PHASE_LABELS[step])}</span>
                </div>
              </div>
            );
          })}
        </div>

        <div className="progress-bar-container">
          <div className="progress-bar-header">
            <span>{currentPhase === 0 && !allDone ? t("progressWaiting") : info.label}</span>
            <strong>{allDone ? 100 : aggPercent}%</strong>
          </div>
          <div className="progress-bar-track">
            <div className="progress-bar-fill" style={{ width: `${allDone ? 100 : aggPercent}%` }} />
          </div>
        </div>

        {!allDone && (
          <div className="progress-modal-actions">
            {!cancelConfirm ? (
              <button className="btn danger" type="button" onClick={onRequestCancel} disabled={canceling}>
                {t("cancel")}
              </button>
            ) : (
              <div className="cancel-confirm-row">
                <span className="muted-text">{t("cancelConfirm")}</span>
                <div className="cancel-confirm-actions">
                  <button className="btn danger" type="button" onClick={onCancel} disabled={canceling}>
                    {canceling ? t("canceling") : t("cancel")}
                  </button>
                  <button className="btn secondary" type="button" onClick={onDismissCancel} disabled={canceling}>
                    {t("close")}
                  </button>
                </div>
              </div>
            )}
            {cancelError ? <p className="muted-text">{cancelError}</p> : null}
          </div>
        )}

        {allDone && (
          <div className="progress-modal-actions">
            <p className="muted-text">
              {anyCanceled ? t("progressCanceled") : anyFailed ? t("errorRequestFailed") : t("submissionSuccessBody")}
            </p>
            <button className="btn" type="button" onClick={onClose}>
              {t("close")}
            </button>
          </div>
        )}
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

function DbUpdaterWorkspace({
  file,
  onFileChange,
  dryRun,
  onDryRunChange,
  deleteMissingSites,
  onDeleteMissingSitesChange,
  forceDeleteMissingSites,
  onForceDeleteMissingSitesChange,
  onSubmit,
  submitting,
  progressPercent,
  uploadPercent,
  stageLabel,
  job,
  error,
  success,
  historyItems,
  onLogout,
}) {
  const report = job?.report || null;
  return (
    <div className="db-updater-shell">
      <div className="db-updater-topbar">
        <div>
          <strong>DB Updater</strong>
          <p className="muted-text small-text">updatedb.elci.live</p>
        </div>
        {onLogout ? (
          <button className="btn secondary" type="button" onClick={onLogout}>
            Logout
          </button>
        ) : null}
      </div>

      <div className="panel db-updater-panel">
        <h1>Master Site Sync</h1>
        <p className="muted-text">
          Upload one CSV or XLSX file. The backend will sync `master_site_info`, `publishing_sites`, and `publishing_site_credentials`.
        </p>

        <form className="db-updater-form" onSubmit={onSubmit}>
          <label className="db-updater-upload-box">
            <span className="db-updater-upload-title">Choose file (CSV/XLSX)</span>
            <span className="db-updater-upload-filename">{file ? file.name : "No file selected"}</span>
            <input
              type="file"
              accept=".csv,.xlsx"
              onChange={(event) => onFileChange(event.target.files?.[0] || null)}
              disabled={submitting}
            />
          </label>

          <div className="db-updater-form-controls">
            <div className="db-updater-options">
              <label className="db-updater-checkbox">
                <input
                  type="checkbox"
                  checked={dryRun}
                  onChange={(event) => onDryRunChange(event.target.checked)}
                  disabled={submitting}
                />
                <span>Dry run (preview only, no DB writes)</span>
              </label>
              <label className="db-updater-checkbox db-updater-checkbox-danger">
                <input
                  type="checkbox"
                  checked={deleteMissingSites}
                  onChange={(event) => onDeleteMissingSitesChange(event.target.checked)}
                  disabled={submitting}
                />
                <span>Delete sites missing from master file (skips sites referenced by submissions/jobs)</span>
              </label>
              <label className="db-updater-checkbox db-updater-checkbox-danger">
                <input
                  type="checkbox"
                  checked={forceDeleteMissingSites}
                  onChange={(event) => onForceDeleteMissingSitesChange(event.target.checked)}
                  disabled={submitting || !deleteMissingSites}
                />
                <span>Force delete referenced missing sites (deletes related submissions/jobs history)</span>
              </label>
            </div>

            <button className="btn submit-btn db-updater-submit" type="submit" disabled={submitting || !file}>
              {submitting ? "Running sync..." : "Upload & Sync"}
            </button>
          </div>
        </form>

        {(submitting || job || success) ? (
          <div className="db-updater-progress" aria-live="polite">
            <div className="upload-meter-row">
              <span>{stageLabel}</span>
              <strong>{progressPercent}%</strong>
            </div>
            <div className="upload-meter-track">
              <div className="upload-meter-fill" style={{ width: `${progressPercent}%` }} />
            </div>
            {submitting && !job ? <p className="muted-text small-text">Upload progress: {uploadPercent}%</p> : null}
          </div>
        ) : null}

        {error ? <div className="error">{error}</div> : null}
        {success ? <div className="success">{success}</div> : null}

        {report ? (
          <div className="db-updater-report-grid">
            <div className="db-updater-report-card">
              <span className="stat-label">Prepared rows</span>
              <strong>{report.master_rows_prepared || 0}</strong>
            </div>
            <div className="db-updater-report-card">
              <span className="stat-label">Master updates</span>
              <strong>{report.master_rows_to_write || 0}</strong>
            </div>
            <div className="db-updater-report-card">
              <span className="stat-label">Site updates</span>
              <strong>{report.publishing_sites_rows_to_write || 0}</strong>
            </div>
            <div className="db-updater-report-card">
              <span className="stat-label">Credential updates</span>
              <strong>{report.credentials_rows_to_write || 0}</strong>
            </div>
            <div className="db-updater-report-card">
              <span className="stat-label">Issues</span>
              <strong>{report.issues_count || 0}</strong>
            </div>
            <div className="db-updater-report-card">
              <span className="stat-label">Mode</span>
              <strong>{report.dry_run ? "Dry Run" : "Live Sync"}</strong>
            </div>
            <div className="db-updater-report-card">
              <span className="stat-label">Delete Missing</span>
              <strong>{report.delete_missing_sites ? "On" : "Off"}</strong>
            </div>
            <div className="db-updater-report-card">
              <span className="stat-label">Missing in DB</span>
              <strong>{report.missing_sites_in_db_not_in_master || 0}</strong>
            </div>
            <div className="db-updater-report-card">
              <span className="stat-label">Delete Candidates</span>
              <strong>{report.missing_sites_delete_candidates || 0}</strong>
            </div>
            <div className="db-updater-report-card">
              <span className="stat-label">Deleted</span>
              <strong>{report.missing_sites_deleted || 0}</strong>
            </div>
            <div className="db-updater-report-card">
              <span className="stat-label">Blocked Deletes</span>
              <strong>{report.missing_sites_blocked || 0}</strong>
            </div>
            <div className="db-updater-report-card">
              <span className="stat-label">Force Deleted</span>
              <strong>{report.missing_sites_force_deleted || 0}</strong>
            </div>
          </div>
        ) : null}

        {Array.isArray(historyItems) && historyItems.length > 0 ? (
          <div className="db-updater-history">
            <h2>Recent Sync Jobs</h2>
            <div className="db-updater-history-table">
              <div className="db-updater-history-row db-updater-history-head">
                <span>Time (UTC)</span>
                <span>File</span>
                <span>Mode</span>
                <span>Status</span>
                <span>Progress</span>
                <span>Issues</span>
              </div>
              {historyItems.map((item) => (
                <div key={item.id} className="db-updater-history-row">
                  <span>{(item.updated_at || item.created_at || "").replace("T", " ").replace("Z", "")}</span>
                  <span>{item.file_name || "-"}</span>
                  <span>{item.dry_run ? "Dry Run" : "Live"}</span>
                  <span>{item.status || "-"}</span>
                  <span>{Number(item.progress_percent || 0)}%</span>
                  <span>{item.report?.issues_count ?? "-"}</span>
                </div>
              ))}
            </div>
          </div>
        ) : null}
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
        { id: "guest-posts", label: t("navGuestPosts") },
        { id: "orders", label: t("navOrders") },
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
