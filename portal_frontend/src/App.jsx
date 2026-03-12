import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
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
const ADMIN_SECTIONS = ["admin", "websites", "clients", "pending-jobs", "published-articles", "queue-dashboard", "submit-article", "create-article"];
const CLIENT_SECTIONS = ["dashboard", "submit-article", "create-article"];
const CLIENT_IDLE_LOGOUT_MS = 24 * 60 * 60 * 1000;
const ADMIN_IDLE_LOGOUT_MS = 1 * 60 * 60 * 1000;
const PUBLISHED_PAGE_SIZE = 25;
const PUBLISHED_PAGE_SIZES = [25, 50, 100];
const CREATE_ARTICLE_BLOCKS_STORAGE_PREFIX = "portal_create_article_blocks_v1";
const CREATOR_JOBS_STORAGE_PREFIX = "portal_creator_jobs_by_block_v1";
const TREND_RECENT_LIMIT = 6;
const SITE_FIT_RECENT_LIMIT = 6;

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

const getStoredSectionForRole = (role) => (localStorage.getItem(`active_section_${role}`) || "").trim();

const resolveSectionForRole = (role, section) => {
  const allowed = getAllowedSectionsForRole(role);
  return allowed.includes(section) ? section : getDefaultSectionForRole(role);
};

const getLandingSectionForRole = (role) =>
  role === "admin" ? getDefaultSectionForRole(role) : resolveSectionForRole(role, getStoredSectionForRole(role));

const getUserStorageSuffix = (user) => (
  (user?.id || user?.email || user?.role || "default").toString().trim().toLowerCase()
);

const getCreateArticleBlocksStorageKey = (user) => `${CREATE_ARTICLE_BLOCKS_STORAGE_PREFIX}:${getUserStorageSuffix(user)}`;
const getCreatorJobsStorageKey = (user) => `${CREATOR_JOBS_STORAGE_PREFIX}:${getUserStorageSuffix(user)}`;

const clearCreatorDraftStorage = () => {
  if (typeof window === "undefined") return;
  const keysToRemove = [];
  for (let i = 0; i < localStorage.length; i += 1) {
    const key = localStorage.key(i);
    if (!key) continue;
    if (key.startsWith(`${CREATE_ARTICLE_BLOCKS_STORAGE_PREFIX}:`) || key.startsWith(`${CREATOR_JOBS_STORAGE_PREFIX}:`)) {
      keysToRemove.push(key);
    }
  }
  keysToRemove.forEach((key) => localStorage.removeItem(key));
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
  const [activeSection, setActiveSection] = useState("submit-article");
  const [language, setLanguage] = useState(getInitialLanguage());
  const [theme, setTheme] = useState(() => {
    const stored = localStorage.getItem("theme");
    return stored === "light" || stored === "dark" ? stored : "dark";
  });
  const [sidebarHidden, setSidebarHidden] = useState(getInitialSidebarHidden);
  const [readySites, setReadySites] = useState([]);
  const [isNarrowViewport, setIsNarrowViewport] = useState(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") return false;
    return window.matchMedia("(max-width: 1080px)").matches;
  });
  const [isMobileViewport, setIsMobileViewport] = useState(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") return false;
    return window.matchMedia("(max-width: 900px)").matches;
  });
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

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
  const [batchBlockStatus, setBatchBlockStatus] = useState({});
  const [creatorCancelingByBlock, setCreatorCancelingByBlock] = useState({});
  const [creatorCancelErrorByBlock, setCreatorCancelErrorByBlock] = useState({});
  const [creatorCancelConfirmByBlock, setCreatorCancelConfirmByBlock] = useState({});
  const [imageRegenToast, setImageRegenToast] = useState({ open: false, message: "", closing: false });
  const [creatorJobsByBlock, setCreatorJobsByBlock] = useState({});
  const [creatorProgress, setCreatorProgress] = useState({});
  const creatorPollRef = useRef(null);
  const [creatorDraftsHydrated, setCreatorDraftsHydrated] = useState(false);

  const [clients, setClients] = useState([]);
  const [sites, setSites] = useState([]);
  const submitArticleBlockIdRef = useRef(2);
  const createArticleBlockIdRef = useRef(2);
  const [submitArticleSubmissionBlocks, setSubmitArticleSubmissionBlocks] = useState(() => [createSubmissionBlock(1)]);
  const [createArticleSubmissionBlocks, setCreateArticleSubmissionBlocks] = useState(() => [createSubmissionBlock(1)]);
  const [adminUsers, setAdminUsers] = useState([]);
  const [adminUserForm, setAdminUserForm] = useState(emptyAdminUserForm());
  const [adminUserEdits, setAdminUserEdits] = useState({});
  const [adminLoading, setAdminLoading] = useState(false);
  const [adminSubmitting, setAdminSubmitting] = useState(false);
  const [adminSavingUserId, setAdminSavingUserId] = useState("");
  const [keywordTrendDashboard, setKeywordTrendDashboard] = useState(null);
  const [keywordTrendLoading, setKeywordTrendLoading] = useState(false);
  const [siteFitDashboard, setSiteFitDashboard] = useState(null);
  const [siteFitLoading, setSiteFitLoading] = useState(false);
  const [pendingJobs, setPendingJobs] = useState([]);
  const [pendingLoading, setPendingLoading] = useState(false);
  const [publishingJobId, setPublishingJobId] = useState("");
  const [rejectingJobId, setRejectingJobId] = useState("");
  const [regeneratingImageJobId, setRegeneratingImageJobId] = useState("");
  const [openRejectJobId, setOpenRejectJobId] = useState("");
  const [rejectForms, setRejectForms] = useState({});
  const [publishedArticles, setPublishedArticles] = useState([]);
  const [publishedLoading, setPublishedLoading] = useState(false);
  const [publishedTotal, setPublishedTotal] = useState(0);
  const [publishedOffset, setPublishedOffset] = useState(0);
  const [publishedLimit, setPublishedLimit] = useState(PUBLISHED_PAGE_SIZE);
  const [publishedQuery, setPublishedQuery] = useState("");
  const [publishedClientId, setPublishedClientId] = useState("");
  const [publishedSiteId, setPublishedSiteId] = useState("");
  const [publishedSort, setPublishedSort] = useState("published_at");
  const [queueStats, setQueueStats] = useState(null);
  const [queueStatsLoading, setQueueStatsLoading] = useState(false);
  const [queueAutoRefresh, setQueueAutoRefresh] = useState(true);
  const queueAutoRefreshRef = useRef(true);
  const [siteSuggestionsBlockId, setSiteSuggestionsBlockId] = useState(null);
  const [clientSuggestionsBlockId, setClientSuggestionsBlockId] = useState(null);
  const [targetSiteSuggestionsBlockId, setTargetSiteSuggestionsBlockId] = useState(null);
  const submissionBlocksRef = useRef(null);
  const clientSuggestInputRefs = useRef({});
  const siteSuggestInputRefs = useRef({});
  const targetSiteSuggestInputRefs = useRef({});
  const submissionFieldErrorTimersRef = useRef({});
  const [suggestionStyle, setSuggestionStyle] = useState(null);
  const [uploadProgressBlockId, setUploadProgressBlockId] = useState(null);
  const inactivityTimerRef = useRef(null);
  const portalRefreshInFlightRef = useRef(false);

  const t = useMemo(() => (key) => getLabel(language, key), [language]);

  const serializeCreateArticleBlock = useCallback((block) => ({
    id: Number(block?.id || 0),
    client_name: (block?.client_name || "").trim(),
    publishing_site: (block?.publishing_site || "").trim(),
    target_site_id: (block?.target_site_id || "").trim(),
    target_site_url: (block?.target_site_url || "").trim(),
    anchor: (block?.anchor || "").trim(),
    topic: (block?.topic || "").trim(),
  }), []);

  const deserializeCreateArticleBlock = useCallback((raw, fallbackId) => {
    const parsedId = Number(raw?.id);
    const id = Number.isFinite(parsedId) && parsedId > 0 ? Math.floor(parsedId) : fallbackId;
    return createSubmissionBlock(id, {
      client_name: (raw?.client_name || "").trim(),
      publishing_site: (raw?.publishing_site || "").trim(),
      target_site_id: (raw?.target_site_id || "").trim(),
      target_site_url: (raw?.target_site_url || "").trim(),
      anchor: (raw?.anchor || "").trim(),
      topic: (raw?.topic || "").trim(),
    });
  }, []);

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
    if (isCreateArticleSection) {
      createArticleBlockIdRef.current = 2;
      setCreateArticleSubmissionBlocks([createSubmissionBlock(1, getDefaultSubmissionTargetSite())]);
    } else {
      submitArticleBlockIdRef.current = 2;
      setSubmitArticleSubmissionBlocks([createSubmissionBlock(1)]);
    }
    setSiteSuggestionsBlockId(null);
    setClientSuggestionsBlockId(null);
    setUploadProgressBlockId(null);
    setSubmissionFieldErrors({});
    setBatchBlockStatus({});
  };

  const resetClientSubmissionState = () => {
    clearCreatorDraftStorage();
    setClients([]);
    setSites([]);
    setError("");
    setSuccess("");
    submitArticleBlockIdRef.current = 2;
    createArticleBlockIdRef.current = 2;
    setSubmitArticleSubmissionBlocks([createSubmissionBlock(1)]);
    setCreateArticleSubmissionBlocks([createSubmissionBlock(1, getDefaultSubmissionTargetSite())]);
    setSiteSuggestionsBlockId(null);
    setClientSuggestionsBlockId(null);
    setUploadProgressBlockId(null);
    setShowSubmissionSuccessModal(false);
    setShowSubmissionErrorModal(false);
    setSubmissionErrorCode("");
    setSubmissionErrorMessage("");
    setSubmissionFieldErrors({});
    setBatchBlockStatus({});
    setCreatorJobsByBlock({});
    setCreatorProgress({});
    setCreatorCancelErrorByBlock({});
    setCreatorCancelingByBlock({});
    setCreatorCancelConfirmByBlock({});
    setCreatorDraftsHydrated(false);
  };

  const setSubmissionBlockField = (blockId, field, value) => {
    const setter = isCreateArticleSection ? setCreateArticleSubmissionBlocks : setSubmitArticleSubmissionBlocks;
    setter((prev) => prev.map((block) => (block.id === blockId ? { ...block, [field]: value } : block)));
  };

  const clearSubmissionFieldError = useCallback((blockId, field) => {
    setSubmissionFieldErrors((prev) => ({
      ...prev,
      [blockId]: {
        ...(prev[blockId] || {}),
        [field]: false,
      },
    }));
    const timerKey = `${blockId}:${field}`;
    const existingTimer = submissionFieldErrorTimersRef.current[timerKey];
    if (existingTimer) {
      clearTimeout(existingTimer);
      delete submissionFieldErrorTimersRef.current[timerKey];
    }
  }, []);

  const updateActiveSubmissionBlocks = (updater) => {
    const setter = isCreateArticleSection ? setCreateArticleSubmissionBlocks : setSubmitArticleSubmissionBlocks;
    setter((prev) => updater(prev));
  };

  const updateAllSubmissionBlocks = (updater) => {
    setSubmitArticleSubmissionBlocks((prev) => updater(prev));
    setCreateArticleSubmissionBlocks((prev) => updater(prev));
  };

  const addSubmissionBlock = (afterBlockId) => {
    const setter = isCreateArticleSection ? setCreateArticleSubmissionBlocks : setSubmitArticleSubmissionBlocks;
    const idRef = isCreateArticleSection ? createArticleBlockIdRef : submitArticleBlockIdRef;
    setter((prev) => {
      const nextId = idRef.current;
      idRef.current += 1;
      const nextBlock = createSubmissionBlock(nextId, isCreateArticleSection ? getDefaultSubmissionTargetSite() : {});
      const insertIndex = prev.findIndex((block) => block.id === afterBlockId);
      if (insertIndex < 0) return [...prev, nextBlock];
      return [...prev.slice(0, insertIndex + 1), nextBlock, ...prev.slice(insertIndex + 1)];
    });
  };

  const removeSubmissionBlock = (blockId) => {
    const trackedJobIds = creatorJobsByBlock[blockId] || [];
    const setter = isCreateArticleSection ? setCreateArticleSubmissionBlocks : setSubmitArticleSubmissionBlocks;
    setter((prev) => {
      if (prev.length <= 1) return prev;
      const next = prev.filter((block) => block.id !== blockId);
      return next.length ? next : [createSubmissionBlock(1, isCreateArticleSection ? getDefaultSubmissionTargetSite() : {})];
    });
    setSiteSuggestionsBlockId((prev) => (prev === blockId ? null : prev));
    setClientSuggestionsBlockId((prev) => (prev === blockId ? null : prev));
    setTargetSiteSuggestionsBlockId((prev) => (prev === blockId ? null : prev));
    setUploadProgressBlockId((prev) => (prev === blockId ? null : prev));
    setCreatorJobsByBlock((prev) => {
      const next = { ...prev };
      delete next[blockId];
      return next;
    });
    setCreatorCancelErrorByBlock((prev) => {
      const next = { ...prev };
      delete next[blockId];
      return next;
    });
    setCreatorCancelingByBlock((prev) => {
      const next = { ...prev };
      delete next[blockId];
      return next;
    });
    setCreatorCancelConfirmByBlock((prev) => {
      const next = { ...prev };
      delete next[blockId];
      return next;
    });
    if (trackedJobIds.length > 0) {
      setCreatorProgress((prev) => {
        const next = { ...prev };
        trackedJobIds.forEach((jobId) => {
          delete next[jobId];
        });
        return next;
      });
    }
  };

  const openSubmissionErrorSupportModal = ({ statusCode, message, blockIndex }) => {
    const code = statusCode ? `HTTP_${statusCode}` : "CLIENT_SUBMIT_UNKNOWN";
    const prefix = typeof blockIndex === "number" ? `Block ${blockIndex + 1}: ` : "";
    setSubmissionErrorCode(code);
    setSubmissionErrorMessage(`${prefix}${message || t("errorRequestFailed")}`);
    setShowSubmissionErrorModal(true);
  };

  const getSubmissionBlockError = (block, { isCreateArticle, clientName, requiresTargetSite }) => {
    const effectiveClientName = ((block.client_name || "").trim() || clientName);
    if (!effectiveClientName) return t("errorClientRequired");
    if (requiresTargetSite && !(block.target_site_id || "").trim() && !(block.target_site_url || "").trim()) {
      return t("errorClientTargetSiteRequired");
    }
    const publishingSite = (block.publishing_site || "").trim();
    if (!isCreateArticle && !publishingSite) return t("errorTargetRequired");
    const sourceType = isCreateArticle ? "" : (block.source_type || "").trim();
    if (!isCreateArticle && !sourceType) return t("errorFileTypeRequired");
    if (!isCreateArticle && sourceType === "google-doc" && !(block.doc_url || "").trim()) return t("errorGoogleDocRequired");
    if (sourceType === "word-doc" && !block.docx_file) return t("errorDocxRequired");
    return "";
  };

  const buildSubmissionFormData = (block, { isCreateArticle, clientName }) => {
    const formData = new FormData();
    const sourceType = isCreateArticle ? "google-doc" : (block.source_type || "").trim();
    const effectiveClientName = ((block.client_name || "").trim() || clientName);
    const publishingSite = (block.publishing_site || "").trim();
    if (publishingSite) formData.append("publishing_site", publishingSite);
    formData.append("client_name", effectiveClientName);
    if (isCreateArticle) {
      const targetSiteId = (block.target_site_id || "").trim();
      if (/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(targetSiteId)) {
        formData.append("target_site_id", targetSiteId);
      }
      if ((block.target_site_url || "").trim()) formData.append("target_site_url", block.target_site_url.trim());
    }
    formData.append("request_kind", isCreateArticle ? "create_article" : "submit_article");
    formData.append("source_type", sourceType);
    formData.append("execution_mode", "async");
    if ((block.anchor || "").trim()) formData.append("anchor", block.anchor.trim());
    if ((block.topic || "").trim()) formData.append("topic", block.topic.trim());
    if (isCreateArticle) formData.append("creator_mode", "true");
    if (!isCreateArticle && sourceType === "google-doc") {
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
    updateAllSubmissionBlocks((prev) => prev.map((block) => {
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
    updateAllSubmissionBlocks((prev) => prev.map((block) => (
      (block.client_name || "").trim() ? block : { ...block, client_name: fallbackClientName }
    )));
  }, [clients, currentUser?.role]);

  useEffect(() => {
    const activeKeys = new Set();
    for (const [blockId, errors] of Object.entries(submissionFieldErrors || {})) {
      for (const field of ["client_name", "publishing_site", "target_site"]) {
        if (!errors?.[field]) continue;
        const timerKey = `${blockId}:${field}`;
        activeKeys.add(timerKey);
        if (!submissionFieldErrorTimersRef.current[timerKey]) {
          submissionFieldErrorTimersRef.current[timerKey] = setTimeout(() => {
            clearSubmissionFieldError(blockId, field);
          }, 5000);
        }
      }
    }
    for (const timerKey of Object.keys(submissionFieldErrorTimersRef.current)) {
      if (!activeKeys.has(timerKey)) {
        clearTimeout(submissionFieldErrorTimersRef.current[timerKey]);
        delete submissionFieldErrorTimersRef.current[timerKey];
      }
    }
  }, [submissionFieldErrors, clearSubmissionFieldError]);

  useEffect(() => () => {
    for (const timer of Object.values(submissionFieldErrorTimersRef.current)) {
      clearTimeout(timer);
    }
    submissionFieldErrorTimersRef.current = {};
  }, []);

  useEffect(() => {
    if (theme === "dark") {
      document.documentElement.removeAttribute("data-theme");
    } else {
      document.documentElement.setAttribute("data-theme", "light");
    }
    localStorage.setItem("theme", theme);
  }, [theme]);

  useEffect(() => {
    localStorage.setItem("portal_sidebar_hidden", sidebarHidden ? "true" : "false");
  }, [sidebarHidden]);

  useEffect(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") return undefined;
    const widthMedia = window.matchMedia("(max-width: 1080px)");

    const onWidthMediaChange = (event) => {
      const narrow = Boolean(event.matches);
      setIsNarrowViewport(narrow);
      if (narrow) setSidebarHidden(true);
    };

    setIsNarrowViewport(widthMedia.matches);
    if (widthMedia.matches) setSidebarHidden(true);

    if (typeof widthMedia.addEventListener === "function") {
      widthMedia.addEventListener("change", onWidthMediaChange);
      return () => {
        widthMedia.removeEventListener("change", onWidthMediaChange);
      };
    }

    widthMedia.addListener(onWidthMediaChange);
    return () => {
      widthMedia.removeListener(onWidthMediaChange);
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") return undefined;
    const mobileMedia = window.matchMedia("(max-width: 900px)");

    const onMobileMediaChange = (event) => {
      const mobile = Boolean(event.matches);
      setIsMobileViewport(mobile);
      if (!mobile) setMobileMenuOpen(false);
    };

    setIsMobileViewport(mobileMedia.matches);
    if (typeof mobileMedia.addEventListener === "function") {
      mobileMedia.addEventListener("change", onMobileMediaChange);
      return () => {
        mobileMedia.removeEventListener("change", onMobileMediaChange);
      };
    }

    mobileMedia.addListener(onMobileMediaChange);
    return () => {
      mobileMedia.removeListener(onMobileMediaChange);
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
      const isAdmin = forUser?.role === "admin";
      const sitesPath = forUser?.role === "client" ? "/sites?status=active&ready_only=true" : "/sites";
      if (isAdmin) {
        const [clientsData, sitesData, readySitesData] = await Promise.all([
          api.get("/clients"),
          api.get("/sites"),
          api.get("/sites?status=active&ready_only=true"),
        ]);
        setClients((clientsData || []).filter((item) => item.status === "active"));
        setSites((sitesData || []).filter((item) => item.status === "active"));
        setReadySites((readySitesData || []).filter((item) => item.status === "active"));
      } else {
        const [clientsData, sitesData] = await Promise.all([api.get("/clients"), api.get(sitesPath)]);
        setClients((clientsData || []).filter((item) => item.status === "active"));
        setSites((sitesData || []).filter((item) => item.status === "active"));
        setReadySites((sitesData || []).filter((item) => item.status === "active"));
      }
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

  const loadPublishedArticles = async (forUser = currentUser, overrides = {}) => {
    if (forUser?.role !== "admin") return;
    try {
      setPublishedLoading(true);
      const params = new URLSearchParams();
      const nextQuery = (overrides.query ?? publishedQuery).trim();
      const nextClientId = ((overrides.clientId ?? publishedClientId) || "").trim();
      const nextSiteId = ((overrides.siteId ?? publishedSiteId) || "").trim();
      const nextLimit = Number(overrides.limit ?? publishedLimit) || publishedLimit;
      const nextOffset = Number(overrides.offset ?? publishedOffset) || 0;
      const nextSort = ((overrides.sort ?? publishedSort) || "published_at").trim();
      params.set("limit", String(nextLimit));
      params.set("offset", String(nextOffset));
      if (nextSort) params.set("sort", nextSort);
      if (nextQuery) params.set("q", nextQuery);
      if (nextClientId) params.set("client_id", nextClientId);
      if (nextSiteId) params.set("site_id", nextSiteId);
      const payload = await api.get(`/jobs/published?${params.toString()}`);
      const items = Array.isArray(payload?.items) ? payload.items : [];
      setPublishedArticles(items);
      setPublishedTotal(Number(payload?.total || 0));
      setPublishedLimit(Number(payload?.limit || nextLimit));
      setPublishedOffset(Number(payload?.offset || nextOffset));
    } catch (err) {
      setError(err.message);
    } finally {
      setPublishedLoading(false);
    }
  };

  const loadQueueStats = async () => {
    try {
      setQueueStatsLoading(true);
      const data = await api.get("/queue/stats");
      setQueueStats(data);
    } catch (err) {
      console.error("Failed to load queue stats", err);
    } finally {
      setQueueStatsLoading(false);
    }
  };

  const loadKeywordTrendDashboard = async (forUser = currentUser) => {
    if (forUser?.role !== "admin") return;
    try {
      setKeywordTrendLoading(true);
      const data = await api.get("/admin/keyword-trends/dashboard");
      setKeywordTrendDashboard(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setKeywordTrendLoading(false);
    }
  };

  const loadSiteFitDashboard = async (forUser = currentUser) => {
    if (forUser?.role !== "admin") return;
    try {
      setSiteFitLoading(true);
      const data = await api.get("/admin/site-fit/dashboard");
      setSiteFitDashboard(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setSiteFitLoading(false);
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

  const formatPublishedAt = (value) => {
    if (!value) return t("notAvailable");
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    const locale = language === "de" ? "de-DE" : "en-US";
    return date.toLocaleString(locale, { dateStyle: "medium", timeStyle: "short" });
  };

  const formatPublishedStatus = (value) => {
    const normalized = (value || "").trim().toLowerCase();
    if (!normalized) return t("notAvailable");
    if (normalized === "succeeded" || normalized === "published" || normalized === "publish") {
      return t("publishedStatusPublished");
    }
    if (normalized === "pending_approval") return t("publishedStatusPending");
    if (normalized === "failed") return t("publishedStatusFailed");
    return normalized.replace(/_/g, " ");
  };

  const summarizeSeoEvaluation = (evaluation) => {
    if (!evaluation || typeof evaluation !== "object") {
      return { score: null, issueCount: 0, passedCount: 0, failingChecks: [], topIssues: [] };
    }
    const score = typeof evaluation.score === "number" ? Math.round(evaluation.score) : null;
    const checks = evaluation.checks && typeof evaluation.checks === "object" ? evaluation.checks : {};
    const failingChecks = Object.entries(checks)
      .filter(([, issues]) => Array.isArray(issues) && issues.length > 0)
      .map(([name, issues]) => ({
        name,
        issues: issues.filter((item) => typeof item === "string" && item.trim()).slice(0, 3),
      }));
    const issueCount = failingChecks.reduce((sum, item) => sum + item.issues.length, 0);
    const passedCount = Math.max(0, Object.keys(checks).length - failingChecks.length);
    const topIssues = failingChecks.flatMap((item) => item.issues).slice(0, 4);
    return { score, issueCount, passedCount, failingChecks, topIssues };
  };

  const applyPublishedSearch = () => {
    setPublishedOffset(0);
    loadPublishedArticles(currentUser, { query: publishedQuery.trim(), offset: 0 });
  };

  const resetPublishedFilters = () => {
    setPublishedQuery("");
    setPublishedClientId("");
    setPublishedSiteId("");
    setPublishedSort("published_at");
    setPublishedLimit(PUBLISHED_PAGE_SIZE);
    setPublishedOffset(0);
    loadPublishedArticles(currentUser, {
      query: "",
      clientId: "",
      siteId: "",
      sort: "published_at",
      limit: PUBLISHED_PAGE_SIZE,
      offset: 0,
    });
  };

  const goToPublishedOffset = (nextOffset) => {
    const safeOffset = Math.max(0, nextOffset);
    setPublishedOffset(safeOffset);
    loadPublishedArticles(currentUser, { offset: safeOffset });
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
        setActiveSection(getLandingSectionForRole(user.role));
        setLoading(true);
        await loadAll(user);
        if (user.role === "admin") {
          await loadAdminUsers(user);
          await loadKeywordTrendDashboard(user);
          await loadSiteFitDashboard(user);
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
    if (!currentUser) {
      setCreatorDraftsHydrated(false);
      return;
    }
    if (typeof window === "undefined") {
      setCreatorDraftsHydrated(true);
      return;
    }

    const blocksKey = getCreateArticleBlocksStorageKey(currentUser);
    const jobsKey = getCreatorJobsStorageKey(currentUser);

    let restoredBlocks = [createSubmissionBlock(1)];
    try {
      const parsedBlocks = JSON.parse(localStorage.getItem(blocksKey) || "[]");
      if (Array.isArray(parsedBlocks) && parsedBlocks.length > 0) {
        restoredBlocks = parsedBlocks
          .map((item, index) => deserializeCreateArticleBlock(item, index + 1))
          .filter((item) => Number(item.id) > 0);
      }
    } catch {
      restoredBlocks = [createSubmissionBlock(1)];
    }

    let restoredJobsByBlock = {};
    try {
      const parsedJobs = JSON.parse(localStorage.getItem(jobsKey) || "{}");
      if (parsedJobs && typeof parsedJobs === "object" && !Array.isArray(parsedJobs)) {
        restoredJobsByBlock = Object.fromEntries(
          Object.entries(parsedJobs)
            .map(([blockId, jobIds]) => {
              const normalizedBlockId = Number(blockId);
              if (!Number.isFinite(normalizedBlockId) || normalizedBlockId <= 0) return null;
              const normalizedJobIds = Array.isArray(jobIds)
                ? Array.from(new Set(jobIds.map((jid) => String(jid || "").trim()).filter(Boolean)))
                : [];
              if (normalizedJobIds.length === 0) return null;
              return [normalizedBlockId, normalizedJobIds];
            })
            .filter(Boolean)
        );
      }
    } catch {
      restoredJobsByBlock = {};
    }

    const blockIds = new Set(restoredBlocks.map((block) => Number(block.id)));
    Object.keys(restoredJobsByBlock).forEach((rawBlockId) => {
      const blockId = Number(rawBlockId);
      if (blockIds.has(blockId)) return;
      restoredBlocks.push(createSubmissionBlock(blockId));
      blockIds.add(blockId);
    });

    restoredBlocks.sort((a, b) => Number(a.id) - Number(b.id));
    setCreateArticleSubmissionBlocks(restoredBlocks);

    const maxBlockId = restoredBlocks.reduce((max, block) => Math.max(max, Number(block.id) || 0), 0);
    createArticleBlockIdRef.current = Math.max(2, maxBlockId + 1);

    setCreatorJobsByBlock(restoredJobsByBlock);
    setCreatorProgress(() => {
      const seeded = {};
      Object.values(restoredJobsByBlock).flat().forEach((jobId) => {
        seeded[jobId] = {
          phase: 0,
          label: "",
          percent: 0,
          done: false,
          failed: false,
          canceled: false,
        };
      });
      return seeded;
    });
    setCreatorCancelErrorByBlock({});
    setCreatorCancelingByBlock({});
    setCreatorCancelConfirmByBlock({});
    setCreatorDraftsHydrated(true);
  }, [currentUser, deserializeCreateArticleBlock]);

  useEffect(() => {
    if (!currentUser || !creatorDraftsHydrated) return;
    if (typeof window === "undefined") return;

    const blocksKey = getCreateArticleBlocksStorageKey(currentUser);
    const jobsKey = getCreatorJobsStorageKey(currentUser);

    const serializedBlocks = (createArticleSubmissionBlocks || [])
      .map((block) => serializeCreateArticleBlock(block))
      .filter((block) => Number(block.id) > 0);

    localStorage.setItem(blocksKey, JSON.stringify(serializedBlocks.length ? serializedBlocks : [serializeCreateArticleBlock(createSubmissionBlock(1))]));

    const serializedJobsByBlock = Object.fromEntries(
      Object.entries(creatorJobsByBlock || {})
        .map(([blockId, jobIds]) => {
          const normalizedBlockId = Number(blockId);
          if (!Number.isFinite(normalizedBlockId) || normalizedBlockId <= 0) return null;
          const normalizedJobIds = Array.isArray(jobIds)
            ? Array.from(new Set(jobIds.map((jid) => String(jid || "").trim()).filter(Boolean)))
            : [];
          if (normalizedJobIds.length === 0) return null;
          return [normalizedBlockId, normalizedJobIds];
        })
        .filter(Boolean)
    );

    if (Object.keys(serializedJobsByBlock).length > 0) {
      localStorage.setItem(jobsKey, JSON.stringify(serializedJobsByBlock));
    } else {
      localStorage.removeItem(jobsKey);
    }
  }, [currentUser, creatorDraftsHydrated, createArticleSubmissionBlocks, creatorJobsByBlock, serializeCreateArticleBlock]);

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
    if (adminUsers.length === 0) loadAdminUsers(currentUser);
    if (!keywordTrendDashboard) loadKeywordTrendDashboard(currentUser);
    if (!siteFitDashboard) loadSiteFitDashboard(currentUser);
  }, [currentUser, activeSection]);

  useEffect(() => {
    if (!currentUser || currentUser.role !== "admin") return;
    if (activeSection !== "pending-jobs") return;
    loadPendingJobs();
  }, [currentUser, activeSection]);

  useEffect(() => {
    if (!currentUser || currentUser.role !== "admin") return;
    if (activeSection !== "published-articles") return;
    loadPublishedArticles();
  }, [currentUser, activeSection]);

  useEffect(() => {
    if (!currentUser || currentUser.role !== "admin") return;
    if (activeSection !== "queue-dashboard") return;
    loadQueueStats();
  }, [currentUser, activeSection]);

  useEffect(() => {
    if (!currentUser || currentUser.role !== "admin") return;
    if (activeSection !== "queue-dashboard") return;
    if (!queueAutoRefreshRef.current) return;
    const intervalId = window.setInterval(() => {
      if (queueAutoRefreshRef.current) loadQueueStats();
    }, 5000);
    return () => window.clearInterval(intervalId);
  }, [currentUser, activeSection, queueAutoRefresh]);

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
          if (activeSection === "published-articles") {
            await loadPublishedArticles(currentUser);
          }
          if (activeSection === "admin") {
            await loadAdminUsers(currentUser);
            await loadKeywordTrendDashboard(currentUser);
            await loadSiteFitDashboard(currentUser);
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
      setAuthError("");
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
      setActiveSection(getLandingSectionForRole(user.role));
      setLoading(true);
      await loadAll(user);
      if (user.role === "admin") {
        await loadAdminUsers(user);
        await loadKeywordTrendDashboard(user);
        await loadSiteFitDashboard(user);
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

  const attachCreatorJobsToBlock = useCallback((blockId, jobIds) => {
    const nextJobIds = (jobIds || []).filter(Boolean);
    if (nextJobIds.length === 0) return;
    setCreatorJobsByBlock((prev) => ({ ...prev, [blockId]: nextJobIds }));
    setCreatorProgress((prev) => {
      const next = { ...prev };
      nextJobIds.forEach((jobId) => {
        next[jobId] = next[jobId] || {
          phase: 0,
          label: "",
          percent: 0,
          done: false,
          failed: false,
          canceled: false,
        };
      });
      return next;
    });
    setCreatorCancelErrorByBlock((prev) => ({ ...prev, [blockId]: "" }));
    setCreatorCancelingByBlock((prev) => ({ ...prev, [blockId]: false }));
    setCreatorCancelConfirmByBlock((prev) => ({ ...prev, [blockId]: false }));
  }, []);

  const closeCreatorProgress = useCallback((blockId) => {
    const trackedJobIds = creatorJobsByBlock[blockId] || [];
    setCreatorJobsByBlock((prev) => {
      const next = { ...prev };
      delete next[blockId];
      return next;
    });
    setCreatorCancelErrorByBlock((prev) => {
      const next = { ...prev };
      delete next[blockId];
      return next;
    });
    setCreatorCancelingByBlock((prev) => {
      const next = { ...prev };
      delete next[blockId];
      return next;
    });
    setCreatorCancelConfirmByBlock((prev) => {
      const next = { ...prev };
      delete next[blockId];
      return next;
    });
    if (trackedJobIds.length > 0) {
      setCreatorProgress((prev) => {
        const next = { ...prev };
        trackedJobIds.forEach((jobId) => {
          delete next[jobId];
        });
        return next;
      });
    }
  }, [creatorJobsByBlock]);

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

  const trackedCreatorJobIds = useMemo(
    () => Array.from(new Set(Object.values(creatorJobsByBlock).flat().filter(Boolean))),
    [creatorJobsByBlock],
  );

  useEffect(() => {
    if (trackedCreatorJobIds.length === 0) {
      stopCreatorPolling();
      return undefined;
    }
    let cancelled = false;
    const poll = async () => {
      const updates = {};
      let allDone = true;
      const movedToPendingApproval = new Set();
      for (const jid of trackedCreatorJobIds) {
        try {
          const data = await api.get(`/automation/status?job_id=${encodeURIComponent(jid)}`);
          if (!data?.found) { allDone = false; continue; }
          const phaseEvents = (data.events || []).filter((e) => e.event_type === "creator_phase");
          const last = phaseEvents.length > 0 ? phaseEvents[phaseEvents.length - 1] : null;
          if (data.job_status === "pending_approval") {
            movedToPendingApproval.add(jid);
          }
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
        const movedToPendingIds = movedToPendingApproval;
        if (movedToPendingIds.size > 0) {
          setCreatorJobsByBlock((prev) => {
            const next = {};
            for (const [blockId, jobIds] of Object.entries(prev || {})) {
              const filtered = (jobIds || []).filter((jid) => !movedToPendingIds.has(jid));
              if (filtered.length > 0) next[blockId] = filtered;
            }
            return next;
          });
        }
        setCreatorProgress((prev) => {
          const next = { ...prev, ...updates };
          movedToPendingIds.forEach((jid) => {
            delete next[jid];
          });
          return next;
        });
        if (allDone) stopCreatorPolling();
      }
    };
    poll();
    creatorPollRef.current = setInterval(poll, 3000);
    return () => { cancelled = true; stopCreatorPolling(); };
  }, [trackedCreatorJobIds, stopCreatorPolling]);

  const cancelCreatorJobs = useCallback(async (blockId) => {
    const creatorJobIds = creatorJobsByBlock[blockId] || [];
    if (creatorCancelingByBlock[blockId] || creatorJobIds.length === 0) return;
    setCreatorCancelingByBlock((prev) => ({ ...prev, [blockId]: true }));
    setCreatorCancelErrorByBlock((prev) => ({ ...prev, [blockId]: "" }));
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
            label: t("createArticleCanceled"),
          };
        });
        return next;
      });
    } catch (err) {
      setCreatorCancelErrorByBlock((prev) => ({ ...prev, [blockId]: err?.message || t("errorRequestFailed") }));
    } finally {
      setCreatorCancelingByBlock((prev) => ({ ...prev, [blockId]: false }));
    }
  }, [creatorCancelingByBlock, creatorJobsByBlock, t]);

  const requestCancelCreatorJobs = useCallback((blockId) => {
    if (creatorCancelingByBlock[blockId]) return;
    setCreatorCancelConfirmByBlock((prev) => ({ ...prev, [blockId]: true }));
    setCreatorCancelErrorByBlock((prev) => ({ ...prev, [blockId]: "" }));
  }, [creatorCancelingByBlock]);

  const dismissCancelConfirm = useCallback((blockId) => {
    if (creatorCancelingByBlock[blockId]) return;
    setCreatorCancelConfirmByBlock((prev) => ({ ...prev, [blockId]: false }));
  }, [creatorCancelingByBlock]);

  const submitSubmissionBlock = async (block, blockIndex) => {
    setError("");
    setSuccess("");
    setShowSubmissionErrorModal(false);
    setSubmissionErrorCode("");
    setSubmissionErrorMessage("");
    setSubmissionFieldErrors({});

    if (activeSection === "admin") {
      setError(t("errorSelectArticleSection"));
      return;
    }
    const resolvedClientName = isAdminUser ? "" : ((clients[0]?.name) || "").trim();
    const requiresTargetSiteSelection = isCreateArticleSection;
    const validationError = getSubmissionBlockError(block, {
      isCreateArticle: isCreateArticleSection,
      clientName: resolvedClientName,
      requiresTargetSite: requiresTargetSiteSelection,
    });
    if (validationError) {
      if (
        validationError === t("errorFileTypeRequired")
        || validationError === t("errorClientRequired")
        || validationError === t("errorTargetRequired")
        || validationError === t("errorClientTargetSiteRequired")
      ) {
        const blockId = block.id;
        setSubmissionFieldErrors((prev) => ({
          ...prev,
          [blockId]: {
            ...(prev[blockId] || {}),
            ...(validationError === t("errorFileTypeRequired") ? { source_type: true } : {}),
            ...(validationError === t("errorClientRequired") ? { client_name: true } : {}),
            ...(validationError === t("errorTargetRequired") ? { publishing_site: true } : {}),
            ...(validationError === t("errorClientTargetSiteRequired") ? { target_site: true } : {}),
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
        isCreateArticle: isCreateArticleSection,
        clientName: resolvedClientName,
      });
      const effectiveSourceType = isCreateArticleSection ? "google-doc" : (block.source_type || "").trim();
      let responseData = null;
      if (effectiveSourceType === "word-doc" && block.docx_file) {
        setUploadProgressBlockId(block.id);
        setUploadProgress(0);
        responseData = await postMultipartWithProgress(`${baseApiUrl}/automation/submit-article-webhook`, formData);
        setUploadProgress(100);
      } else {
        setUploadProgressBlockId(null);
        const response = await fetch(`${baseApiUrl}/automation/submit-article-webhook`, {
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

      if (responseData?.job_id && isCreateArticleSection) {
        const jobId = responseData.job_id;
        attachCreatorJobsToBlock(block.id, [jobId]);
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

  const submitAllBlocks = async () => {
    setError("");
    setSuccess("");
    setShowSubmissionErrorModal(false);
    setSubmissionErrorCode("");
    setSubmissionErrorMessage("");
    setSubmissionFieldErrors({});
    setBatchBlockStatus({});

    const blocks = isCreateArticleSection ? createArticleSubmissionBlocks : submitArticleSubmissionBlocks;
    const resolvedClientName = isAdminUser ? "" : ((clients[0]?.name) || "").trim();
    const requiresTargetSiteSelection = isCreateArticleSection;

    // --- validate all blocks first ---
    const fieldErrors = {};
    const blockErrors = [];
    for (let i = 0; i < blocks.length; i++) {
      const block = blocks[i];
      const validationError = getSubmissionBlockError(block, {
        isCreateArticle: isCreateArticleSection,
        clientName: resolvedClientName,
        requiresTargetSite: requiresTargetSiteSelection,
      });
      if (validationError) {
        if (validationError === t("errorFileTypeRequired")) {
          fieldErrors[block.id] = { ...(fieldErrors[block.id] || {}), source_type: true };
        } else if (validationError === t("errorClientRequired")) {
          fieldErrors[block.id] = { ...(fieldErrors[block.id] || {}), client_name: true };
        } else if (validationError === t("errorTargetRequired")) {
          fieldErrors[block.id] = { ...(fieldErrors[block.id] || {}), publishing_site: true };
        } else if (validationError === t("errorClientTargetSiteRequired")) {
          fieldErrors[block.id] = { ...(fieldErrors[block.id] || {}), target_site: true };
        }
        blockErrors.push(
          t("batchBlockError").replace("{n}", String(i + 1)).replace("{error}", validationError)
        );
      }
    }
    if (blockErrors.length) {
      setSubmissionFieldErrors(fieldErrors);
      setError(blockErrors.join("\n"));
      return;
    }

    // --- submit sequentially ---
    try {
      setSubmitting(true);
      const collectedJobsByBlock = {};
      let succeeded = 0;
      let failed = 0;

      for (let i = 0; i < blocks.length; i++) {
        const block = blocks[i];
        setBatchBlockStatus((prev) => ({ ...prev, [block.id]: "submitting" }));
        try {
          const formData = buildSubmissionFormData(block, {
            isCreateArticle: isCreateArticleSection,
            clientName: resolvedClientName,
          });
          const effectiveSourceType = isCreateArticleSection ? "google-doc" : (block.source_type || "").trim();
          let responseData = null;

          if (effectiveSourceType === "word-doc" && block.docx_file) {
            setUploadProgressBlockId(block.id);
            setUploadProgress(0);
            responseData = await postMultipartWithProgress(`${baseApiUrl}/automation/submit-article-webhook`, formData);
            setUploadProgress(100);
          } else {
            setUploadProgressBlockId(null);
            const response = await fetch(`${baseApiUrl}/automation/submit-article-webhook`, {
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

          if (responseData?.job_id && isCreateArticleSection) {
            collectedJobsByBlock[block.id] = [responseData.job_id];
          }
          succeeded++;
          setBatchBlockStatus((prev) => ({ ...prev, [block.id]: "success" }));
        } catch (blockErr) {
          failed++;
          setBatchBlockStatus((prev) => ({ ...prev, [block.id]: "error" }));
        }
      }

      // --- show results ---
      if (isCreateArticleSection && Object.keys(collectedJobsByBlock).length > 0) {
        Object.entries(collectedJobsByBlock).forEach(([blockId, jobIds]) => {
          attachCreatorJobsToBlock(Number(blockId), jobIds);
        });
      } else if (failed === 0) {
        setShowSubmissionSuccessModal(true);
      }

      if (failed > 0 && succeeded > 0) {
        setError(
          t("batchPartialSuccess")
            .replace("{succeeded}", String(succeeded))
            .replace("{total}", String(blocks.length))
            .replace("{failed}", String(failed))
        );
      } else if (failed > 0 && succeeded === 0) {
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

  const isAdminSection = activeSection === "admin";
  const isWebsitesSection = activeSection === "websites";
  const isClientsSection = activeSection === "clients";
  const isAdminUser = currentUser?.role === "admin";
  const isAdminPendingSection = isAdminUser && activeSection === "pending-jobs";
  const isPublishedArticlesSection = isAdminUser && activeSection === "published-articles";
  const isQueueDashboardSection = isAdminUser && activeSection === "queue-dashboard";
  const isClientDashboardSection = !isAdminUser && activeSection === "dashboard";
  const isCreateArticleSection = activeSection === "create-article";
  const isSubmitArticleSection = activeSection === "submit-article";
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
  const keywordTrendSummary = keywordTrendDashboard?.summary || {};
  const keywordTrendRecent = Array.isArray(keywordTrendDashboard?.recent_queries)
    ? keywordTrendDashboard.recent_queries.slice(0, TREND_RECENT_LIMIT)
    : [];
  const keywordTrendTotalQueries = Number(keywordTrendSummary.total_queries || 0);
  const keywordTrendFreshQueries = Number(keywordTrendSummary.fresh_queries || 0);
  const keywordTrendStaleQueries = Number(keywordTrendSummary.stale_queries || 0);
  const keywordTrendFreshPercent = keywordTrendTotalQueries > 0
    ? Math.round((keywordTrendFreshQueries / keywordTrendTotalQueries) * 100)
    : 0;
  const keywordTrendLatestRefresh = keywordTrendSummary.latest_refresh_at
    ? formatPublishedAt(keywordTrendSummary.latest_refresh_at)
    : t("notAvailable");
  const siteProfileSummary = siteFitDashboard?.profiles?.summary || {};
  const siteProfileRecent = Array.isArray(siteFitDashboard?.profiles?.recent_profiles)
    ? siteFitDashboard.profiles.recent_profiles.slice(0, SITE_FIT_RECENT_LIMIT)
    : [];
  const siteFitSummary = siteFitDashboard?.pair_fits?.summary || {};
  const siteFitRecent = Array.isArray(siteFitDashboard?.pair_fits?.recent_pair_fits)
    ? siteFitDashboard.pair_fits.recent_pair_fits.slice(0, SITE_FIT_RECENT_LIMIT)
    : [];
  const recentHostDecisions = Array.isArray(siteFitDashboard?.recent_host_decisions)
    ? siteFitDashboard.recent_host_decisions.slice(0, SITE_FIT_RECENT_LIMIT)
    : [];
  const siteProfileTotal = Number(siteProfileSummary.total_profiles || 0);
  const publishingProfileCount = Number(siteProfileSummary.publishing_profiles || 0);
  const targetProfileCount = Number(siteProfileSummary.target_profiles || 0);
  const latestProfileRefresh = siteProfileSummary.latest_profile_update_at
    ? formatPublishedAt(siteProfileSummary.latest_profile_update_at)
    : t("notAvailable");
  const totalPairFits = Number(siteFitSummary.total_pair_fits || 0);
  const acceptedPairFits = Number(siteFitSummary.accepted_pair_fits || 0);
  const rejectedPairFits = Number(siteFitSummary.rejected_pair_fits || 0);
  const acceptedPairFitPercent = totalPairFits > 0
    ? Math.round((acceptedPairFits / totalPairFits) * 100)
    : 0;
  const latestPairFitRefresh = siteFitSummary.latest_pair_fit_update_at
    ? formatPublishedAt(siteFitSummary.latest_pair_fit_update_at)
    : t("notAvailable");
  const autoSelectedHostCount = recentHostDecisions.filter((item) => item.auto_selected).length;
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
  const suggestedSubmittedArticlesMonthly = Math.max(4, Math.min(36, sites.length * 2));
  const suggestedCreatedArticlesMonthly = Math.max(2, Math.min(18, Math.ceil(Math.max(sites.length, 1) / 2)));
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
  const publishedPageCount = publishedTotal === 0 ? 0 : Math.ceil(publishedTotal / Math.max(publishedLimit, 1));
  const publishedPage = publishedTotal === 0
    ? 0
    : Math.min(publishedPageCount, Math.floor(publishedOffset / Math.max(publishedLimit, 1)) + 1);
  const publishedFrom = publishedTotal === 0 ? 0 : publishedOffset + 1;
  const publishedTo = publishedTotal === 0 ? 0 : Math.min(publishedOffset + publishedLimit, publishedTotal);
  const publishedCanPrev = publishedTotal > 0 && publishedOffset > 0;
  const publishedCanNext = publishedTotal > 0 && publishedOffset + publishedLimit < publishedTotal;
  const activeSuggestion = useMemo(() => {
    const activeBlocks = isCreateArticleSection ? createArticleSubmissionBlocks : submitArticleSubmissionBlocks;
    if (clientSuggestionsBlockId) {
      const block = activeBlocks.find((item) => item.id === clientSuggestionsBlockId);
      if (!block) return null;
      const items = isAdminUser
        ? sortByLabel(clients, (client) => (client.name || "").trim() || String(client.id || "")).filter((client) => {
            const q = (block.client_name || "").trim().toLowerCase();
            if (!q) return true;
            const name = ((client.name || "").trim()).toLowerCase();
            return name.includes(q);
          })
        : [];
      if (!items.length) return null;
      return {
        type: "client",
        blockId: block.id,
        items,
      };
    }
    if (siteSuggestionsBlockId) {
      const block = activeBlocks.find((item) => item.id === siteSuggestionsBlockId);
      if (!block) return null;
      const items = getFilteredSitesForQuery(block.publishing_site);
      if (!items.length) return null;
      return {
        type: "site",
        blockId: block.id,
        items,
      };
    }
    if (isCreateArticleSection && targetSiteSuggestionsBlockId) {
      const block = activeBlocks.find((item) => item.id === targetSiteSuggestionsBlockId);
      if (!block) return null;
      const selectedClient = isAdminUser
        ? clients.find((client) => (client.name || "").trim() === (block.client_name || "").trim())
        : null;
      const availableTargetSites = sortByLabel(
        isAdminUser ? getTargetSitesForClient(selectedClient) : clientTargetSites,
        (row) => `${row.target_site_domain || ""} ${row.target_site_url || ""}`,
      );
      const query = (block.target_site_url || "").trim().toLowerCase();
      const items = availableTargetSites.filter((row) => {
        if (!query) return true;
        const urlValue = (row.target_site_url || "").trim().toLowerCase();
        const domainValue = (row.target_site_domain || "").trim().toLowerCase();
        const domainUrlValue = domainValue ? `https://${domainValue}` : "";
        return (
          urlValue.includes(query)
          || domainValue.includes(query)
          || domainUrlValue.includes(query)
        );
      });
      if (!items.length) return null;
      return {
        type: "target",
        blockId: block.id,
        items,
      };
    }
    return null;
  }, [
    clientSuggestionsBlockId,
    siteSuggestionsBlockId,
    targetSiteSuggestionsBlockId,
    isCreateArticleSection,
    createArticleSubmissionBlocks,
    submitArticleSubmissionBlocks,
    clients,
    sites,
    isAdminUser,
    clientTargetSites,
    getFilteredSitesForQuery,
  ]);
  const activeSuggestionKey = activeSuggestion
    ? `${activeSuggestion.type}:${activeSuggestion.blockId}:${activeSuggestion.items.length}`
    : "";
  const updateSuggestionPosition = useCallback(() => {
    if (!activeSuggestion || !submissionBlocksRef.current) {
      setSuggestionStyle(null);
      return;
    }
    const anchorMap = activeSuggestion.type === "client"
      ? clientSuggestInputRefs
      : activeSuggestion.type === "site"
        ? siteSuggestInputRefs
        : targetSiteSuggestInputRefs;
    const anchor = anchorMap.current[activeSuggestion.blockId];
    if (!anchor) {
      setSuggestionStyle(null);
      return;
    }
    const containerRect = submissionBlocksRef.current.getBoundingClientRect();
    const anchorRect = anchor.getBoundingClientRect();
    setSuggestionStyle({
      top: Math.round(anchorRect.bottom - containerRect.top + 6),
      left: Math.round(anchorRect.left - containerRect.left),
      width: Math.round(anchorRect.width),
    });
  }, [activeSuggestion]);

  useLayoutEffect(() => {
    if (!activeSuggestion) {
      setSuggestionStyle(null);
      return;
    }
    let frame = 0;
    const handle = () => {
      if (frame) cancelAnimationFrame(frame);
      frame = requestAnimationFrame(() => updateSuggestionPosition());
    };
    handle();
    window.addEventListener("scroll", handle, true);
    window.addEventListener("resize", handle);
    return () => {
      if (frame) cancelAnimationFrame(frame);
      window.removeEventListener("scroll", handle, true);
      window.removeEventListener("resize", handle);
    };
  }, [activeSuggestionKey, updateSuggestionPosition]);
  const renderSuggestionItems = (suggestion) => {
    if (!suggestion) return null;
    if (suggestion.type === "client") {
      return suggestion.items.slice(0, 30).map((client) => (
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
            updateActiveSubmissionBlocks((prev) => prev.map((item) => (
              item.id === suggestion.blockId
                ? {
                    ...item,
                    client_name: nextClientName,
                    target_site_id: nextPrimary ? String(nextPrimary.id || "") : "",
                    target_site_url: nextPrimary ? (nextPrimary.target_site_url || "").trim() : "",
                  }
                : item
            )));
            clearSubmissionFieldError(suggestion.blockId, "client_name");
            setClientSuggestionsBlockId(null);
          }}
        >
          <span>{(client.name || "").trim() || client.id}</span>
          <span className="muted-text small-text">{client.email || client.id}</span>
        </button>
      ));
    }
    if (suggestion.type === "site") {
      return suggestion.items.slice(0, 30).map((site) => (
        <button
          key={site.id}
          type="button"
          className="site-suggest-item"
          onMouseDown={(event) => {
            event.preventDefault();
            setSubmissionBlockField(suggestion.blockId, "publishing_site", site.site_url);
            clearSubmissionFieldError(suggestion.blockId, "publishing_site");
            setSiteSuggestionsBlockId(null);
          }}
        >
          <span>{site.site_url}</span>
          <span className="muted-text small-text">{site.name}</span>
        </button>
      ));
    }
    return suggestion.items.slice(0, 30).map((row) => {
      const optionId = String(row.id || "");
      const domainLabel = (row.target_site_domain || "").trim();
      const urlLabel = (row.target_site_url || "").trim();
      const label = urlLabel || (domainLabel ? `https://${domainLabel}` : "") || optionId;
      return (
        <button
          key={optionId}
          type="button"
          className="site-suggest-item"
          onMouseDown={(event) => {
            event.preventDefault();
            const nextUrl = label;
            updateActiveSubmissionBlocks((prev) => prev.map((item) => (
              item.id === suggestion.blockId
                ? {
                    ...item,
                    target_site_id: optionId,
                    target_site_url: nextUrl,
                  }
                : item
            )));
            clearSubmissionFieldError(suggestion.blockId, "target_site");
            setTargetSiteSuggestionsBlockId(null);
          }}
        >
          <span>{label}</span>
          <span className="muted-text small-text">{domainLabel || optionId}</span>
        </button>
      );
    });
  };

  if (authLoading) {
    return (
      <div className="auth-shell">
        <div className="auth-loading" role="status" aria-live="polite">
          <span className="sr-only">{t("loading")}</span>
        </div>
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

  return (
    <div className={`app-shell ${sidebarHidden ? "sidebar-hidden" : ""}`.trim()}>
      <div className="brand">
        <div className="brand-logo">e</div>
        <div>
          <strong>Elci Solutions</strong>
          <span>Operations Hub</span>
        </div>
      </div>
      <Sidebar
        t={t}
        userRole={currentUser.role}
        activeSection={activeSection}
        onSectionChange={(next) => {
          setActiveSection(next);
          if (isMobileViewport) setMobileMenuOpen(false);
          if (isNarrowViewport && !isMobileViewport) setSidebarHidden(true);
        }}
        pendingJobsCount={pendingJobs.length}
      />
      {!isMobileViewport ? (
        <button
          className="sidebar-collapse-btn"
          type="button"
          onClick={() => setSidebarHidden((prev) => !prev)}
          aria-label={sidebarHidden ? "Show side panel" : "Hide side panel"}
          title={sidebarHidden ? "Show side panel" : "Hide side panel"}
        >
          <svg viewBox="0 0 24 24" role="img" focusable="false" aria-hidden="true">
            {sidebarHidden ? (
              <>
                <rect x="3" y="3" width="18" height="18" rx="3" fill="none" stroke="currentColor" strokeWidth="1.5" />
                <line x1="9" y1="3" x2="9" y2="21" stroke="currentColor" strokeWidth="1.5" />
                <path d="M13 10l2 2-2 2" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
              </>
            ) : (
              <>
                <rect x="3" y="3" width="18" height="18" rx="3" fill="none" stroke="currentColor" strokeWidth="1.5" />
                <line x1="9" y1="3" x2="9" y2="21" stroke="currentColor" strokeWidth="1.5" />
                <path d="M15 10l-2 2 2 2" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
              </>
            )}
          </svg>
        </button>
      ) : null}
      {!isMobileViewport && isNarrowViewport && !sidebarHidden ? (
        <button
          type="button"
          className="sidebar-backdrop"
          aria-label="Close menu"
          onClick={() => setSidebarHidden(true)}
        />
      ) : null}

      <div className="header">
        <div className="title">{isAdminUser ? t("heroAdminPanel") : t("clientsPortal")}</div>
        {!isMobileViewport ? (
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
        ) : (
          <>
            <button
              type="button"
              className={`mobile-menu-btn ${mobileMenuOpen ? "open" : ""}`.trim()}
              aria-label={mobileMenuOpen ? "Close menu" : "Open menu"}
              aria-expanded={mobileMenuOpen}
              onClick={() => setMobileMenuOpen((prev) => !prev)}
            >
              <svg viewBox="0 0 24 24" role="img" focusable="false" aria-hidden="true">
                {mobileMenuOpen ? (
                  <path
                    d="M6 6l12 12M18 6L6 18"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="1.8"
                    strokeLinecap="round"
                  />
                ) : (
                  <path
                    d="M4 7h16M4 12h16M4 17h16"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="1.8"
                    strokeLinecap="round"
                  />
                )}
              </svg>
            </button>
            {mobileMenuOpen ? (
              <>
                <button
                  type="button"
                  className="mobile-menu-backdrop"
                  aria-label="Close account controls"
                  onClick={() => setMobileMenuOpen(false)}
                />
                <div className="mobile-menu-panel">
                  <div className="user-chip mobile-menu-user">
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
                  <button className="btn secondary mobile-menu-logout" type="button" onClick={handleLogout}>
                    {t("logout")}
                  </button>
                </div>
              </>
            ) : null}
          </>
        )}
      </div>
      <div className="app-main">

        <div className={`container ${(isAdminPendingSection || isPublishedArticlesSection || isQueueDashboardSection) ? "container-wide" : ""} ${(isSubmitArticleSection || isCreateArticleSection) ? "request-container" : ""}`.trim()}>
          {(isSubmitArticleSection || isCreateArticleSection) ? (
            <div className="hero">
              <h1>{isCreateArticleSection ? t("heroCreateArticle") : t("heroSubmitArticle")}</h1>
            </div>
          ) : null}

          {isAdminSection ? (
            <div className="stats-grid admin-kpi-grid">
              <div className="stat-card" style={{"--i": 0}}>
                <span className="stat-label">{t("statTotalSites")}</span>
                <strong>{sites.length}</strong>
              </div>
              <div className="stat-card" style={{"--i": 1}}>
                <span className="stat-label">{t("statActiveSites")}</span>
                <strong>{readySites.length}</strong>
              </div>
              <div className="stat-card" style={{"--i": 2}}>
                <span className="stat-label">{t("kpiTotalUsers")}</span>
                <strong>{adminUsers.length}</strong>
              </div>
              <div className="stat-card" style={{"--i": 3}}>
                <span className="stat-label">{t("kpiAdmins")}</span>
                <strong>{adminCount}</strong>
              </div>
              <div className="stat-card" style={{"--i": 4}}>
                <span className="stat-label">{t("kpiClientUsers")}</span>
                <strong>{clientUserCount}</strong>
              </div>
              <div className="stat-card" style={{"--i": 5}}>
                <span className="stat-label">{t("kpiInactiveUsers")}</span>
                <strong>{inactiveUserCount}</strong>
              </div>
              <div className="stat-card" style={{"--i": 6}}>
                <span className="stat-label">{t("kpiMappedClientUsers")}</span>
                <strong>{mappedClientUserCount}</strong>
              </div>
              <div className="stat-card" style={{"--i": 7}}>
                <span className="stat-label">{t("kpiUnmappedClientUsers")}</span>
                <strong>{unmappedClientUserCount}</strong>
              </div>
            </div>
          ) : null}

          {loading ? (
            <div className="panel loading-panel" role="status" aria-live="polite">
              <span className="sr-only">{t("loading")}</span>
            </div>
          ) : null}
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
                <div className="panel admin-summary-card">
                  <h3>{t("adminTrendCacheTitle")}</h3>
                  <p className="muted-text">{t("adminTrendCacheBody")}</p>
                  <strong className="admin-summary-number">
                    {keywordTrendTotalQueries > 0 ? `${keywordTrendFreshPercent}%` : "0%"}
                  </strong>
                  <span className="muted-text">
                    {keywordTrendFreshQueries} / {keywordTrendTotalQueries} {t("adminTrendCacheFreshLabel")}
                  </span>
                </div>
                <div className="panel admin-summary-card">
                  <h3>{t("adminTrendCacheStaleTitle")}</h3>
                  <p className="muted-text">{t("adminTrendCacheStaleBody")}</p>
                  <strong className="admin-summary-number">{keywordTrendStaleQueries}</strong>
                  <span className="muted-text">
                    {t("adminTrendCacheLatestRefresh")}: {keywordTrendLatestRefresh}
                  </span>
                </div>
                <div className="panel admin-summary-card">
                  <h3>{t("adminSiteProfileTitle")}</h3>
                  <p className="muted-text">{t("adminSiteProfileBody")}</p>
                  <strong className="admin-summary-number">{siteProfileTotal}</strong>
                  <span className="muted-text">
                    {publishingProfileCount} / {targetProfileCount} {t("adminSiteProfileSplitLabel")}
                  </span>
                </div>
                <div className="panel admin-summary-card">
                  <h3>{t("adminPairFitTitle")}</h3>
                  <p className="muted-text">{t("adminPairFitBody")}</p>
                  <strong className="admin-summary-number">
                    {totalPairFits > 0 ? `${acceptedPairFitPercent}%` : "0%"}
                  </strong>
                  <span className="muted-text">
                    {acceptedPairFits} / {totalPairFits} {t("adminPairFitAcceptedLabel")}
                  </span>
                </div>
                <div className="panel admin-summary-card">
                  <h3>{t("adminPairFitRejectedTitle")}</h3>
                  <p className="muted-text">{t("adminPairFitRejectedBody")}</p>
                  <strong className="admin-summary-number">{rejectedPairFits}</strong>
                  <span className="muted-text">
                    {t("adminPairFitLatestRefresh")}: {latestPairFitRefresh}
                  </span>
                </div>
                <div className="panel admin-summary-card">
                  <h3>{t("adminAutoHostTitle")}</h3>
                  <p className="muted-text">{t("adminAutoHostBody")}</p>
                  <strong className="admin-summary-number">{autoSelectedHostCount}</strong>
                  <span className="muted-text">
                    {t("adminSiteProfileLatestRefresh")}: {latestProfileRefresh}
                  </span>
                </div>
              </div>

              <div className="admin-trend-panel">
                <div className="admin-trend-header">
                  <div>
                    <h3>{t("adminTrendCacheRecentTitle")}</h3>
                    <p className="muted-text">{t("adminTrendCacheRecentBody")}</p>
                  </div>
                  <button
                    className="btn secondary small"
                    type="button"
                    onClick={() => loadKeywordTrendDashboard()}
                    disabled={keywordTrendLoading}
                  >
                    {keywordTrendLoading ? t("loading") : t("refresh")}
                  </button>
                </div>
                {keywordTrendRecent.length === 0 && !keywordTrendLoading ? (
                  <p className="muted-text">{t("adminTrendCacheNoData")}</p>
                ) : (
                  <div className="admin-trend-list">
                    {keywordTrendRecent.map((item) => (
                      <div key={`${item.normalized_query}-${item.updated_at || item.fetched_at || ""}`} className="admin-trend-item">
                        <div className="admin-trend-query-row">
                          <strong>{item.query || item.normalized_query}</strong>
                          <span className={`admin-trend-status ${item.is_fresh ? "fresh" : "stale"}`}>
                            {item.is_fresh ? t("adminTrendCacheStatusFresh") : t("adminTrendCacheStatusStale")}
                          </span>
                        </div>
                        <div className="admin-trend-meta">
                          <span>{item.locale}</span>
                          <span>{t("adminTrendCacheSuggestionCount")}: {item.suggestion_count || 0}</span>
                          <span>{t("adminTrendCacheHitCount")}: {item.hit_count || 0}</span>
                          <span>{t("adminTrendCacheLatestRefresh")}: {item.fetched_at ? formatPublishedAt(item.fetched_at) : t("notAvailable")}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div className="admin-trend-panel">
                <div className="admin-trend-header">
                  <div>
                    <h3>{t("adminSiteProfileRecentTitle")}</h3>
                    <p className="muted-text">{t("adminSiteProfileRecentBody")}</p>
                  </div>
                  <button
                    className="btn secondary small"
                    type="button"
                    onClick={() => loadSiteFitDashboard()}
                    disabled={siteFitLoading}
                  >
                    {siteFitLoading ? t("loading") : t("refresh")}
                  </button>
                </div>
                {siteProfileRecent.length === 0 && !siteFitLoading ? (
                  <p className="muted-text">{t("adminSiteProfileNoData")}</p>
                ) : (
                  <div className="admin-trend-list">
                    {siteProfileRecent.map((item) => (
                      <div key={`${item.normalized_url}-${item.updated_at || ""}`} className="admin-trend-item">
                        <div className="admin-trend-query-row">
                          <strong>{item.normalized_url}</strong>
                          <span className="admin-trend-status neutral">
                            {item.profile_kind === "publishing_site" ? t("adminSiteProfileKindPublishing") : t("adminSiteProfileKindTarget")}
                          </span>
                        </div>
                        <div className="admin-trend-meta">
                          <span>{item.primary_context || item.domain_level_topic || t("notAvailable")}</span>
                          <span>{t("adminSiteProfileGeneratorLabel")}: {item.generator_mode || t("notAvailable")}</span>
                          <span>{t("adminSiteProfileLatestRefresh")}: {item.updated_at ? formatPublishedAt(item.updated_at) : t("notAvailable")}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div className="admin-trend-panel">
                <div className="admin-trend-header">
                  <div>
                    <h3>{t("adminPairFitRecentTitle")}</h3>
                    <p className="muted-text">{t("adminPairFitRecentBody")}</p>
                  </div>
                </div>
                {siteFitRecent.length === 0 && !siteFitLoading ? (
                  <p className="muted-text">{t("adminPairFitNoData")}</p>
                ) : (
                  <div className="admin-trend-list">
                    {siteFitRecent.map((item) => (
                      <div key={`${item.publishing_site_url}-${item.target_url}-${item.updated_at || ""}`} className="admin-trend-item">
                        <div className="admin-trend-query-row">
                          <strong>{item.final_article_topic || `${item.publishing_site_name} -> ${item.target_url}`}</strong>
                          <span className={`admin-trend-status ${item.decision === "accepted" ? "fresh" : "stale"}`}>
                            {item.decision === "accepted" ? t("adminPairFitDecisionAccepted") : t("adminPairFitDecisionRejected")}
                          </span>
                        </div>
                        <div className="admin-trend-meta">
                          <span>{item.publishing_site_name || item.publishing_site_url}</span>
                          <span>{t("adminPairFitScoreLabel")}: {item.fit_score || 0}</span>
                          <span>{item.target_url}</span>
                          <span>{t("adminPairFitLatestRefresh")}: {item.updated_at ? formatPublishedAt(item.updated_at) : t("notAvailable")}</span>
                        </div>
                        {item.best_overlap_reason ? (
                          <p className="muted-text small-text admin-trend-detail">{item.best_overlap_reason}</p>
                        ) : null}
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div className="admin-trend-panel">
                <div className="admin-trend-header">
                  <div>
                    <h3>{t("adminAutoHostRecentTitle")}</h3>
                    <p className="muted-text">{t("adminAutoHostRecentBody")}</p>
                  </div>
                </div>
                {recentHostDecisions.length === 0 && !siteFitLoading ? (
                  <p className="muted-text">{t("adminAutoHostNoData")}</p>
                ) : (
                  <div className="admin-trend-list">
                    {recentHostDecisions.map((item) => (
                      <div key={item.job_id} className="admin-trend-item">
                        <div className="admin-trend-query-row">
                          <strong>{item.topic || item.target_url || item.publishing_site_name}</strong>
                          <span className={`admin-trend-status ${item.auto_selected ? "fresh" : "neutral"}`}>
                            {item.auto_selected ? t("adminAutoHostStatusAuto") : t("adminAutoHostStatusManual")}
                          </span>
                        </div>
                        <div className="admin-trend-meta">
                          <span>{item.client_name}</span>
                          <span>{item.publishing_site_name || item.publishing_site_url}</span>
                          <span>{t("adminPairFitScoreLabel")}: {item.fit_score || 0}</span>
                          <span>{item.created_at ? formatPublishedAt(item.created_at) : t("notAvailable")}</span>
                        </div>
                        <div className="admin-trend-meta">
                          <span>{item.target_url || t("notAvailable")}</span>
                          <span>{formatPublishedStatus(item.job_status)}</span>
                        </div>
                        {item.overlap_reason ? (
                          <p className="muted-text small-text admin-trend-detail">{item.overlap_reason}</p>
                        ) : null}
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {adminLoading ? (
                <div className="loading-inline" role="status" aria-live="polite">
                  <span className="sr-only">{t("loading")}</span>
                </div>
              ) : null}
            </div>
          ) : isClientDashboardSection ? (
            <ClientDashboardPanel
              t={t}
              clientName={resolvedClientName}
              siteCount={sites.length}
              targetSiteCount={clientTargetSitesCount}
              targetSitePreview={clientTargetSitePreview}
              readySitesLabel={readySitesLabel}
              suggestedSubmittedArticlesMonthly={suggestedSubmittedArticlesMonthly}
              suggestedCreatedArticlesMonthly={suggestedCreatedArticlesMonthly}
              weeklyCadenceText={weeklyCadenceText}
              siteMixPreview={siteMixPreview}
              uniqueDomainCount={uniqueSiteDomains.length}
              onOpenSubmitArticles={() => setActiveSection("submit-article")}
              onOpenCreateArticles={() => setActiveSection("create-article")}
            />
          ) : isWebsitesSection ? (
            <div className="panel form-panel">
              <h2>{t("navWebsites")}</h2>
              <div className="admin-entity-list">
                {sites.map((site, index) => (
                  <div key={site.id} className="admin-entity-card" style={{"--i": index}}>
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
                {clients.map((client, index) => (
                  <div key={client.id} className="admin-entity-card" style={{"--i": index}}>
                    <strong>{client.name}</strong>
                    <span className="muted-text">{client.email || client.phone_number || "-"}</span>
                  </div>
                ))}
              </div>
            </div>
          ) : isPublishedArticlesSection ? (
            <div className="panel form-panel published-panel">
              <div className="published-header">
                <h2>{t("publishedArticlesTitle")}</h2>
                {!publishedLoading && publishedTotal > 0 ? (
                  <span className="published-total-badge">{publishedTotal}</span>
                ) : null}
              </div>
              <div className="published-controls">
                <div className="published-field">
                  <label>{t("publishedSearchLabel")}</label>
                  <input
                    type="text"
                    value={publishedQuery}
                    onChange={(e) => setPublishedQuery(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        applyPublishedSearch();
                      }
                    }}
                    placeholder={t("publishedSearchPlaceholder")}
                  />
                </div>
                <div className="published-field">
                  <label>{t("publishedFilterClient")}</label>
                  <select
                    value={publishedClientId}
                    onChange={(e) => {
                      const next = e.target.value;
                      setPublishedClientId(next);
                      setPublishedOffset(0);
                      loadPublishedArticles(currentUser, { clientId: next, offset: 0 });
                    }}
                  >
                    <option value="">{t("publishedAllClients")}</option>
                    {clients.map((client) => (
                      <option key={client.id} value={client.id}>
                        {(client.name || "").trim() || client.id}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="published-field">
                  <label>{t("publishedFilterSite")}</label>
                  <select
                    value={publishedSiteId}
                    onChange={(e) => {
                      const next = e.target.value;
                      setPublishedSiteId(next);
                      setPublishedOffset(0);
                      loadPublishedArticles(currentUser, { siteId: next, offset: 0 });
                    }}
                  >
                    <option value="">{t("publishedAllSites")}</option>
                    {sites.map((site) => (
                      <option key={site.id} value={site.id}>
                        {(site.site_url || "").trim() || (site.name || "").trim() || site.id}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="published-field">
                  <label>{t("publishedSortLabel")}</label>
                  <select
                    value={publishedSort}
                    onChange={(e) => {
                      const next = e.target.value;
                      setPublishedSort(next);
                      setPublishedOffset(0);
                      loadPublishedArticles(currentUser, { sort: next, offset: 0 });
                    }}
                  >
                    <option value="published_at">{t("publishedSortPublishedAt")}</option>
                    <option value="url">{t("publishedSortUrl")}</option>
                  </select>
                </div>
                <div className="published-field">
                  <label>{t("publishedPageSizeLabel")}</label>
                  <select
                    value={publishedLimit}
                    onChange={(e) => {
                      const next = Number(e.target.value) || PUBLISHED_PAGE_SIZE;
                      setPublishedLimit(next);
                      setPublishedOffset(0);
                      loadPublishedArticles(currentUser, { limit: next, offset: 0 });
                    }}
                  >
                    {PUBLISHED_PAGE_SIZES.map((size) => (
                      <option key={size} value={size}>
                        {size}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="published-actions">
                  <button className="btn secondary" type="button" onClick={applyPublishedSearch} disabled={publishedLoading}>
                    {t("publishedSearchButton")}
                  </button>
                  <button className="btn" type="button" onClick={resetPublishedFilters} disabled={publishedLoading}>
                    {t("publishedClearFilters")}
                  </button>
                </div>
              </div>
              {publishedLoading ? (
                <div className="loading-inline" role="status" aria-live="polite">
                  <span className="sr-only">{t("loading")}</span>
                </div>
              ) : null}
              {!publishedLoading && publishedArticles.length === 0 ? (
                <p className="muted-text">{t("publishedArticlesEmpty")}</p>
              ) : null}
              <div className="published-list-table">
                <div className="published-list-header">
                  <span>{t("publishedUrlLabel")}</span>
                  <span>{t("publishedClientLabel")}</span>
                  <span>{t("publishedSiteLabel")}</span>
                  <span>{t("publishedStatusLabel")}</span>
                  <span>{t("seoScoreLabel")}</span>
                  <span>{t("seoEvaluationLabel")}</span>
                  <span>{t("publishedByLabel")}</span>
                  <span>{t("publishedAtLabel")}</span>
                </div>
                {publishedArticles.map((item, index) => {
                  const url = (item?.wp_post_url || "").trim();
                  const publishedBy = (item?.published_by || "").trim() || t("publishedBySystem");
                  const clientName = (item?.client_name || "").trim() || item?.client_id || t("notAvailable");
                  const siteLabel = (item?.site_url || "").trim() || (item?.site_name || "").trim() || item?.site_id || t("notAvailable");
                  const statusLabel = formatPublishedStatus(item?.status);
                  const seoSummary = summarizeSeoEvaluation(item?.seo_evaluation);
                  return (
                    <div key={item.job_id} className="published-item-row" style={{"--i": index}}>
                      {url ? (
                        <a className="published-link" href={url} target="_blank" rel="noreferrer" data-label={t("publishedUrlLabel")}>
                          {url}
                        </a>
                      ) : (
                        <span className="muted-text" data-label={t("publishedUrlLabel")}>{t("notAvailable")}</span>
                      )}
                      <span data-label={t("publishedClientLabel")}>{clientName}</span>
                      <span data-label={t("publishedSiteLabel")}>{siteLabel}</span>
                      <span data-label={t("publishedStatusLabel")}>{statusLabel}</span>
                      <span data-label={t("seoScoreLabel")}>{item?.seo_score ?? t("notAvailable")}</span>
                      <div className="seo-evaluation-cell" data-label={t("seoEvaluationLabel")}>
                        <details className="seo-evaluation-details">
                          <summary>
                            <span className="seo-evaluation-summary-title">{t("seoEvaluationView")}</span>
                            <span className="seo-evaluation-summary-meta">
                              {seoSummary.issueCount > 0
                                ? `${seoSummary.failingChecks.length} ${t("seoEvaluationFailing")} • ${seoSummary.issueCount} ${t("seoEvaluationIssues")}`
                                : t("seoEvaluationNoIssues")}
                            </span>
                          </summary>
                          <div className="seo-evaluation-content">
                            <div className="seo-evaluation-stats">
                              <span>{t("seoScoreLabel")}: {seoSummary.score ?? t("notAvailable")}</span>
                              <span>{seoSummary.passedCount} {t("seoEvaluationPassing")}</span>
                            </div>
                            {seoSummary.topIssues.length > 0 ? (
                              <ul className="seo-evaluation-issues">
                                {seoSummary.topIssues.map((issue, issueIndex) => (
                                  <li key={`${item.job_id}-seo-issue-${issueIndex}`}>{issue}</li>
                                ))}
                              </ul>
                            ) : (
                              <p className="seo-evaluation-empty">{t("seoEvaluationNoIssues")}</p>
                            )}
                          </div>
                        </details>
                      </div>
                      <span data-label={t("publishedByLabel")}>{publishedBy}</span>
                      <span data-label={t("publishedAtLabel")}>{formatPublishedAt(item?.published_at)}</span>
                    </div>
                  );
                })}
              </div>
              <div className="published-pagination">
                <span className="muted-text">
                  {t("publishedShowingLabel")} {publishedFrom}-{publishedTo} {t("publishedOfLabel")} {publishedTotal}
                </span>
                <div className="pagination-actions">
                  <button
                    className="btn secondary"
                    type="button"
                    onClick={() => goToPublishedOffset(publishedOffset - publishedLimit)}
                    disabled={!publishedCanPrev || publishedLoading}
                  >
                    {t("publishedPrevious")}
                  </button>
                  <span className="muted-text">
                    {t("publishedPageLabel")} {publishedPage} {t("publishedOfLabel")} {publishedPageCount}
                  </span>
                  <button
                    className="btn secondary"
                    type="button"
                    onClick={() => goToPublishedOffset(publishedOffset + publishedLimit)}
                    disabled={!publishedCanNext || publishedLoading}
                  >
                    {t("publishedNext")}
                  </button>
                </div>
              </div>
            </div>
          ) : isAdminPendingSection ? (
            <div className="panel form-panel pending-panel">
              <div className="pending-header">
                <h2>{t("navPendingJobs")}</h2>
                {!pendingLoading && pendingJobs.length > 0 ? (
                  <span className="pending-count">{pendingJobs.length}</span>
                ) : null}
              </div>
              {pendingLoading ? (
                <div className="loading-inline" role="status" aria-live="polite">
                  <span className="sr-only">{t("loading")}</span>
                </div>
              ) : null}
              {!pendingLoading && pendingJobs.length === 0 ? (
                <p className="muted-text">
                  {t("pendingJobsEmpty")}
                </p>
              ) : null}
              <div className="pending-list-table">
                <div className="pending-list-header">
                  <span>{t("createdByLabel")}</span>
                  <span>{t("publishingSiteLabel")}</span>
                  <span>{t("targetSiteLabel")}</span>
                  <span>{t("contentTitleLabel")}</span>
                  <span>{t("jobTypeLabel")}</span>
                  <span>{t("createdAtLabel")}</span>
                  <span>{t("actionsLabel")}</span>
                </div>
                {pendingJobs.map((item, index) => {
                  const draftReviewUrl = getDraftReviewUrl(item);
                  const requestKind = item.request_kind === "create_article" ? "create_article" : "submit_article";
                  return (
                    <div key={item.job_id} className="pending-item-wrap" style={{"--i": index}}>
                      <div className="pending-item-row">
                        <span data-label={t("createdByLabel")}>{item.client_name}</span>
                        <span data-label={t("publishingSiteLabel")}>{item.site_url || item.site_name}</span>
                        <span data-label={t("targetSiteLabel")}>{item.target_site_url || t("contentTitleFallback")}</span>
                        <span data-label={t("contentTitleLabel")}>{item.content_title || t("contentTitleFallback")}</span>
                        <span data-label={t("jobTypeLabel")}>{requestKind === "create_article" ? t("jobTypeCreatedArticle") : t("jobTypeSubmittedArticle")}</span>
                        <span data-label={t("createdAtLabel")}>{formatPublishedAt(item?.created_at)}</span>
                        <div className="pending-actions">
                          {draftReviewUrl ? (
                            <a className="btn secondary" href={draftReviewUrl} target="_blank" rel="noreferrer">
                              {t("viewDraft")}
                            </a>
                          ) : (
                            <button className="btn secondary" type="button" disabled>
                              {t("viewDraft")}
                            </button>
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
          ) : isQueueDashboardSection ? (
            <QueueDashboardPanel
              t={t}
              queueStats={queueStats}
              queueStatsLoading={queueStatsLoading}
              queueAutoRefresh={queueAutoRefresh}
              onToggleAutoRefresh={() => {
                setQueueAutoRefresh((prev) => {
                  const next = !prev;
                  queueAutoRefreshRef.current = next;
                  return next;
                });
              }}
              onRefresh={async () => {
                setQueueStatsLoading(true);
                try {
                  const data = await api.get("/queue/stats");
                  setQueueStats(data);
                } catch (err) {
                  console.error("Failed to load queue stats", err);
                } finally {
                  setQueueStatsLoading(false);
                }
              }}
            />
          ) : (
            <div className="panel form-panel request-form-panel">
              <div className="submit-article-form request-builder-form">
                <div className="submission-blocks" ref={submissionBlocksRef}>
                  {(isCreateArticleSection ? createArticleSubmissionBlocks : submitArticleSubmissionBlocks).map((block, blockIndex) => {
                    const selectedClient = isAdminUser
                      ? clients.find((client) => (client.name || "").trim() === (block.client_name || "").trim())
                      : null;
                    const availableTargetSites = sortByLabel(
                      isAdminUser ? getTargetSitesForClient(selectedClient) : clientTargetSites,
                      (row) => `${row.target_site_domain || ""} ${row.target_site_url || ""}`,
                    );
                    const showRemoveControl = blockIndex > 0;
                    const activeBlocks = isCreateArticleSection ? createArticleSubmissionBlocks : submitArticleSubmissionBlocks;
                    const isBatchMode = activeBlocks.length > 1;
                    const blockStatus = batchBlockStatus[block.id] || null;
                    const isSuggestionsOpen = activeSuggestion && activeSuggestion.blockId === block.id;
                    const creatorJobIds = creatorJobsByBlock[block.id] || [];
                    return (
                      <div
                        key={block.id}
                        className={`submission-block-wrap ${isSuggestionsOpen ? "suggestions-open" : ""}`.trim()}
                      >
                        <div className={`submission-block panel ${isCreateArticleSection ? "create-article-block" : ""} ${blockStatus ? `batch-${blockStatus}` : ""}`.trim()}>
                          <div className="submission-block-header">
                            <h3>{`${t("requestBlockLabel")} ${blockIndex + 1}`}</h3>
                            {blockStatus ? (
                              <span className={`batch-status-indicator batch-status-${blockStatus}`}>
                                {blockStatus === "submitting" ? "⏳" : blockStatus === "success" ? "✓" : "✗"}
                              </span>
                            ) : null}
                          </div>

                          {isAdminUser ? (
                            <div className="submission-field submission-field-inline submission-field-client">
                              <label>{t("clientName")}</label>
                              <div className="site-suggest-wrap">
                                <input
                                  type="text"
                                  value={block.client_name || ""}
                                  ref={(node) => {
                                    if (node) {
                                      clientSuggestInputRefs.current[block.id] = node;
                                    }
                                  }}
                                  onFocus={() => {
                                    clearSubmissionFieldError(block.id, "client_name");
                                    setClientSuggestionsBlockId(block.id);
                                    setSiteSuggestionsBlockId(null);
                                    setTargetSiteSuggestionsBlockId(null);
                                  }}
                                  onBlur={() => setTimeout(() => {
                                    setClientSuggestionsBlockId((prev) => (prev === block.id ? null : prev));
                                  }, 120)}
                                  onChange={(e) => {
                                    const nextClientName = e.target.value;
                                    updateActiveSubmissionBlocks((prev) => prev.map((item) => (
                                      item.id === block.id
                                        ? {
                                            ...item,
                                            client_name: nextClientName,
                                            target_site_id: "",
                                            target_site_url: "",
                                          }
                                        : item
                                    )));
                                    clearSubmissionFieldError(block.id, "client_name");
                                    setClientSuggestionsBlockId(block.id);
                                    setSiteSuggestionsBlockId(null);
                                    setTargetSiteSuggestionsBlockId(null);
                                  }}
                                  placeholder={t("selectClient")}
                                  required
                                />
                                {submissionFieldErrors[block.id]?.client_name ? (
                                  <div className="file-type-tooltip" role="alert">
                                    <span className="file-type-tooltip-icon">!</span>
                                    <span>{t("errorClientRequired")}</span>
                                  </div>
                                ) : null}
                              </div>
                            </div>
                          ) : null}

                          {isCreateArticleSection ? (
                            <div className="submission-field submission-field-site submission-field-target-site">
                              <label>{t("targetSiteForBacklink")}</label>
                              <div className="site-suggest-wrap">
                                <input
                                  type="url"
                                  value={block.target_site_url || ""}
                                  ref={(node) => {
                                    if (node) {
                                      targetSiteSuggestInputRefs.current[block.id] = node;
                                    }
                                  }}
                                  onFocus={() => {
                                    clearSubmissionFieldError(block.id, "target_site");
                                    setTargetSiteSuggestionsBlockId(block.id);
                                    setClientSuggestionsBlockId(null);
                                    setSiteSuggestionsBlockId(null);
                                  }}
                                  onBlur={() => setTimeout(() => {
                                    setTargetSiteSuggestionsBlockId((prev) => (prev === block.id ? null : prev));
                                  }, 120)}
                                  onChange={(e) => {
                                    const nextUrl = e.target.value;
                                    const nextTarget = availableTargetSites.find((row) => {
                                      const urlValue = (row.target_site_url || "").trim();
                                      const domainValue = (row.target_site_domain || "").trim();
                                      const domainUrlValue = domainValue ? `https://${domainValue}` : "";
                                      return urlValue === nextUrl || domainUrlValue === nextUrl;
                                    });
                                    const nextId = nextTarget ? String(nextTarget.id || "") : "";
                                    updateActiveSubmissionBlocks((prev) => prev.map((item) => (
                                      item.id === block.id
                                        ? {
                                            ...item,
                                            target_site_id: nextId,
                                            target_site_url: nextUrl,
                                          }
                                        : item
                                    )));
                                    clearSubmissionFieldError(block.id, "target_site");
                                    setTargetSiteSuggestionsBlockId(block.id);
                                    setClientSuggestionsBlockId(null);
                                    setSiteSuggestionsBlockId(null);
                                  }}
                                  placeholder={t("placeholderTargetWebsite")}
                                  required
                                />
                                {submissionFieldErrors[block.id]?.target_site ? (
                                  <div className="file-type-tooltip" role="alert">
                                    <span className="file-type-tooltip-icon">!</span>
                                    <span>{t("errorClientTargetSiteRequired")}</span>
                                  </div>
                                ) : null}
                              </div>
                            </div>
                          ) : null}

                          <div className={`submission-field submission-field-site ${isCreateArticleSection ? "submission-field-inline" : ""}`.trim()}>
                            <label>
                              {t("targetWebsite")}
                              {isCreateArticleSection ? ` (${t("optional")})` : ""}
                            </label>
                            <div className="site-suggest-wrap">
                              <input
                                value={block.publishing_site}
                                ref={(node) => {
                                  if (node) {
                                    siteSuggestInputRefs.current[block.id] = node;
                                  }
                                }}
                                onFocus={() => {
                                  clearSubmissionFieldError(block.id, "publishing_site");
                                  setSiteSuggestionsBlockId(block.id);
                                  setClientSuggestionsBlockId(null);
                                  setTargetSiteSuggestionsBlockId(null);
                                }}
                                onBlur={() => setTimeout(() => {
                                  setSiteSuggestionsBlockId((prev) => (prev === block.id ? null : prev));
                                }, 120)}
                                onChange={(e) => {
                                  setSubmissionBlockField(block.id, "publishing_site", e.target.value);
                                  clearSubmissionFieldError(block.id, "publishing_site");
                                  setSiteSuggestionsBlockId(block.id);
                                  setClientSuggestionsBlockId(null);
                                  setTargetSiteSuggestionsBlockId(null);
                                }}
                                placeholder={t("placeholderTargetWebsite")}
                                required={!isCreateArticleSection}
                              />
                              {submissionFieldErrors[block.id]?.publishing_site ? (
                                <div className="file-type-tooltip" role="alert">
                                  <span className="file-type-tooltip-icon">!</span>
                                  <span>{t("errorTargetRequired")}</span>
                                </div>
                              ) : null}
                            </div>
                          </div>

                          {!isCreateArticleSection ? (
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

                          {!isCreateArticleSection ? (
                            <div
                              className={`submission-field submission-field-wide submission-field-file ${
                                block.source_type === "google-doc" || block.source_type === "word-doc" ? "" : "is-empty"
                              }`.trim()}
                            >
                              {block.source_type === "google-doc" ? (
                                <>
                                  <label>{t("googleDocLink")}</label>
                                  <input
                                    type="url"
                                    value={block.doc_url}
                                    onChange={(e) => setSubmissionBlockField(block.id, "doc_url", e.target.value)}
                                    placeholder={t("placeholderGoogleDoc")}
                                    required
                                  />
                                </>
                              ) : block.source_type === "word-doc" ? (
                                <>
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
                                </>
                              ) : null}
                            </div>
                          ) : null}

                          {isCreateArticleSection ? (
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

                          {!isBatchMode ? (
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
                          ) : null}

                          {isCreateArticleSection && creatorJobIds.length > 0 ? (
                            <CreatorProgressInline
                              jobIds={creatorJobIds}
                              progress={creatorProgress}
                              t={t}
                            />
                          ) : null}
                        </div>
                      </div>
                    );
                  })}
                  {activeSuggestion && suggestionStyle ? (
                    <div className="suggestion-layer">
                      <div className="site-suggest-list" style={suggestionStyle}>
                        {renderSuggestionItems(activeSuggestion)}
                      </div>
                    </div>
                  ) : null}
                </div>
                <div className="submission-block-controls submission-block-controls-global">
                  <button
                    className="btn block-control-btn"
                    type="button"
                    aria-label={t("addAnotherBlock")}
                    onClick={() => {
                      const currentBlocks = isCreateArticleSection ? createArticleSubmissionBlocks : submitArticleSubmissionBlocks;
                      addSubmissionBlock(currentBlocks[currentBlocks.length - 1]?.id);
                    }}
                    disabled={submitting}
                  >
                    +
                  </button>
                  {(isCreateArticleSection ? createArticleSubmissionBlocks : submitArticleSubmissionBlocks).length > 1 ? (
                    <button
                      className="btn secondary block-control-btn block-control-remove"
                      type="button"
                      aria-label={t("removeBlock")}
                      onClick={() => {
                        const currentBlocks = isCreateArticleSection ? createArticleSubmissionBlocks : submitArticleSubmissionBlocks;
                        removeSubmissionBlock(currentBlocks[currentBlocks.length - 1]?.id);
                      }}
                      disabled={submitting}
                    >
                      -
                    </button>
                  ) : null}
                </div>
                {(isCreateArticleSection ? createArticleSubmissionBlocks : submitArticleSubmissionBlocks).length > 1 ? (
                  <div className="batch-submit-bar">
                    <button
                      className="btn submit-btn batch-submit-btn"
                      type="button"
                      onClick={submitAllBlocks}
                      disabled={submitting}
                    >
                      {submitting
                        ? t("submittingAll")
                        : `${t("submitAllArticles")} (${(isCreateArticleSection ? createArticleSubmissionBlocks : submitArticleSubmissionBlocks).length})`}
                    </button>
                  </div>
                ) : null}
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

function CreatorProgressInline({
  jobIds,
  progress,
  t,
}) {
  const translate = typeof t === "function" ? t : (key) => key;
  const firstId = jobIds[0];
  const info = progress[firstId] || { phase: 0, percent: 0, done: false, failed: false };
  const allDone = jobIds.length > 0 && jobIds.every((jid) => progress[jid]?.done);
  const aggPercent = jobIds.length > 0
    ? Math.round(jobIds.reduce((sum, jid) => sum + (progress[jid]?.percent || 0), 0) / jobIds.length)
    : 0;
  const currentPhase = info.phase || 0;

  return (
    <div className="creator-progress-inline" role="status" aria-live="polite" aria-label="Creator progress">
      <div className="progress-steps progress-steps-inline" aria-hidden="true">
        {Array.from({ length: CREATOR_TOTAL_PHASES }, (_, i) => {
          const step = i + 1;
          const isCompleted = allDone || step < currentPhase;
          const isActive = !allDone && step === currentPhase;
          const cls = isCompleted ? "completed" : isActive ? "active" : "";
          return (
            <div key={step} className={`progress-step progress-step-inline ${cls}`}>
              <div className="progress-step-indicator progress-step-indicator-inline">
                <div className="progress-step-dot" />
                {step < CREATOR_TOTAL_PHASES && <div className="progress-step-line progress-step-line-inline" />}
              </div>
              <div className="progress-step-content progress-step-content-inline">
                <span className="progress-step-label">{translate(CREATOR_PHASE_LABELS[step])}</span>
              </div>
            </div>
          );
        })}
      </div>
      <div className={`creator-progress-inline-end ${allDone ? "is-complete" : ""}`.trim()} aria-live="polite">
        {allDone ? <span className="creator-progress-inline-check">✓</span> : <strong>{aggPercent}%</strong>}
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
  suggestedSubmittedArticlesMonthly,
  suggestedCreatedArticlesMonthly,
  weeklyCadenceText,
  siteMixPreview,
  uniqueDomainCount,
  onOpenSubmitArticles,
  onOpenCreateArticles,
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
              <span className="stat-label">{t("navSubmitArticle")}</span>
              <strong>{suggestedSubmittedArticlesMonthly}</strong>
            </div>
            <div>
              <span className="stat-label">{t("navCreateArticle")}</span>
              <strong>{suggestedCreatedArticlesMonthly}</strong>
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
            <button className="btn" type="button" onClick={onOpenSubmitArticles}>
              {t("clientDashboardSubmitArticlesCta")}
            </button>
            <button className="btn secondary" type="button" onClick={onOpenCreateArticles}>
              {t("clientDashboardCreateArticlesCta")}
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
  const sectionIcons = {
    admin: (
      <svg viewBox="0 0 24 24" role="img" focusable="false">
        <path
          d="M12 3.5l7 3.2v5.1c0 4.3-2.8 8.1-7 9.7-4.2-1.6-7-5.4-7-9.7V6.7l7-3.2z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
        />
        <path d="M12 7.5v6.2l3.2 1.8" fill="none" stroke="currentColor" strokeWidth="1.6" />
      </svg>
    ),
    websites: (
      <svg viewBox="0 0 24 24" role="img" focusable="false">
        <circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" strokeWidth="1.6" />
        <path d="M3 12h18M12 3c3 3.2 3 14.8 0 18M12 3c-3 3.2-3 14.8 0 18" fill="none" stroke="currentColor" strokeWidth="1.6" />
      </svg>
    ),
    clients: (
      <svg viewBox="0 0 24 24" role="img" focusable="false">
        <path
          d="M8 12a3 3 0 1 0 0-6 3 3 0 0 0 0 6zm8 1.5a2.6 2.6 0 1 0 0-5.2 2.6 2.6 0 0 0 0 5.2z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
        />
        <path
          d="M3.5 19c.6-3 3-4.7 6.1-4.7 2.8 0 5.2 1.4 6.2 4.2M12.8 18.2c.6-1.8 2.3-2.9 4.5-2.9 1.5 0 2.9.5 3.7 1.6"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
      </svg>
    ),
    "submit-article": (
      <svg viewBox="0 0 24 24" role="img" focusable="false">
        <path d="M4 12h10.5" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
        <path d="M12 6l6 6-6 6" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
        <path d="M4 7v10" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      </svg>
    ),
    "create-article": (
      <svg viewBox="0 0 24 24" role="img" focusable="false">
        <path
          d="M4 17.5V20h2.5l9-9-2.5-2.5-9 9zM14.5 7.5l2.5 2.5 1.8-1.8a1.8 1.8 0 0 0 0-2.5l-.9-.9a1.8 1.8 0 0 0-2.5 0l-1.4 1.2z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinejoin="round"
        />
      </svg>
    ),
    "pending-jobs": (
      <svg viewBox="0 0 24 24" role="img" focusable="false">
        <circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" strokeWidth="1.6" />
        <path d="M12 7v5l3 2" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      </svg>
    ),
    "published-articles": (
      <svg viewBox="0 0 24 24" role="img" focusable="false">
        <path d="M7 4h10a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H7l-3-3V6a2 2 0 0 1 2-2z" fill="none" stroke="currentColor" strokeWidth="1.6" />
        <path d="M8 9h8M8 13h5" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      </svg>
    ),
    "queue-dashboard": (
      <svg viewBox="0 0 24 24" role="img" focusable="false">
        <path d="M4 6h16M4 10h12M4 14h14M4 18h10" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
        <circle cx="20" cy="18" r="2.5" fill="none" stroke="currentColor" strokeWidth="1.4" />
        <path d="M20 16v2l1.2.7" fill="none" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
      </svg>
    ),
    dashboard: (
      <svg viewBox="0 0 24 24" role="img" focusable="false">
        <path d="M4 4h7v7H4zM13 4h7v4h-7zM13 10h7v10h-7zM4 13h7v7H4z" fill="none" stroke="currentColor" strokeWidth="1.6" />
      </svg>
    ),
  };

  const sections = userRole === "admin"
    ? [
        { id: "admin", label: t("navAdmin") },
        { id: "websites", label: t("navWebsites") },
        { id: "clients", label: t("navClients") },
        { id: "submit-article", label: t("navSubmitArticle") },
        { id: "create-article", label: t("navCreateArticle") },
        { id: "pending-jobs", label: t("navPendingJobs"), badge: pendingJobsCount },
        { id: "published-articles", label: t("navPublishedArticles") },
        { id: "queue-dashboard", label: t("navQueueDashboard") },
      ]
    : [
        { id: "dashboard", label: t("navClientDashboard") },
        { id: "submit-article", label: t("navSubmitArticle") },
        { id: "create-article", label: t("navCreateArticle") },
      ];

  return (
    <aside className="sidebar">
      <nav className="nav">
        {sections.map((section) => (
          <button
            key={section.id}
            type="button"
            className={`nav-item ${activeSection === section.id ? "active" : ""}`}
            onClick={() => onSectionChange(section.id)}
          >
            <span className="nav-item-content">
              <span className="nav-icon" aria-hidden="true">
                {sectionIcons[section.id]}
              </span>
              <span className="nav-label">{section.label}</span>
            </span>
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
    <div className="inline">
      <span className="icon" aria-hidden="true">
        <SunIcon />
      </span>
      <div className="toggle theme-toggle">
        <button
          type="button"
          className={theme === "light" ? "active" : ""}
          onClick={() => onChange("light")}
          aria-label={t("lightTheme")}
          title={t("lightTheme")}
        >
          <SunIcon />
        </button>
        <button
          type="button"
          className={theme === "dark" ? "active" : ""}
          onClick={() => onChange("dark")}
          aria-label={t("darkTheme")}
          title={t("darkTheme")}
        >
          <MoonIcon />
        </button>
      </div>
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

function QueueDashboardPanel({ t, queueStats, queueStatsLoading, queueAutoRefresh, onToggleAutoRefresh, onRefresh }) {
  const data = queueStats || {};
  const workerRunning = data.worker_running ?? false;
  const hasData = queueStats !== null;
  const startedAtFormatted = data.started_at
    ? new Date(data.started_at).toLocaleString()
    : "—";

  return (
    <div className="panel form-panel queue-dashboard-panel">
      <div className="queue-dashboard-header">
        <h2>{t("queueDashboardTitle")}</h2>
        <div className="queue-dashboard-actions">
          <label className="queue-auto-toggle">
            <input
              type="checkbox"
              checked={queueAutoRefresh}
              onChange={onToggleAutoRefresh}
            />
            <span>{t("queueAutoRefresh")}</span>
          </label>
          <button className="btn secondary small" type="button" onClick={onRefresh} disabled={queueStatsLoading}>
            {queueStatsLoading ? t("loading") : t("queueRefresh")}
          </button>
        </div>
      </div>

      {!hasData && !queueStatsLoading ? (
        <p className="muted-text">{t("queueNoData")}</p>
      ) : null}

      {queueStatsLoading && !hasData ? (
        <div className="loading-inline" role="status" aria-live="polite">
          <span className="sr-only">{t("loading")}</span>
        </div>
      ) : null}

      {hasData ? (
        <>
          <div className="queue-status-badge-row">
            <span className={`queue-status-badge ${workerRunning ? "queue-status-running" : "queue-status-stopped"}`}>
              <span className="queue-status-dot" />
              {workerRunning ? t("queueWorkerRunning") : t("queueWorkerStopped")}
            </span>
            {data.started_at ? (
              <span className="queue-started-at">
                {t("queueStartedAt")}: {startedAtFormatted}
              </span>
            ) : null}
          </div>

          <div className="stats-grid queue-stats-grid">
            <div className="stat-card" style={{"--i": 0}}>
              <span className="stat-label">{t("queueConcurrency")}</span>
              <strong>{data.concurrency ?? "—"}</strong>
            </div>
            <div className="stat-card queue-stat-active" style={{"--i": 1}}>
              <span className="stat-label">{t("queueActiveJobs")}</span>
              <strong>{data.active_jobs ?? 0}</strong>
            </div>
            <div className="stat-card queue-stat-queued" style={{"--i": 2}}>
              <span className="stat-label">{t("queueQueuedJobs")}</span>
              <strong>{data.queued_jobs ?? 0}</strong>
            </div>
            <div className="stat-card" style={{"--i": 3}}>
              <span className="stat-label">{t("queueTotalProcessed")}</span>
              <strong>{data.total_processed ?? 0}</strong>
            </div>
            <div className="stat-card queue-stat-succeeded" style={{"--i": 4}}>
              <span className="stat-label">{t("queueTotalSucceeded")}</span>
              <strong>{data.total_succeeded ?? 0}</strong>
            </div>
            <div className="stat-card queue-stat-failed" style={{"--i": 5}}>
              <span className="stat-label">{t("queueTotalFailed")}</span>
              <strong>{data.total_failed ?? 0}</strong>
            </div>
          </div>

          {Array.isArray(data.active_job_ids) && data.active_job_ids.length > 0 ? (
            <div className="queue-active-ids">
              <h3>{t("queueActiveJobIds")}</h3>
              <ul className="queue-id-list">
                {data.active_job_ids.map((id) => (
                  <li key={id}><code>{id}</code></li>
                ))}
              </ul>
            </div>
          ) : null}
        </>
      ) : null}
    </div>
  );
}
