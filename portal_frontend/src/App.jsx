import { useEffect, useMemo, useState } from "react";
import { api } from "./api.js";
import { getLabel } from "./i18n.js";

const getInitialLanguage = () => localStorage.getItem("ui_language") || "en";

const emptySubmissionForm = () => ({
  target_site: "",
  client_name: "",
  source_type: "",
  doc_url: "",
  docx_file: null,
  anchor: "",
  topic: "",
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
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

  const [clients, setClients] = useState([]);
  const [sites, setSites] = useState([]);
  const [submissionForm, setSubmissionForm] = useState(emptySubmissionForm());
  const [adminUsers, setAdminUsers] = useState([]);
  const [adminUserForm, setAdminUserForm] = useState(emptyAdminUserForm());
  const [adminUserEdits, setAdminUserEdits] = useState({});
  const [adminLoading, setAdminLoading] = useState(false);
  const [adminSubmitting, setAdminSubmitting] = useState(false);
  const [adminSavingUserId, setAdminSavingUserId] = useState("");

  const t = useMemo(() => (key) => getLabel(language, key), [language]);

  useEffect(() => {
    if (theme === "system") {
      document.documentElement.removeAttribute("data-theme");
    } else {
      document.documentElement.setAttribute("data-theme", theme);
    }
    localStorage.setItem("theme", theme);
  }, [theme]);

  const loadAll = async () => {
    try {
      setError("");
      const [clientsData, sitesData] = await Promise.all([api.get("/clients"), api.get("/sites")]);
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
        setLoading(true);
        await loadAll();
        if (user.role === "admin") {
          await loadAdminUsers(user);
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
    if (currentUser.role === "admin") {
      if (
        activeSection !== "admin"
        && activeSection !== "websites"
        && activeSection !== "clients"
        && activeSection !== "guest-posts"
        && activeSection !== "orders"
      ) {
        setActiveSection("admin");
      }
      return;
    }
    const allowedSections = ["guest-posts", "orders"];
    if (!allowedSections.includes(activeSection)) {
      setActiveSection("guest-posts");
    }
  }, [currentUser, activeSection]);

  useEffect(() => {
    if (!currentUser) return;
    if (currentUser.role === "admin") {
      setActiveSection("admin");
    }
  }, [currentUser]);

  useEffect(() => {
    if (!currentUser || currentUser.role !== "admin") return;
    if (activeSection !== "admin") return;
    if (adminUsers.length > 0) return;
    loadAdminUsers(currentUser);
  }, [currentUser, activeSection]);

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
      setActiveSection(user.role === "admin" ? "admin" : "guest-posts");
      setLoading(true);
      await loadAll();
      if (user.role === "admin") {
        await loadAdminUsers(user);
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
    setClients([]);
    setSites([]);
    setAdminUsers([]);
    setAdminUserEdits({});
    setAdminUserForm(emptyAdminUserForm());
    setError("");
    setSuccess("");
    setSubmissionForm(emptySubmissionForm());
  };

  const submitGuestPost = async (event) => {
    event.preventDefault();
    setError("");
    setSuccess("");

    if (activeSection === "admin") {
      setError(t("errorSelectClientSection"));
      return;
    }

    if (activeSection === "orders") {
      setError(t("ordersNotConnected"));
      return;
    }

    const effectiveSourceType = submissionForm.source_type;
    const targetSite = submissionForm.target_site.trim();
    const clientName = submissionForm.client_name.trim();

    if (!targetSite) {
      setError(t("errorTargetRequired"));
      return;
    }
    if (!clientName) {
      setError(t("errorClientRequired"));
      return;
    }
    if (!effectiveSourceType) {
      setError(t("errorFileTypeRequired"));
      return;
    }

    if (effectiveSourceType === "google-doc" && !submissionForm.doc_url.trim()) {
      setError(t("errorGoogleDocRequired"));
      return;
    }

    if (effectiveSourceType === "word-doc" && !submissionForm.docx_file) {
      setError(t("errorDocxRequired"));
      return;
    }

    const formData = new FormData();
    formData.append("target_site", targetSite);
    formData.append("client_name", clientName);
    formData.append("source_type", effectiveSourceType);
    formData.append("execution_mode", "async");
    if (submissionForm.anchor.trim()) formData.append("anchor", submissionForm.anchor.trim());
    if (submissionForm.topic.trim()) formData.append("topic", submissionForm.topic.trim());

    if (effectiveSourceType === "google-doc") {
      formData.append("doc_url", submissionForm.doc_url.trim());
    } else if (submissionForm.docx_file) {
      formData.append("docx_file", submissionForm.docx_file);
    }

    try {
      setSubmitting(true);
      const response = await fetch(`${baseApiUrl}/automation/guest-post-webhook`, {
        method: "POST",
        credentials: "include",
        body: formData,
      });

      if (!response.ok) {
        const detail = await readApiError(response, t("errorRequestFailed"));
        throw new Error(detail);
      }

      const payload = await response.json().catch(() => ({}));
      const jobId = payload?.job_id || payload?.result?.job_id;
      setSuccess(jobId ? t("successSubmittedWithJob").replace("{{jobId}}", jobId) : t("successSubmitted"));
      setSubmissionForm((prev) => ({
        ...emptySubmissionForm(),
        target_site: prev.target_site,
        client_name: prev.client_name,
      }));
    } catch (err) {
      setError(err.message);
    } finally {
      setSubmitting(false);
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
  const isOrders = activeSection === "orders";
  const adminCount = adminUsers.filter((item) => item.role === "admin").length;
  const clientUserCount = adminUsers.filter((item) => item.role === "client").length;
  const inactiveUserCount = adminUsers.filter((item) => !item.is_active).length;
  const mappedClientUserCount = adminUsers.filter((item) => item.role === "client" && (item.client_ids || []).length > 0).length;
  const unmappedClientUserCount = adminUsers.filter((item) => item.role === "client" && (item.client_ids || []).length === 0).length;
  const activeCoveragePercent = clients.length
    ? Math.round((mappedClientUserCount / Math.max(clientUserCount, 1)) * 100)
    : 0;

  return (
    <div className="app-shell">
      <Sidebar
        t={t}
        userRole={currentUser.role}
        activeSection={activeSection}
        onSectionChange={setActiveSection}
      />

      <div className="app-main">
        <div className="header">
          <div className="title">{isAdminSection ? t("heroAdminPanel") : t("clientsPortal")}</div>
          <div className="inline header-actions">
            <div className="user-chip">
              <span>{currentUser.email}</span>
              <span className="role-pill">{currentUser.role === "admin" ? t("roleAdmin") : t("roleClient")}</span>
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

        <div className="container">
          {!isAdminSection && !isWebsitesSection && !isClientsSection ? (
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
          ) : !isWebsitesSection && !isClientsSection ? (
            <div className="stats-grid">
              <div className="stat-card">
                <span className="stat-label">{t("statActiveSites")}</span>
                <strong>{sites.length}</strong>
              </div>
              <div className="stat-card">
                <span className="stat-label">{t("statActiveClients")}</span>
                <strong>{clients.length}</strong>
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
          ) : (
            <div className="panel form-panel">
              <h2>{isOrders ? t("formOrder") : t("formSubmission")}</h2>
              <form className="guest-form" onSubmit={submitGuestPost}>
                <div>
                  <label>{t("targetWebsite")}</label>
                  <input
                    list="target-site-options"
                    value={submissionForm.target_site}
                    onChange={(e) => setSubmissionForm((prev) => ({ ...prev, target_site: e.target.value }))}
                    placeholder={t("placeholderTargetWebsite")}
                    required
                  />
                  <datalist id="target-site-options">
                    {sites.map((site) => (
                      <option key={site.id} value={site.site_url}>
                        {site.name}
                      </option>
                    ))}
                  </datalist>
                </div>

                <div>
                  <label>{t("clientName")}</label>
                  <input
                    list="client-name-options"
                    value={submissionForm.client_name}
                    onChange={(e) => setSubmissionForm((prev) => ({ ...prev, client_name: e.target.value }))}
                    placeholder={t("placeholderClientName")}
                    required
                  />
                  <datalist id="client-name-options">
                    {clients.map((client) => (
                      <option key={client.id} value={client.name} />
                    ))}
                  </datalist>
                </div>

                {!isOrders ? (
                  <div>
                    <label>{t("fileType")}</label>
                    <div className="toggle source-toggle">
                      <button
                        type="button"
                        className={submissionForm.source_type === "google-doc" ? "active" : ""}
                        onClick={() => setSubmissionForm((prev) => ({ ...prev, source_type: "google-doc" }))}
                      >
                        {t("googleDoc")}
                      </button>
                      <button
                        type="button"
                        className={submissionForm.source_type === "word-doc" ? "active" : ""}
                        onClick={() => setSubmissionForm((prev) => ({ ...prev, source_type: "word-doc" }))}
                      >
                        {t("docxFile")}
                      </button>
                    </div>
                  </div>
                ) : null}

                {!isOrders && submissionForm.source_type === "google-doc" ? (
                  <div>
                    <label>{t("googleDocLink")}</label>
                    <input
                      type="url"
                      value={submissionForm.doc_url}
                      onChange={(e) => setSubmissionForm((prev) => ({ ...prev, doc_url: e.target.value }))}
                      placeholder={t("placeholderGoogleDoc")}
                      required
                    />
                  </div>
                ) : submissionForm.source_type === "word-doc" ? (
                  <div>
                    <label>{t("fileUpload")}</label>
                    <input
                      type="file"
                      accept=".doc,.docx"
                      required
                      onChange={(e) => {
                        const file = e.target.files?.[0] || null;
                        setSubmissionForm((prev) => ({ ...prev, docx_file: file }));
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
                        value={submissionForm.anchor}
                        onChange={(e) => setSubmissionForm((prev) => ({ ...prev, anchor: e.target.value }))}
                        placeholder={t("placeholderAnchor")}
                      />
                    </div>
                    <div>
                      <label>{t("topic")}</label>
                      <input
                        type="text"
                        value={submissionForm.topic}
                        onChange={(e) => setSubmissionForm((prev) => ({ ...prev, topic: e.target.value }))}
                        placeholder={t("placeholderTopic")}
                      />
                    </div>
                  </>
                ) : null}

                <button className="btn submit-btn" type="submit" disabled={submitting}>
                  {submitting ? t("submitting") : t("submit")}
                </button>
              </form>
            </div>
          )}
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

function Sidebar({ t, userRole, activeSection, onSectionChange }) {
  const sections = userRole === "admin"
    ? [
        { id: "admin", label: t("navAdmin") },
        { id: "websites", label: t("navWebsites") },
        { id: "clients", label: t("navClients") },
        { id: "guest-posts", label: t("navPendingGuestPosts") },
        { id: "orders", label: t("navPendingOrders") },
      ]
    : [
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
            {section.label}
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
