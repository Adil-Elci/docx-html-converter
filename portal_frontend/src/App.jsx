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

const baseApiUrl = import.meta.env.VITE_API_BASE_URL || "";

export default function App() {
  const [activeSection, setActiveSection] = useState("guest-posts");
  const [language, setLanguage] = useState(getInitialLanguage());
  const [theme, setTheme] = useState(() => localStorage.getItem("theme") || "system");
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

  const [clients, setClients] = useState([]);
  const [sites, setSites] = useState([]);
  const [submissionForm, setSubmissionForm] = useState(emptySubmissionForm());

  const t = useMemo(() => (key) => getLabel(language, key), [language]);

  useEffect(() => {
    if (theme === "system") {
      document.documentElement.removeAttribute("data-theme");
    } else {
      document.documentElement.setAttribute("data-theme", theme);
    }
    localStorage.setItem("theme", theme);
  }, [theme]);

  useEffect(() => {
    loadAll().finally(() => setLoading(false));
  }, []);

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

  const submitGuestPost = async (event) => {
    event.preventDefault();
    setError("");
    setSuccess("");
    const effectiveSourceType = activeSection === "orders" ? "google-doc" : submissionForm.source_type;

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

      const rawBody = await response.text();
      let payload = null;
      try {
        payload = rawBody ? JSON.parse(rawBody) : null;
      } catch {
        payload = null;
      }

      if (!response.ok) {
        const detail = payload?.detail || payload?.error || rawBody || t("errorRequestFailed");
        throw new Error(detail);
      }

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

  if (loading) {
    return (
      <div className="app-shell">
        <Sidebar t={t} activeSection={activeSection} onSectionChange={setActiveSection} />
        <div className="app-main">
          <div className="header">
            <div className="title">{t("appTitle")}</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="app-shell">
      <Sidebar t={t} activeSection={activeSection} onSectionChange={setActiveSection} />

      <div className="app-main">
        <div className="header">
          <div className="title">{t("clientsPortal")}</div>
          <div className="inline">
            <LanguageToggle
              language={language}
              onChange={(next) => {
                setLanguage(next);
                localStorage.setItem("ui_language", next);
              }}
            />
            <ThemeToggle theme={theme} onChange={setTheme} t={t} />
          </div>
        </div>

        <div className="container">
          <div className="hero">
            <h1>{activeSection === "orders" ? t("heroCreateOrder") : t("heroCreateGuestPost")}</h1>
          </div>

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

          {error && error !== "Load failed" && error !== "Failed to fetch" ? <div className="panel error">{error}</div> : null}
          {success ? <div className="panel success">{success}</div> : null}

          <div className="panel form-panel">
            <h2>{activeSection === "orders" ? t("formOrder") : t("formSubmission")}</h2>
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

              {activeSection !== "orders" ? (
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

              {activeSection !== "orders" && submissionForm.source_type === "google-doc" ? (
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

              {activeSection === "orders" ? (
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
        </div>
      </div>
    </div>
  );
}

function Sidebar({ t, activeSection, onSectionChange }) {
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
        <button
          type="button"
          className={`nav-item ${activeSection === "guest-posts" ? "active" : ""}`}
          onClick={() => onSectionChange("guest-posts")}
        >
          {t("navGuestPosts")}
        </button>
        <button
          type="button"
          className={`nav-item ${activeSection === "orders" ? "active" : ""}`}
          onClick={() => onSectionChange("orders")}
        >
          {t("navOrders")}
        </button>
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
