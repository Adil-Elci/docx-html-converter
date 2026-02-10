import { useEffect, useMemo, useState } from "react";
import { api } from "./api.js";
import { getLabel } from "./i18n.js";

const emptySection = () => ({ section_title: "", section_body: "" });

const emptyForm = () => ({
  target_site_id: "",
  backlink_url: "",
  auto_backlink: true,
  backlink_placement: "intro",
  title_h1: "",
  introduction: "",
  sections: [emptySection()],
});

const getInitialLanguage = () => localStorage.getItem("ui_language") || "en";

export default function App() {
  const [user, setUser] = useState(null);
  const [language, setLanguage] = useState(getInitialLanguage());
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const t = (key) => getLabel(language, key);

  useEffect(() => {
    let mounted = true;
    api
      .get("/me")
      .then((data) => {
        if (!mounted) return;
        setUser(data);
        if (data?.ui_language) {
          setLanguage(data.ui_language);
          localStorage.setItem("ui_language", data.ui_language);
        }
      })
      .catch(() => {})
      .finally(() => {
        if (mounted) setLoading(false);
      });
    return () => {
      mounted = false;
    };
  }, []);

  const handleLanguageChange = (next) => {
    setLanguage(next);
    localStorage.setItem("ui_language", next);
    if (user) {
      api.patch("/me/ui-language", { ui_language: next }).catch(() => {});
    }
  };

  const handleLogout = () => {
    api.post("/auth/logout", {}).finally(() => {
      setUser(null);
    });
  };

  if (loading) {
    return (
      <div className="app">
        <div className="header">
          <div className="title">{t("appTitle")}</div>
        </div>
      </div>
    );
  }

  if (!user) {
    return (
      <div className="app">
        <div className="header">
          <div className="title">{t("appTitle")}</div>
          <LanguageToggle language={language} onChange={handleLanguageChange} />
        </div>
        <div className="container auth-container">
          <AuthScreen
            language={language}
            onLogin={(data) => {
              setUser(data.user);
              setLanguage(data.user.ui_language);
              localStorage.setItem("ui_language", data.user.ui_language);
            }}
            setError={setError}
            error={error}
          />
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <div className="header">
        <div className="title">{t("appTitle")}</div>
        <div className="inline">
          <span className="pill">{user.role}</span>
          <LanguageToggle language={language} onChange={handleLanguageChange} />
          <button className="btn secondary" onClick={handleLogout}>
            {t("logout")}
          </button>
        </div>
      </div>
      <div className="container">
        {user.role === "admin" ? <AdminDashboard t={t} /> : <ClientDashboard t={t} />}
      </div>
    </div>
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

function AuthScreen({ language, onLogin, setError, error }) {
  const tokenFromUrl = new URLSearchParams(window.location.search).get("token") || "";
  const [mode] = useState(() => (tokenFromUrl ? "register" : "login"));
  const [loginEmail, setLoginEmail] = useState("");
  const [loginPassword, setLoginPassword] = useState("");
  const [showLoginPassword, setShowLoginPassword] = useState(false);
  const [regEmail, setRegEmail] = useState("");
  const [regPassword, setRegPassword] = useState("");
  const [showRegPassword, setShowRegPassword] = useState(false);
  const [token, setToken] = useState(tokenFromUrl);

  const t = useMemo(() => (key) => getLabel(language, key), [language]);

  const submitLogin = async (event) => {
    event.preventDefault();
    setError("");
    try {
      const data = await api.post("/auth/login", {
        email: loginEmail,
        password: loginPassword,
      });
      onLogin(data);
    } catch (err) {
      setError(err.message);
    }
  };

  const submitRegister = async (event) => {
    event.preventDefault();
    setError("");
    try {
      const data = await api.post("/auth/register", {
        token,
        email: regEmail,
        password: regPassword,
        ui_language: language,
      });
      onLogin(data);
    } catch (err) {
      setError(err.message);
    }
  };

  return (
    <div className="panel auth-panel">
      <h2>{mode === "login" ? t("login") : t("register")}</h2>
      {error ? <div className="error">{error}</div> : null}
      {mode === "login" ? (
        <form className="row auth-form" onSubmit={submitLogin}>
          <div>
            <label>{t("email")}</label>
            <input value={loginEmail} onChange={(e) => setLoginEmail(e.target.value)} />
          </div>
          <div>
            <label>{t("password")}</label>
            <div className="password-field">
              <input
                type={showLoginPassword ? "text" : "password"}
                value={loginPassword}
                onChange={(e) => setLoginPassword(e.target.value)}
              />
              <button
                type="button"
                className="icon-btn"
                onClick={() => setShowLoginPassword((prev) => !prev)}
                aria-label={showLoginPassword ? "Hide password" : "Show password"}
              >
                <EyeIcon open={showLoginPassword} />
              </button>
            </div>
          </div>
          <button className="btn" type="submit">
            {t("login")}
          </button>
        </form>
      ) : (
        <form className="row auth-form" onSubmit={submitRegister}>
          <div>
            <label>{t("inviteToken")}</label>
            <input value={token} onChange={(e) => setToken(e.target.value)} />
          </div>
          <div>
            <label>{t("email")}</label>
            <input value={regEmail} onChange={(e) => setRegEmail(e.target.value)} />
          </div>
          <div>
            <label>{t("password")}</label>
            <div className="password-field">
              <input
                type={showRegPassword ? "text" : "password"}
                value={regPassword}
                onChange={(e) => setRegPassword(e.target.value)}
              />
              <button
                type="button"
                className="icon-btn"
                onClick={() => setShowRegPassword((prev) => !prev)}
                aria-label={showRegPassword ? "Hide password" : "Show password"}
              >
                <EyeIcon open={showRegPassword} />
              </button>
            </div>
          </div>
          <button className="btn" type="submit">
            {t("register")}
          </button>
        </form>
      )}
    </div>
  );
}

function EyeIcon({ open }) {
  return open ? (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="M2.5 12s3.6-6 9.5-6 9.5 6 9.5 6-3.6 6-9.5 6-9.5-6-9.5-6Z"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle cx="12" cy="12" r="3.2" fill="none" stroke="currentColor" strokeWidth="1.6" />
    </svg>
  ) : (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="M2.5 12s3.6-6 9.5-6 9.5 6 9.5 6-3.6 6-9.5 6-9.5-6-9.5-6Z"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
        strokeLinejoin="round"
        opacity="0.6"
      />
      <circle cx="12" cy="12" r="3.2" fill="none" stroke="currentColor" strokeWidth="1.6" />
      <path
        d="M4 4l16 16"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
      />
    </svg>
  );
}

function ClientDashboard({ t }) {
  const [posts, setPosts] = useState([]);
  const [sites, setSites] = useState([]);
  const [form, setForm] = useState(emptyForm());
  const [editingId, setEditingId] = useState(null);
  const [error, setError] = useState("");

  const load = () => {
    api.get("/guest-posts").then(setPosts).catch(() => {});
    api.get("/target-sites").then(setSites).catch(() => {});
  };

  useEffect(() => {
    load();
  }, []);

  const updateForm = (field, value) => {
    setForm((prev) => ({ ...prev, [field]: value }));
  };

  const updateSection = (index, field, value) => {
    setForm((prev) => {
      const sections = prev.sections.map((section, idx) =>
        idx === index ? { ...section, [field]: value } : section
      );
      return { ...prev, sections };
    });
  };

  const addSection = () => {
    setForm((prev) => {
      if (prev.sections.length >= 6) return prev;
      return { ...prev, sections: [...prev.sections, emptySection()] };
    });
  };

  const resetForm = () => {
    setEditingId(null);
    setForm(emptyForm());
  };

  const saveDraft = async () => {
    setError("");
    const payload = {
      target_site_id: form.target_site_id,
      backlink_url: form.backlink_url,
      auto_backlink: form.auto_backlink,
      backlink_placement: form.auto_backlink ? null : form.backlink_placement,
      title_h1: form.title_h1,
      content_json: {
        introduction: form.introduction,
        sections: form.sections,
      },
    };
    try {
      if (editingId) {
        await api.patch(`/guest-posts/${editingId}`, payload);
      } else {
        await api.post("/guest-posts", payload);
      }
      resetForm();
      load();
    } catch (err) {
      setError(err.message);
    }
  };

  const submitDraft = async () => {
    if (!editingId) return;
    try {
      await api.post(`/guest-posts/${editingId}/submit`, {});
      resetForm();
      load();
    } catch (err) {
      setError(err.message);
    }
  };

  const editPost = (post) => {
    setEditingId(post.id);
    setForm({
      target_site_id: post.target_site_id,
      backlink_url: post.backlink_url,
      auto_backlink: post.auto_backlink,
      backlink_placement: post.backlink_placement || "intro",
      title_h1: post.title_h1,
      introduction: post.content_json.introduction || "",
      sections: post.content_json.sections || [emptySection()],
    });
  };

  return (
    <>
      <div className="panel">
        <h2>{t("clientDashboard")}</h2>
        {error ? <div className="error">{error}</div> : null}
        <div className="inline">
          <button className="btn secondary" onClick={resetForm} type="button">
            {t("createDraft")}
          </button>
          <button className="btn ghost small" onClick={load} type="button">
            {t("refresh")}
          </button>
        </div>
        <div className="list">
          {posts.map((post) => (
            <div key={post.id} className="list-item">
              <div className="inline">
                <div>
                  <div className="status">{post.status}</div>
                  <div>{post.title_h1}</div>
                </div>
                {post.status === "draft" ? (
                  <button className="btn small" onClick={() => editPost(post)} type="button">
                    Edit
                  </button>
                ) : null}
              </div>
            </div>
          ))}
        </div>
      </div>

      <div className="panel">
        <h2>{editingId ? t("saveDraft") : t("createDraft")}</h2>
        <div className="row two">
          <div>
            <label>{t("targetSite")}</label>
            <select
              value={form.target_site_id}
              onChange={(e) => updateForm("target_site_id", e.target.value)}
            >
              <option value="">Select</option>
              {sites.map((site) => (
                <option key={site.id} value={site.id}>
                  {site.site_name}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label>{t("backlinkUrl")}</label>
            <input
              value={form.backlink_url}
              onChange={(e) => updateForm("backlink_url", e.target.value)}
            />
          </div>
          <div>
            <label>{t("autoBacklink")}</label>
            <select
              value={form.auto_backlink ? "on" : "off"}
              onChange={(e) => updateForm("auto_backlink", e.target.value === "on")}
            >
              <option value="on">On</option>
              <option value="off">Off</option>
            </select>
          </div>
          {!form.auto_backlink ? (
            <div>
              <label>{t("backlinkPlacement")}</label>
              <select
                value={form.backlink_placement}
                onChange={(e) => updateForm("backlink_placement", e.target.value)}
              >
                <option value="intro">Intro</option>
                <option value="conclusion">Conclusion</option>
              </select>
            </div>
          ) : null}
          <div>
            <label>{t("titleH1")}</label>
            <input value={form.title_h1} onChange={(e) => updateForm("title_h1", e.target.value)} />
          </div>
          <div>
            <label>{t("introduction")}</label>
            <textarea
              value={form.introduction}
              onChange={(e) => updateForm("introduction", e.target.value)}
            />
          </div>
        </div>
        <div className="row">
          <label>{t("sections")}</label>
          {form.sections.map((section, idx) => (
            <div className="row" key={idx}>
              <input
                placeholder={t("sectionTitle")}
                value={section.section_title}
                onChange={(e) => updateSection(idx, "section_title", e.target.value)}
              />
              <textarea
                placeholder={t("sectionBody")}
                value={section.section_body}
                onChange={(e) => updateSection(idx, "section_body", e.target.value)}
              />
            </div>
          ))}
          <button className="btn ghost" type="button" onClick={addSection}>
            {t("addSection")}
          </button>
        </div>
        <div className="inline">
          <button className="btn" type="button" onClick={saveDraft}>
            {t("saveDraft")}
          </button>
          {editingId ? (
            <button className="btn secondary" type="button" onClick={submitDraft}>
              {t("submitForReview")}
            </button>
          ) : null}
        </div>
      </div>
    </>
  );
}

function AdminDashboard({ t }) {
  const [clients, setClients] = useState([]);
  const [targets, setTargets] = useState([]);
  const [posts, setPosts] = useState([]);
  const [selectedPost, setSelectedPost] = useState(null);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteClient, setInviteClient] = useState("");
  const [inviteToken, setInviteToken] = useState("");
  const [targetName, setTargetName] = useState("");
  const [targetUrl, setTargetUrl] = useState("");

  const load = () => {
    api.get("/clients").then(setClients).catch(() => {});
    api.get("/target-sites").then(setTargets).catch(() => {});
    api.get("/admin/guest-posts").then(setPosts).catch(() => {});
  };

  useEffect(() => {
    load();
  }, []);

  const toggleClient = async (client) => {
    await api.patch(`/clients/${client.id}`, { active: !client.active });
    load();
  };

  const toggleTarget = async (site) => {
    await api.patch(`/target-sites/${site.id}`, { active: !site.active });
    load();
  };

  const createInvite = async () => {
    const data = await api.post("/auth/invite", {
      email: inviteEmail,
      client_id: inviteClient,
    });
    setInviteToken(data.token);
  };

  const createTarget = async () => {
    await api.post("/target-sites", { site_name: targetName, site_url: targetUrl });
    setTargetName("");
    setTargetUrl("");
    load();
  };

  return (
    <>
      <div className="panel">
        <h2>{t("manageClients")}</h2>
        <div className="list">
          {clients.map((client) => (
            <div key={client.id} className="list-item">
              <div className="inline">
                <div>
                  <div>{client.name}</div>
                  <div className="status">{client.website_domain}</div>
                </div>
                <button className="btn small" type="button" onClick={() => toggleClient(client)}>
                  {client.active ? t("active") : "Inactive"}
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>

      <div className="panel">
        <h2>{t("manageTargets")}</h2>
        <div className="row two">
          <div>
            <label>{t("targetSiteName")}</label>
            <input value={targetName} onChange={(e) => setTargetName(e.target.value)} />
          </div>
          <div>
            <label>{t("targetSiteUrl")}</label>
            <input value={targetUrl} onChange={(e) => setTargetUrl(e.target.value)} />
          </div>
        </div>
        <button className="btn" onClick={createTarget} type="button">
          Create
        </button>
        <div className="list">
          {targets.map((site) => (
            <div key={site.id} className="list-item">
              <div className="inline">
                <div>
                  <div>{site.site_name}</div>
                  <div className="status">{site.site_url}</div>
                </div>
                <button className="btn small" type="button" onClick={() => toggleTarget(site)}>
                  {site.active ? t("active") : "Inactive"}
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>

      <div className="panel">
        <h2>{t("createInvite")}</h2>
        <div className="row two">
          <div>
            <label>{t("email")}</label>
            <input value={inviteEmail} onChange={(e) => setInviteEmail(e.target.value)} />
          </div>
          <div>
            <label>{t("client")}</label>
            <select value={inviteClient} onChange={(e) => setInviteClient(e.target.value)}>
              <option value="">Select</option>
              {clients.map((client) => (
                <option key={client.id} value={client.id}>
                  {client.name}
                </option>
              ))}
            </select>
          </div>
        </div>
        <button className="btn" type="button" onClick={createInvite}>
          {t("createInvite")}
        </button>
        {inviteToken ? (
          <div className="list-item">
            {t("inviteCreated")}: {inviteToken}
          </div>
        ) : null}
      </div>

      <div className="panel">
        <h2>{t("posts")}</h2>
        <div className="list">
          {posts.map((post) => (
            <div key={post.id} className="list-item">
              <div className="inline">
                <div>
                  <div className="status">{post.status}</div>
                  <div>{post.title_h1}</div>
                </div>
                <button className="btn small" type="button" onClick={() => setSelectedPost(post)}>
                  View
                </button>
              </div>
            </div>
          ))}
        </div>
        {selectedPost ? (
          <div className="list-item">
            <div className="status">{selectedPost.status}</div>
            <div>{selectedPost.title_h1}</div>
            <div>{selectedPost.backlink_url}</div>
            <pre>{selectedPost.content_markdown}</pre>
          </div>
        ) : null}
      </div>
    </>
  );
}
