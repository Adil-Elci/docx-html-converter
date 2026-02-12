import { useEffect, useMemo, useState } from "react";
import { api } from "./api.js";
import { getLabel } from "./i18n.js";

const getInitialLanguage = () => localStorage.getItem("ui_language") || "en";

const emptyClientForm = () => ({
  name: "",
  primary_domain: "",
  backlink_url: "",
  status: "active",
});

const emptySiteForm = () => ({
  name: "",
  site_url: "",
  wp_rest_base: "/wp-json/wp/v2",
  status: "active",
});

const emptyCredentialForm = () => ({
  site_id: "",
  auth_type: "application_password",
  wp_username: "",
  wp_app_password: "",
  enabled: true,
});

const emptyAccessForm = () => ({
  client_id: "",
  site_id: "",
  enabled: true,
});

const emptySubmissionForm = () => ({
  client_id: "",
  site_id: "",
  source_type: "google-doc",
  doc_url: "",
  file_url: "",
  backlink_placement: "intro",
  post_status: "draft",
  title: "",
  raw_text: "",
  notes: "",
  status: "received",
  rejection_reason: "",
});

const emptyJobForm = () => ({
  submission_id: "",
  job_status: "queued",
});

const emptyEventForm = () => ({
  event_type: "converter_called",
  payload: "{}",
});

const emptyAssetForm = () => ({
  asset_type: "featured_image",
  provider: "openai",
  source_url: "",
  storage_url: "",
  meta: "{}",
});

export default function App() {
  const [language, setLanguage] = useState(getInitialLanguage());
  const [theme, setTheme] = useState(() => localStorage.getItem("theme") || "system");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [clients, setClients] = useState([]);
  const [sites, setSites] = useState([]);
  const [siteCredentials, setSiteCredentials] = useState([]);
  const [accessRows, setAccessRows] = useState([]);
  const [submissions, setSubmissions] = useState([]);
  const [jobs, setJobs] = useState([]);
  const [selectedJobId, setSelectedJobId] = useState("");
  const [jobEvents, setJobEvents] = useState([]);
  const [jobAssets, setJobAssets] = useState([]);

  const [clientForm, setClientForm] = useState(emptyClientForm());
  const [siteForm, setSiteForm] = useState(emptySiteForm());
  const [credentialForm, setCredentialForm] = useState(emptyCredentialForm());
  const [accessForm, setAccessForm] = useState(emptyAccessForm());
  const [submissionForm, setSubmissionForm] = useState(emptySubmissionForm());
  const [jobForm, setJobForm] = useState(emptyJobForm());
  const [eventForm, setEventForm] = useState(emptyEventForm());
  const [assetForm, setAssetForm] = useState(emptyAssetForm());

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

  useEffect(() => {
    if (!selectedJobId) {
      setJobEvents([]);
      setJobAssets([]);
      return;
    }
    loadJobDetails(selectedJobId);
  }, [selectedJobId]);

  const loadAll = async () => {
    try {
      setError("");
      const [clientsData, sitesData, credentialsData, accessData, submissionsData, jobsData] = await Promise.all([
        api.get("/clients"),
        api.get("/sites"),
        api.get("/site-credentials"),
        api.get("/client-site-access"),
        api.get("/submissions"),
        api.get("/jobs"),
      ]);
      setClients(clientsData || []);
      setSites(sitesData || []);
      setSiteCredentials(credentialsData || []);
      setAccessRows(accessData || []);
      setSubmissions(submissionsData || []);
      setJobs(jobsData || []);
    } catch (err) {
      setError(err.message);
    }
  };

  const loadJobDetails = async (jobId) => {
    try {
      setError("");
      const [eventsData, assetsData] = await Promise.all([
        api.get(`/jobs/${jobId}/events`),
        api.get(`/jobs/${jobId}/assets`),
      ]);
      setJobEvents(eventsData || []);
      setJobAssets(assetsData || []);
    } catch (err) {
      setError(err.message);
    }
  };

  const createClient = async (event) => {
    event.preventDefault();
    try {
      await api.post("/clients", clientForm);
      setClientForm(emptyClientForm());
      await loadAll();
    } catch (err) {
      setError(err.message);
    }
  };

  const toggleClientStatus = async (client) => {
    const nextStatus = client.status === "active" ? "inactive" : "active";
    try {
      await api.patch(`/clients/${client.id}`, { status: nextStatus });
      await loadAll();
    } catch (err) {
      setError(err.message);
    }
  };

  const createSite = async (event) => {
    event.preventDefault();
    try {
      await api.post("/sites", siteForm);
      setSiteForm(emptySiteForm());
      await loadAll();
    } catch (err) {
      setError(err.message);
    }
  };

  const toggleSiteStatus = async (site) => {
    const nextStatus = site.status === "active" ? "inactive" : "active";
    try {
      await api.patch(`/sites/${site.id}`, { status: nextStatus });
      await loadAll();
    } catch (err) {
      setError(err.message);
    }
  };

  const createSiteCredential = async (event) => {
    event.preventDefault();
    try {
      await api.post("/site-credentials", credentialForm);
      setCredentialForm(emptyCredentialForm());
      await loadAll();
    } catch (err) {
      setError(err.message);
    }
  };

  const toggleCredentialEnabled = async (credential) => {
    try {
      await api.patch(`/site-credentials/${credential.id}`, { enabled: !credential.enabled });
      await loadAll();
    } catch (err) {
      setError(err.message);
    }
  };

  const createAccess = async (event) => {
    event.preventDefault();
    try {
      await api.post("/client-site-access", accessForm);
      setAccessForm(emptyAccessForm());
      await loadAll();
    } catch (err) {
      setError(err.message);
    }
  };

  const toggleAccessEnabled = async (access) => {
    try {
      await api.patch(`/client-site-access/${access.id}`, { enabled: !access.enabled });
      await loadAll();
    } catch (err) {
      setError(err.message);
    }
  };

  const createSubmission = async (event) => {
    event.preventDefault();
    const payload = {
      ...submissionForm,
      doc_url: submissionForm.source_type === "google-doc" ? submissionForm.doc_url || null : null,
      file_url: submissionForm.source_type === "docx-upload" ? submissionForm.file_url || null : null,
      title: submissionForm.title || null,
      raw_text: submissionForm.raw_text || null,
      notes: submissionForm.notes || null,
      rejection_reason: submissionForm.rejection_reason || null,
    };
    try {
      await api.post("/submissions", payload);
      setSubmissionForm(emptySubmissionForm());
      await loadAll();
    } catch (err) {
      setError(err.message);
    }
  };

  const setSubmissionQueued = async (submission) => {
    try {
      await api.patch(`/submissions/${submission.id}`, { status: "queued" });
      await loadAll();
    } catch (err) {
      setError(err.message);
    }
  };

  const createJob = async (event) => {
    event.preventDefault();
    try {
      await api.post("/jobs", jobForm);
      setJobForm(emptyJobForm());
      await loadAll();
    } catch (err) {
      setError(err.message);
    }
  };

  const setJobStatus = async (job, jobStatus) => {
    try {
      await api.patch(`/jobs/${job.id}`, { job_status: jobStatus });
      await loadAll();
      if (selectedJobId === job.id) {
        await loadJobDetails(job.id);
      }
    } catch (err) {
      setError(err.message);
    }
  };

  const addJobEvent = async (event) => {
    event.preventDefault();
    if (!selectedJobId) return;
    try {
      const parsedPayload = eventForm.payload.trim() ? JSON.parse(eventForm.payload) : {};
      await api.post(`/jobs/${selectedJobId}/events`, {
        event_type: eventForm.event_type,
        payload: parsedPayload,
      });
      setEventForm(emptyEventForm());
      await loadJobDetails(selectedJobId);
    } catch (err) {
      setError(err.message);
    }
  };

  const addJobAsset = async (event) => {
    event.preventDefault();
    if (!selectedJobId) return;
    try {
      const parsedMeta = assetForm.meta.trim() ? JSON.parse(assetForm.meta) : {};
      await api.post(`/jobs/${selectedJobId}/assets`, {
        asset_type: assetForm.asset_type,
        provider: assetForm.provider,
        source_url: assetForm.source_url || null,
        storage_url: assetForm.storage_url || null,
        meta: parsedMeta,
      });
      setAssetForm(emptyAssetForm());
      await loadJobDetails(selectedJobId);
    } catch (err) {
      setError(err.message);
    }
  };

  const findClientName = (clientId) => clients.find((client) => client.id === clientId)?.name || clientId;
  const findSiteName = (siteId) => sites.find((site) => site.id === siteId)?.name || siteId;

  if (loading) {
    return (
      <div className="app">
        <div className="header">
          <div className="title">{t("appTitle")}</div>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <div className="header">
        <div className="title">Schema Operations Console</div>
        <div className="inline">
          <LanguageToggle
            language={language}
            onChange={(next) => {
              setLanguage(next);
              localStorage.setItem("ui_language", next);
            }}
          />
          <ThemeToggle theme={theme} onChange={setTheme} />
          <button className="btn secondary" type="button" onClick={loadAll}>
            Reload
          </button>
        </div>
      </div>

      <div className="container">
        {error ? <div className="panel error">{error}</div> : null}

        <div className="panel">
          <h2>Clients</h2>
          <form className="row two" onSubmit={createClient}>
            <div>
              <label>Name</label>
              <input value={clientForm.name} onChange={(e) => setClientForm((prev) => ({ ...prev, name: e.target.value }))} />
            </div>
            <div>
              <label>Primary Domain</label>
              <input
                value={clientForm.primary_domain}
                onChange={(e) => setClientForm((prev) => ({ ...prev, primary_domain: e.target.value }))}
              />
            </div>
            <div>
              <label>Backlink URL</label>
              <input
                value={clientForm.backlink_url}
                onChange={(e) => setClientForm((prev) => ({ ...prev, backlink_url: e.target.value }))}
              />
            </div>
            <div>
              <label>Status</label>
              <select value={clientForm.status} onChange={(e) => setClientForm((prev) => ({ ...prev, status: e.target.value }))}>
                <option value="active">active</option>
                <option value="inactive">inactive</option>
              </select>
            </div>
            <button className="btn" type="submit">
              Create Client
            </button>
          </form>

          <div className="list">
            {clients.map((client) => (
              <div key={client.id} className="list-item">
                <div className="inline">
                  <div>
                    <div>{client.name}</div>
                    <div className="status">{client.primary_domain}</div>
                  </div>
                  <button className="btn small" type="button" onClick={() => toggleClientStatus(client)}>
                    {client.status}
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="panel">
          <h2>Sites</h2>
          <form className="row two" onSubmit={createSite}>
            <div>
              <label>Name</label>
              <input value={siteForm.name} onChange={(e) => setSiteForm((prev) => ({ ...prev, name: e.target.value }))} />
            </div>
            <div>
              <label>Site URL</label>
              <input value={siteForm.site_url} onChange={(e) => setSiteForm((prev) => ({ ...prev, site_url: e.target.value }))} />
            </div>
            <div>
              <label>WP REST Base</label>
              <input
                value={siteForm.wp_rest_base}
                onChange={(e) => setSiteForm((prev) => ({ ...prev, wp_rest_base: e.target.value }))}
              />
            </div>
            <div>
              <label>Status</label>
              <select value={siteForm.status} onChange={(e) => setSiteForm((prev) => ({ ...prev, status: e.target.value }))}>
                <option value="active">active</option>
                <option value="inactive">inactive</option>
              </select>
            </div>
            <button className="btn" type="submit">
              Create Site
            </button>
          </form>

          <div className="list">
            {sites.map((site) => (
              <div key={site.id} className="list-item">
                <div className="inline">
                  <div>
                    <div>{site.name}</div>
                    <div className="status">{site.site_url}</div>
                  </div>
                  <button className="btn small" type="button" onClick={() => toggleSiteStatus(site)}>
                    {site.status}
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="panel">
          <h2>Site Credentials</h2>
          <form className="row two" onSubmit={createSiteCredential}>
            <div>
              <label>Site</label>
              <select value={credentialForm.site_id} onChange={(e) => setCredentialForm((prev) => ({ ...prev, site_id: e.target.value }))}>
                <option value="">Select</option>
                {sites.map((site) => (
                  <option key={site.id} value={site.id}>
                    {site.name}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label>WordPress Username</label>
              <input
                value={credentialForm.wp_username}
                onChange={(e) => setCredentialForm((prev) => ({ ...prev, wp_username: e.target.value }))}
              />
            </div>
            <div>
              <label>Application Password</label>
              <input
                value={credentialForm.wp_app_password}
                onChange={(e) => setCredentialForm((prev) => ({ ...prev, wp_app_password: e.target.value }))}
              />
            </div>
            <div>
              <label>Enabled</label>
              <select
                value={credentialForm.enabled ? "true" : "false"}
                onChange={(e) => setCredentialForm((prev) => ({ ...prev, enabled: e.target.value === "true" }))}
              >
                <option value="true">true</option>
                <option value="false">false</option>
              </select>
            </div>
            <button className="btn" type="submit">
              Create Credential
            </button>
          </form>

          <div className="list">
            {siteCredentials.map((credential) => (
              <div key={credential.id} className="list-item">
                <div className="inline">
                  <div>
                    <div>{credential.wp_username}</div>
                    <div className="status">{findSiteName(credential.site_id)}</div>
                  </div>
                  <button className="btn small" type="button" onClick={() => toggleCredentialEnabled(credential)}>
                    {credential.enabled ? "enabled" : "disabled"}
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="panel">
          <h2>Client Site Access</h2>
          <form className="row two" onSubmit={createAccess}>
            <div>
              <label>Client</label>
              <select value={accessForm.client_id} onChange={(e) => setAccessForm((prev) => ({ ...prev, client_id: e.target.value }))}>
                <option value="">Select</option>
                {clients.map((client) => (
                  <option key={client.id} value={client.id}>
                    {client.name}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label>Site</label>
              <select value={accessForm.site_id} onChange={(e) => setAccessForm((prev) => ({ ...prev, site_id: e.target.value }))}>
                <option value="">Select</option>
                {sites.map((site) => (
                  <option key={site.id} value={site.id}>
                    {site.name}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label>Enabled</label>
              <select
                value={accessForm.enabled ? "true" : "false"}
                onChange={(e) => setAccessForm((prev) => ({ ...prev, enabled: e.target.value === "true" }))}
              >
                <option value="true">true</option>
                <option value="false">false</option>
              </select>
            </div>
            <button className="btn" type="submit">
              Create Access
            </button>
          </form>

          <div className="list">
            {accessRows.map((access) => (
              <div key={access.id} className="list-item">
                <div className="inline">
                  <div>
                    <div>{findClientName(access.client_id)} {" -> "} {findSiteName(access.site_id)}</div>
                    <div className="status">{access.id}</div>
                  </div>
                  <button className="btn small" type="button" onClick={() => toggleAccessEnabled(access)}>
                    {access.enabled ? "enabled" : "disabled"}
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="panel">
          <h2>Submissions</h2>
          <form className="row two" onSubmit={createSubmission}>
            <div>
              <label>Client</label>
              <select
                value={submissionForm.client_id}
                onChange={(e) => setSubmissionForm((prev) => ({ ...prev, client_id: e.target.value }))}
              >
                <option value="">Select</option>
                {clients.map((client) => (
                  <option key={client.id} value={client.id}>
                    {client.name}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label>Site</label>
              <select
                value={submissionForm.site_id}
                onChange={(e) => setSubmissionForm((prev) => ({ ...prev, site_id: e.target.value }))}
              >
                <option value="">Select</option>
                {sites.map((site) => (
                  <option key={site.id} value={site.id}>
                    {site.name}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label>Source Type</label>
              <select
                value={submissionForm.source_type}
                onChange={(e) => setSubmissionForm((prev) => ({ ...prev, source_type: e.target.value }))}
              >
                <option value="google-doc">google-doc</option>
                <option value="docx-upload">docx-upload</option>
              </select>
            </div>

            {submissionForm.source_type === "google-doc" ? (
              <div>
                <label>Doc URL</label>
                <input
                  value={submissionForm.doc_url}
                  onChange={(e) => setSubmissionForm((prev) => ({ ...prev, doc_url: e.target.value }))}
                />
              </div>
            ) : (
              <div>
                <label>File URL</label>
                <input
                  value={submissionForm.file_url}
                  onChange={(e) => setSubmissionForm((prev) => ({ ...prev, file_url: e.target.value }))}
                />
              </div>
            )}

            <div>
              <label>Backlink Placement</label>
              <select
                value={submissionForm.backlink_placement}
                onChange={(e) => setSubmissionForm((prev) => ({ ...prev, backlink_placement: e.target.value }))}
              >
                <option value="intro">intro</option>
                <option value="conclusion">conclusion</option>
              </select>
            </div>
            <div>
              <label>Post Status</label>
              <select
                value={submissionForm.post_status}
                onChange={(e) => setSubmissionForm((prev) => ({ ...prev, post_status: e.target.value }))}
              >
                <option value="draft">draft</option>
                <option value="publish">publish</option>
              </select>
            </div>
            <div>
              <label>Submission Status</label>
              <select
                value={submissionForm.status}
                onChange={(e) => setSubmissionForm((prev) => ({ ...prev, status: e.target.value }))}
              >
                <option value="received">received</option>
                <option value="validated">validated</option>
                <option value="rejected">rejected</option>
                <option value="queued">queued</option>
              </select>
            </div>
            <div>
              <label>Title</label>
              <input value={submissionForm.title} onChange={(e) => setSubmissionForm((prev) => ({ ...prev, title: e.target.value }))} />
            </div>
            <div>
              <label>Notes</label>
              <textarea value={submissionForm.notes} onChange={(e) => setSubmissionForm((prev) => ({ ...prev, notes: e.target.value }))} />
            </div>
            <button className="btn" type="submit">
              Create Submission
            </button>
          </form>

          <div className="list">
            {submissions.map((submission) => (
              <div key={submission.id} className="list-item">
                <div className="inline">
                  <div>
                    <div>{submission.title || "(untitled submission)"}</div>
                    <div className="status">
                      {submission.status} | {findClientName(submission.client_id)} | {findSiteName(submission.site_id)}
                    </div>
                  </div>
                  <button className="btn small" type="button" onClick={() => setSubmissionQueued(submission)}>
                    set queued
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="panel">
          <h2>Jobs</h2>
          <form className="row two" onSubmit={createJob}>
            <div>
              <label>Submission</label>
              <select value={jobForm.submission_id} onChange={(e) => setJobForm((prev) => ({ ...prev, submission_id: e.target.value }))}>
                <option value="">Select</option>
                {submissions.map((submission) => (
                  <option key={submission.id} value={submission.id}>
                    {submission.title || submission.id}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label>Initial Job Status</label>
              <select value={jobForm.job_status} onChange={(e) => setJobForm((prev) => ({ ...prev, job_status: e.target.value }))}>
                <option value="queued">queued</option>
                <option value="processing">processing</option>
                <option value="succeeded">succeeded</option>
                <option value="failed">failed</option>
                <option value="retrying">retrying</option>
              </select>
            </div>
            <button className="btn" type="submit">
              Create Job
            </button>
          </form>

          <div className="list">
            {jobs.map((job) => (
              <div key={job.id} className="list-item">
                <div className="inline">
                  <div>
                    <div>{job.id}</div>
                    <div className="status">{job.job_status} | submission {job.submission_id}</div>
                  </div>
                  <div className="inline">
                    <button className="btn small" type="button" onClick={() => setJobStatus(job, "processing")}>processing</button>
                    <button className="btn small" type="button" onClick={() => setJobStatus(job, "succeeded")}>succeeded</button>
                    <button className="btn small" type="button" onClick={() => setJobStatus(job, "failed")}>failed</button>
                    <button className="btn secondary small" type="button" onClick={() => setSelectedJobId(job.id)}>
                      details
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {selectedJobId ? (
          <div className="panel">
            <h2>Job Details: {selectedJobId}</h2>
            <div className="row two">
              <form className="row" onSubmit={addJobEvent}>
                <label>Add Event</label>
                <select
                  value={eventForm.event_type}
                  onChange={(e) => setEventForm((prev) => ({ ...prev, event_type: e.target.value }))}
                >
                  <option value="converter_called">converter_called</option>
                  <option value="converter_ok">converter_ok</option>
                  <option value="image_prompt_ok">image_prompt_ok</option>
                  <option value="image_generated">image_generated</option>
                  <option value="wp_post_created">wp_post_created</option>
                  <option value="wp_post_updated">wp_post_updated</option>
                  <option value="failed">failed</option>
                </select>
                <label>Payload JSON</label>
                <textarea
                  value={eventForm.payload}
                  onChange={(e) => setEventForm((prev) => ({ ...prev, payload: e.target.value }))}
                />
                <button className="btn" type="submit">
                  Add Event
                </button>
              </form>

              <form className="row" onSubmit={addJobAsset}>
                <label>Add Asset</label>
                <select
                  value={assetForm.asset_type}
                  onChange={(e) => setAssetForm((prev) => ({ ...prev, asset_type: e.target.value }))}
                >
                  <option value="featured_image">featured_image</option>
                </select>
                <label>Provider</label>
                <select
                  value={assetForm.provider}
                  onChange={(e) => setAssetForm((prev) => ({ ...prev, provider: e.target.value }))}
                >
                  <option value="openai">openai</option>
                  <option value="leonardo">leonardo</option>
                  <option value="other">other</option>
                </select>
                <label>Source URL</label>
                <input
                  value={assetForm.source_url}
                  onChange={(e) => setAssetForm((prev) => ({ ...prev, source_url: e.target.value }))}
                />
                <label>Storage URL</label>
                <input
                  value={assetForm.storage_url}
                  onChange={(e) => setAssetForm((prev) => ({ ...prev, storage_url: e.target.value }))}
                />
                <label>Meta JSON</label>
                <textarea value={assetForm.meta} onChange={(e) => setAssetForm((prev) => ({ ...prev, meta: e.target.value }))} />
                <button className="btn" type="submit">
                  Add Asset
                </button>
              </form>
            </div>

            <div className="row two">
              <div>
                <h3>Events</h3>
                <div className="list">
                  {jobEvents.map((eventItem) => (
                    <div key={eventItem.id} className="list-item">
                      <div className="status">{eventItem.event_type}</div>
                      <pre>{JSON.stringify(eventItem.payload, null, 2)}</pre>
                    </div>
                  ))}
                </div>
              </div>

              <div>
                <h3>Assets</h3>
                <div className="list">
                  {jobAssets.map((asset) => (
                    <div key={asset.id} className="list-item">
                      <div className="status">{asset.asset_type} | {asset.provider}</div>
                      <div>{asset.source_url || "no source_url"}</div>
                      <div>{asset.storage_url || "no storage_url"}</div>
                      <pre>{JSON.stringify(asset.meta, null, 2)}</pre>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        ) : null}
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

function ThemeToggle({ theme, onChange }) {
  return (
    <div className="theme-toggle">
      <button
        type="button"
        className={theme === "light" ? "active" : ""}
        onClick={() => onChange("light")}
        aria-label="Light theme"
      >
        <SunIcon />
      </button>
      <button
        type="button"
        className={theme === "system" ? "active" : ""}
        onClick={() => onChange("system")}
        aria-label="System theme"
      >
        <SystemIcon />
      </button>
      <button
        type="button"
        className={theme === "dark" ? "active" : ""}
        onClick={() => onChange("dark")}
        aria-label="Dark theme"
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
