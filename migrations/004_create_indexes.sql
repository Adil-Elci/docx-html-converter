CREATE INDEX IF NOT EXISTS idx_clients_status ON clients(status);

CREATE INDEX IF NOT EXISTS idx_sites_status ON sites(status);

CREATE INDEX IF NOT EXISTS idx_site_credentials_site_id ON site_credentials(site_id);
CREATE INDEX IF NOT EXISTS idx_site_credentials_enabled ON site_credentials(enabled);

CREATE INDEX IF NOT EXISTS idx_client_site_access_client_id ON client_site_access(client_id);
CREATE INDEX IF NOT EXISTS idx_client_site_access_site_id ON client_site_access(site_id);
CREATE INDEX IF NOT EXISTS idx_client_site_access_enabled ON client_site_access(enabled);

CREATE INDEX IF NOT EXISTS idx_submissions_client_id ON submissions(client_id);
CREATE INDEX IF NOT EXISTS idx_submissions_site_id ON submissions(site_id);
CREATE INDEX IF NOT EXISTS idx_submissions_status ON submissions(status);
CREATE INDEX IF NOT EXISTS idx_submissions_created_at ON submissions(created_at);

CREATE INDEX IF NOT EXISTS idx_jobs_submission_id ON jobs(submission_id);
CREATE INDEX IF NOT EXISTS idx_jobs_client_id ON jobs(client_id);
CREATE INDEX IF NOT EXISTS idx_jobs_site_id ON jobs(site_id);
CREATE INDEX IF NOT EXISTS idx_jobs_job_status ON jobs(job_status);
CREATE INDEX IF NOT EXISTS idx_jobs_job_status_created_at ON jobs(job_status, created_at);

CREATE INDEX IF NOT EXISTS idx_job_events_job_id_created_at ON job_events(job_id, created_at);

CREATE INDEX IF NOT EXISTS idx_assets_job_id ON assets(job_id);
