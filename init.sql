CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS clients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    primary_domain TEXT NOT NULL,
    backlink_url TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'inactive')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sites (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    site_url TEXT NOT NULL UNIQUE,
    wp_rest_base TEXT NOT NULL DEFAULT '/wp-json/wp/v2',
    status TEXT NOT NULL CHECK (status IN ('active', 'inactive')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS site_credentials (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    site_id UUID NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    auth_type TEXT NOT NULL CHECK (auth_type IN ('application_password')),
    wp_username TEXT NOT NULL,
    wp_app_password TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT site_credentials_site_username_unique UNIQUE (site_id, wp_username)
);

CREATE TABLE IF NOT EXISTS client_site_access (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    site_id UUID NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT client_site_access_client_site_unique UNIQUE (client_id, site_id)
);

CREATE TABLE IF NOT EXISTS submissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL REFERENCES clients(id),
    site_id UUID NOT NULL REFERENCES sites(id),
    source_type TEXT NOT NULL CHECK (source_type IN ('google-doc', 'docx-upload')),
    doc_url TEXT NULL,
    file_url TEXT NULL,
    backlink_placement TEXT NOT NULL CHECK (backlink_placement IN ('intro', 'conclusion')),
    post_status TEXT NOT NULL CHECK (post_status IN ('draft', 'publish')),
    title TEXT NULL,
    raw_text TEXT NULL,
    notes TEXT NULL,
    status TEXT NOT NULL CHECK (status IN ('received', 'validated', 'rejected', 'queued')),
    rejection_reason TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT submissions_source_payload_check CHECK (
        (source_type = 'google-doc' AND doc_url IS NOT NULL AND file_url IS NULL)
        OR (source_type = 'docx-upload' AND file_url IS NOT NULL AND doc_url IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id UUID NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
    client_id UUID NOT NULL REFERENCES clients(id),
    site_id UUID NOT NULL REFERENCES sites(id),
    job_status TEXT NOT NULL CHECK (job_status IN ('queued', 'processing', 'succeeded', 'failed', 'retrying')),
    attempt_count INT NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    wp_post_id BIGINT NULL,
    wp_post_url TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL CHECK (
        event_type IN (
            'converter_called',
            'converter_ok',
            'image_prompt_ok',
            'image_generated',
            'wp_post_created',
            'wp_post_updated',
            'failed'
        )
    ),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    asset_type TEXT NOT NULL CHECK (asset_type IN ('featured_image')),
    provider TEXT NOT NULL CHECK (provider IN ('leonardo', 'openai', 'other')),
    source_url TEXT NULL,
    storage_url TEXT NULL,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

DROP TRIGGER IF EXISTS trg_clients_set_updated_at ON clients;
CREATE TRIGGER trg_clients_set_updated_at
BEFORE UPDATE ON clients
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_sites_set_updated_at ON sites;
CREATE TRIGGER trg_sites_set_updated_at
BEFORE UPDATE ON sites
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_site_credentials_set_updated_at ON site_credentials;
CREATE TRIGGER trg_site_credentials_set_updated_at
BEFORE UPDATE ON site_credentials
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_client_site_access_set_updated_at ON client_site_access;
CREATE TRIGGER trg_client_site_access_set_updated_at
BEFORE UPDATE ON client_site_access
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_submissions_set_updated_at ON submissions;
CREATE TRIGGER trg_submissions_set_updated_at
BEFORE UPDATE ON submissions
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_jobs_set_updated_at ON jobs;
CREATE TRIGGER trg_jobs_set_updated_at
BEFORE UPDATE ON jobs
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
