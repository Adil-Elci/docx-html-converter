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
