CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE clients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    website_domain TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    client_id UUID NULL REFERENCES clients(id),
    ui_language TEXT NOT NULL DEFAULT 'en',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT users_role_check CHECK (role IN ('admin', 'client')),
    CONSTRAINT users_client_role_check CHECK (
        (role = 'admin' AND client_id IS NULL) OR (role = 'client' AND client_id IS NOT NULL)
    )
);

CREATE TABLE invites (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL,
    client_id UUID NOT NULL REFERENCES clients(id),
    token TEXT NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE target_sites (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    site_name TEXT NOT NULL,
    site_url TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE guest_posts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL REFERENCES clients(id),
    target_site_id UUID NOT NULL REFERENCES target_sites(id),
    status TEXT NOT NULL DEFAULT 'draft',
    title_h1 TEXT NOT NULL,
    backlink_url TEXT NOT NULL,
    backlink_placement TEXT NULL,
    auto_backlink BOOLEAN NOT NULL DEFAULT TRUE,
    content_json JSONB NOT NULL,
    content_markdown TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    submitted_at TIMESTAMPTZ NULL,
    CONSTRAINT guest_posts_status_check CHECK (status IN ('draft', 'submitted')),
    CONSTRAINT guest_posts_backlink_placement_check CHECK (
        (auto_backlink = TRUE AND backlink_placement IS NULL)
        OR (auto_backlink = FALSE AND backlink_placement IN ('intro', 'conclusion'))
    )
);

CREATE INDEX idx_users_client_id ON users(client_id);
CREATE INDEX idx_invites_client_id ON invites(client_id);
CREATE INDEX idx_target_sites_active ON target_sites(active);
CREATE INDEX idx_guest_posts_client_id ON guest_posts(client_id);
CREATE INDEX idx_guest_posts_status ON guest_posts(status);
