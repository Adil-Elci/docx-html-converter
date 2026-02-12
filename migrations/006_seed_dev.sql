-- Development seed data for local testing.
-- Uses stable UUIDs so relationships remain deterministic.

INSERT INTO clients (id, name, primary_domain, backlink_url, status)
VALUES
    ('11111111-1111-1111-1111-111111111111', 'Northstar Media', 'northstar.example', 'https://northstar.example/resources', 'active'),
    ('22222222-2222-2222-2222-222222222222', 'Bluepeak Labs', 'bluepeak.example', 'https://bluepeak.example/insights', 'active')
ON CONFLICT (id) DO UPDATE
SET
    name = EXCLUDED.name,
    primary_domain = EXCLUDED.primary_domain,
    backlink_url = EXCLUDED.backlink_url,
    status = EXCLUDED.status;

INSERT INTO sites (id, name, site_url, wp_rest_base, status)
VALUES
    ('33333333-3333-3333-3333-333333333331', 'Publisher One', 'https://publisher-one.example', '/wp-json/wp/v2', 'active'),
    ('33333333-3333-3333-3333-333333333332', 'Publisher Two', 'https://publisher-two.example', '/wp-json/wp/v2', 'active'),
    ('33333333-3333-3333-3333-333333333333', 'Publisher Three', 'https://publisher-three.example', '/wp-json/wp/v2', 'inactive')
ON CONFLICT (id) DO UPDATE
SET
    name = EXCLUDED.name,
    site_url = EXCLUDED.site_url,
    wp_rest_base = EXCLUDED.wp_rest_base,
    status = EXCLUDED.status;

INSERT INTO site_credentials (id, site_id, auth_type, wp_username, wp_app_password, enabled)
VALUES
    ('44444444-4444-4444-4444-444444444441', '33333333-3333-3333-3333-333333333331', 'application_password', 'editor_one', 'app-pass-one', true),
    ('44444444-4444-4444-4444-444444444442', '33333333-3333-3333-3333-333333333332', 'application_password', 'editor_two', 'app-pass-two', true)
ON CONFLICT (site_id, wp_username) DO UPDATE
SET
    auth_type = EXCLUDED.auth_type,
    wp_app_password = EXCLUDED.wp_app_password,
    enabled = EXCLUDED.enabled,
    updated_at = NOW();

INSERT INTO client_site_access (id, client_id, site_id, enabled)
VALUES
    ('55555555-5555-5555-5555-555555555551', '11111111-1111-1111-1111-111111111111', '33333333-3333-3333-3333-333333333331', true),
    ('55555555-5555-5555-5555-555555555552', '22222222-2222-2222-2222-222222222222', '33333333-3333-3333-3333-333333333332', true),
    ('55555555-5555-5555-5555-555555555553', '11111111-1111-1111-1111-111111111111', '33333333-3333-3333-3333-333333333333', false)
ON CONFLICT (client_id, site_id) DO UPDATE
SET
    enabled = EXCLUDED.enabled,
    updated_at = NOW();

INSERT INTO submissions (
    id,
    client_id,
    site_id,
    source_type,
    doc_url,
    file_url,
    backlink_placement,
    post_status,
    title,
    raw_text,
    notes,
    status,
    rejection_reason
)
VALUES
    (
        '66666666-6666-6666-6666-666666666661',
        '11111111-1111-1111-1111-111111111111',
        '33333333-3333-3333-3333-333333333331',
        'google-doc',
        'https://docs.google.com/document/d/sample-northstar/edit',
        NULL,
        'intro',
        'draft',
        'AI Content Operations for Lean Teams',
        'Draft raw text for conversion.',
        'Ninja Forms intake sample.',
        'queued',
        NULL
    ),
    (
        '66666666-6666-6666-6666-666666666662',
        '22222222-2222-2222-2222-222222222222',
        '33333333-3333-3333-3333-333333333332',
        'docx-upload',
        NULL,
        'https://files.example/uploads/bluepeak-playbook.docx',
        'conclusion',
        'publish',
        'Scaling Editorial Pipelines with Automations',
        'Uploaded docx sample text.',
        'Uploaded from client portal.',
        'validated',
        NULL
    )
ON CONFLICT (id) DO UPDATE
SET
    client_id = EXCLUDED.client_id,
    site_id = EXCLUDED.site_id,
    source_type = EXCLUDED.source_type,
    doc_url = EXCLUDED.doc_url,
    file_url = EXCLUDED.file_url,
    backlink_placement = EXCLUDED.backlink_placement,
    post_status = EXCLUDED.post_status,
    title = EXCLUDED.title,
    raw_text = EXCLUDED.raw_text,
    notes = EXCLUDED.notes,
    status = EXCLUDED.status,
    rejection_reason = EXCLUDED.rejection_reason,
    updated_at = NOW();

INSERT INTO jobs (
    id,
    submission_id,
    client_id,
    site_id,
    job_status,
    attempt_count,
    last_error,
    wp_post_id,
    wp_post_url
)
VALUES
    (
        '77777777-7777-7777-7777-777777777771',
        '66666666-6666-6666-6666-666666666661',
        '11111111-1111-1111-1111-111111111111',
        '33333333-3333-3333-3333-333333333331',
        'processing',
        1,
        NULL,
        NULL,
        NULL
    ),
    (
        '77777777-7777-7777-7777-777777777772',
        '66666666-6666-6666-6666-666666666662',
        '22222222-2222-2222-2222-222222222222',
        '33333333-3333-3333-3333-333333333332',
        'succeeded',
        1,
        NULL,
        2841,
        'https://publisher-two.example/scaling-editorial-pipelines-with-automations'
    )
ON CONFLICT (id) DO UPDATE
SET
    submission_id = EXCLUDED.submission_id,
    client_id = EXCLUDED.client_id,
    site_id = EXCLUDED.site_id,
    job_status = EXCLUDED.job_status,
    attempt_count = EXCLUDED.attempt_count,
    last_error = EXCLUDED.last_error,
    wp_post_id = EXCLUDED.wp_post_id,
    wp_post_url = EXCLUDED.wp_post_url,
    updated_at = NOW();

INSERT INTO job_events (id, job_id, event_type, payload)
VALUES
    (
        '88888888-8888-8888-8888-888888888881',
        '77777777-7777-7777-7777-777777777771',
        'converter_called',
        '{"source":"make.com","step":"converter"}'::jsonb
    ),
    (
        '88888888-8888-8888-8888-888888888882',
        '77777777-7777-7777-7777-777777777772',
        'wp_post_created',
        '{"post_id":2841,"status":"publish"}'::jsonb
    )
ON CONFLICT (id) DO UPDATE
SET
    job_id = EXCLUDED.job_id,
    event_type = EXCLUDED.event_type,
    payload = EXCLUDED.payload;

INSERT INTO assets (id, job_id, asset_type, provider, source_url, storage_url, meta)
VALUES
    (
        '99999999-9999-9999-9999-999999999991',
        '77777777-7777-7777-7777-777777777772',
        'featured_image',
        'openai',
        'https://images.example/generated/bluepeak-feature-raw.png',
        'https://cdn.example/assets/bluepeak-feature.webp',
        '{"width":1536,"height":1024,"format":"webp"}'::jsonb
    )
ON CONFLICT (id) DO UPDATE
SET
    job_id = EXCLUDED.job_id,
    asset_type = EXCLUDED.asset_type,
    provider = EXCLUDED.provider,
    source_url = EXCLUDED.source_url,
    storage_url = EXCLUDED.storage_url,
    meta = EXCLUDED.meta;
