# Internal Linking Scripts

## Purpose
These scripts maintain the canonical article inventory used for same-site internal linking in the Creator flow.

## Main Script
`sync_publishing_site_articles.py`

It fetches published posts from each active publishing site's WordPress REST API and upserts them into:
- `publishing_site_articles`
- `publishing_site_article_categories`

## Usage
From the repository root:

```bash
python -m portal_backend.scripts.internal_linking.sync_publishing_site_articles
```

Sync a single site:

```bash
python -m portal_backend.scripts.internal_linking.sync_publishing_site_articles --site-url https://example.com
```
