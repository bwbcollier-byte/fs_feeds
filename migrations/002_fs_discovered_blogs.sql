-- =============================================================================
--  Migration 002 — fs_discovered_blogs
-- =============================================================================
--
--  Records brand / store homepages that EXPOSE A BLOG but don't publish an RSS
--  feed. These are candidates for a future HTML-scraping pipeline; the RSS
--  scraper (fashion_scraper.py) ignores them.
--
--  Populated by `discover_feeds.py` when:
--    - the homepage loads 200, but
--    - every RSS/Atom probe (autodiscovery, fallback paths, feedsearch.dev)
--      returns nothing, and
--    - at least one visible "blog" / "news" / "journal" / "editorial" link
--      was found on the homepage.
-- =============================================================================

CREATE TABLE IF NOT EXISTS fs_discovered_blogs (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_kind      text NOT NULL CHECK (entity_kind IN ('brand', 'store')),
  entity_id        uuid NOT NULL,
  blog_url         text NOT NULL,
  detected_via     text,            -- 'anchor_scan' | 'feedsearch' | 'manual'
  first_seen_at    timestamptz NOT NULL DEFAULT now(),
  last_checked_at  timestamptz NOT NULL DEFAULT now(),
  -- One blog per entity (idempotent discovery; re-runs UPDATE last_checked_at)
  UNIQUE (entity_kind, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_fs_discovered_blogs_entity
  ON fs_discovered_blogs (entity_kind, entity_id);
CREATE INDEX IF NOT EXISTS idx_fs_discovered_blogs_last_checked
  ON fs_discovered_blogs (last_checked_at DESC);
