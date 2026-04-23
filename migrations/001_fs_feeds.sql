-- =============================================================================
--  Migration 001 — fs_feeds registry + fs_feed_{brands,stores} link tables
--  + fs_news.feed_id FK + rollup trigger + health view
-- =============================================================================
--
-- Depends on: fs_brands (id uuid PK), fs_stores (id uuid PK), fs_news (exists).
--
-- Run in the Supabase SQL editor, or pipe to psql. Idempotent — uses
-- `CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, etc.
-- =============================================================================


-- 1. Main feed registry --------------------------------------------------------
CREATE TABLE IF NOT EXISTS fs_feeds (
  id                          uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  -- ── identity & config (mirrors feeds.yaml) ─────
  url                         text        NOT NULL UNIQUE,
  name                        text        NOT NULL,
  domain                      text        NOT NULL,
  category                    text        NOT NULL,      -- industry_news | editorial | streetwear_drops | events | jobs | trade
  country                     text        NOT NULL,      -- ISO alpha-2 or 'GLOBAL'
  language                    text        NOT NULL,      -- ISO 639-1
  paywalled                   boolean     NOT NULL DEFAULT false,
  extract_body                boolean,                   -- NULL = default (not paywalled)
  user_agent                  text,                      -- NULL | 'browser' | literal UA

  -- ── discovery metadata (populated from the feed's <channel>) ─────
  feed_title                  text,
  feed_description            text,
  feed_link                   text,                      -- publisher's homepage URL from RSS <link>
  feed_image_url              text,                      -- publisher logo from RSS <image><url>
  feed_generator              text,
  feed_ttl_minutes            integer,
  feed_copyright              text,
  feed_last_build_date        timestamptz,

  -- ── publisher-site metadata (from homepage HTML) ─────
  favicon_url                 text,
  site_name                   text,
  site_description            text,
  site_image_url              text,
  theme_color                 text,
  twitter_handle              text,

  -- ── social profiles (from JSON-LD Organization.sameAs / meta tags) ─────
  social_twitter              text,
  social_instagram            text,
  social_facebook             text,
  social_tiktok               text,
  social_youtube              text,
  social_pinterest            text,
  social_linkedin             text,
  social_threads              text,
  social_other                jsonb,

  -- ── extras ─────
  apple_touch_icon_url        text,
  webmanifest_url             text,
  sitemap_url                 text,
  autodiscovered_feeds        jsonb,

  -- ── operational state (replaces .scraper_state.sqlite) ─────
  etag                        text,
  last_modified               text,
  last_fetched_at             timestamptz,
  last_successful_fetch_at    timestamptz,
  last_304_at                 timestamptz,
  last_article_published_at   timestamptz,
  last_article_ingested_at    timestamptz,
  consecutive_failures        integer     NOT NULL DEFAULT 0,
  last_error                  text,
  total_articles              integer     NOT NULL DEFAULT 0,

  -- ── flags ─────
  is_active                   boolean     NOT NULL DEFAULT true,
  auto_disabled               boolean     NOT NULL DEFAULT false,
  disabled_reason             text,

  -- ── audit ─────
  notes                       text,
  created_at                  timestamptz NOT NULL DEFAULT now(),
  updated_at                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fs_feeds_domain        ON fs_feeds (domain);
CREATE INDEX IF NOT EXISTS idx_fs_feeds_category      ON fs_feeds (category);
CREATE INDEX IF NOT EXISTS idx_fs_feeds_language      ON fs_feeds (language);
CREATE INDEX IF NOT EXISTS idx_fs_feeds_country       ON fs_feeds (country);
CREATE INDEX IF NOT EXISTS idx_fs_feeds_is_active     ON fs_feeds (is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_fs_feeds_last_article  ON fs_feeds (last_article_published_at DESC NULLS LAST);


-- 2. auto-bump updated_at ------------------------------------------------------
CREATE OR REPLACE FUNCTION fs_feeds_touch_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_fs_feeds_updated_at ON fs_feeds;
CREATE TRIGGER trg_fs_feeds_updated_at
  BEFORE UPDATE ON fs_feeds
  FOR EACH ROW EXECUTE FUNCTION fs_feeds_touch_updated_at();


-- 3. Many-to-many: feeds ↔ brands ---------------------------------------------
CREATE TABLE IF NOT EXISTS fs_feed_brands (
  feed_id    uuid NOT NULL REFERENCES fs_feeds  (id) ON DELETE CASCADE,
  brand_id   uuid NOT NULL REFERENCES fs_brands (id) ON DELETE CASCADE,
  relation   text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (feed_id, brand_id)
);

CREATE INDEX IF NOT EXISTS idx_fs_feed_brands_brand ON fs_feed_brands (brand_id);


-- 4. Many-to-many: feeds ↔ stores ---------------------------------------------
CREATE TABLE IF NOT EXISTS fs_feed_stores (
  feed_id    uuid NOT NULL REFERENCES fs_feeds  (id) ON DELETE CASCADE,
  store_id   uuid NOT NULL REFERENCES fs_stores (id) ON DELETE CASCADE,
  relation   text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (feed_id, store_id)
);

CREATE INDEX IF NOT EXISTS idx_fs_feed_stores_store ON fs_feed_stores (store_id);


-- 5. Link every fs_news row back to its source feed ---------------------------
ALTER TABLE fs_news
  ADD COLUMN IF NOT EXISTS feed_id uuid REFERENCES fs_feeds (id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_fs_news_feed_id ON fs_news (feed_id);


-- 6. Auto-rollup: keep fs_feeds.last_article_* + total_articles fresh ---------
CREATE OR REPLACE FUNCTION fs_news_update_feed_stats() RETURNS trigger AS $$
BEGIN
  IF NEW.feed_id IS NULL THEN
    RETURN NEW;
  END IF;

  UPDATE fs_feeds SET
    last_article_published_at = GREATEST(
      coalesce(last_article_published_at, 'epoch'::timestamptz),
      coalesce(NEW.published_at,           'epoch'::timestamptz)
    ),
    last_article_ingested_at  = GREATEST(
      coalesce(last_article_ingested_at,   'epoch'::timestamptz),
      coalesce(NEW.fetched_at,             now())
    ),
    total_articles            = total_articles + CASE WHEN TG_OP = 'INSERT' THEN 1 ELSE 0 END
  WHERE id = NEW.feed_id;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_fs_news_feed_stats ON fs_news;
CREATE TRIGGER trg_fs_news_feed_stats
  AFTER INSERT OR UPDATE OF published_at, fetched_at, feed_id ON fs_news
  FOR EACH ROW EXECUTE FUNCTION fs_news_update_feed_stats();


-- 7. Health view ---------------------------------------------------------------
CREATE OR REPLACE VIEW v_fs_feeds_health AS
SELECT
  f.id,
  f.name,
  f.domain,
  f.is_active,
  f.auto_disabled,
  f.consecutive_failures,
  f.total_articles,
  f.last_successful_fetch_at,
  f.last_article_published_at,
  now() - f.last_article_published_at AS age_of_newest_article,
  f.last_error
FROM fs_feeds f
ORDER BY f.consecutive_failures DESC, f.last_article_published_at ASC NULLS FIRST;
