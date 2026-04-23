# Fashion News Scraper

RSS/Atom ingest for the `fs_news` table in Supabase. Polls ~60 fashion publishers worldwide, extracts articles, and upserts them.

## Quick start

```bash
cd fashion-scraper
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# edit .env and paste your SUPABASE_SERVICE_ROLE_KEY

# kick the tyres — no DB writes
python fashion_scraper.py --dry-run --limit 3

# probe every feed URL — no ingest, no state changes
python fashion_scraper.py --verify-feeds

# single publisher
python fashion_scraper.py --feed hypebeast.com --limit 5

# full run (English-only by default)
python fashion_scraper.py

# include non-English feeds (fr/it/de/es/ja) — default is English-only
python fashion_scraper.py --all-languages

# operational helpers
python fashion_scraper.py --list-disabled       # show feeds with recorded failures
python fashion_scraper.py --reset-failures      # clear all failure counters
python fashion_scraper.py --reset-failures --feed vogue.it
python fashion_scraper.py --include-disabled    # force-probe auto-disabled feeds

# DB-backed workflows (require the fs_feeds migration applied + seeded)
python fashion_scraper.py --feeds-from-db            # read feed list from fs_feeds instead of feeds.yaml
python fashion_scraper.py --refresh-feed-metadata    # enrich fs_feeds with channel metadata, favicon, socials
python fashion_scraper.py --refresh-feed-metadata --feed hypebeast
```

### DB-backed feed registry

If the `fs_feeds` migration is applied and seeded from `feeds.yaml`:

- Every ingested row gets its `feed_id` FK set automatically (both in yaml and DB modes), so `fs_news JOIN fs_feeds` queries work without manual linking.
- After each run, per-feed state is synced into `fs_feeds`: `last_fetched_at`, `last_successful_fetch_at`, `last_304_at`, `etag`, `last_modified`, `consecutive_failures`, `last_error`, `auto_disabled`, `disabled_reason`.
- The `trg_fs_news_feed_stats` trigger keeps `total_articles`, `last_article_published_at`, and `last_article_ingested_at` fresh on every insert.
- `--refresh-feed-metadata` walks every `fs_feeds` row and fills in: channel `feed_title` / `feed_description` / `feed_link` / `feed_image_url` / `feed_generator` / `feed_ttl_minutes` / `feed_last_build_date`, plus homepage `favicon_url` / `apple_touch_icon_url` / `webmanifest_url` / `sitemap_url` / `site_name` / `site_description` / `site_image_url` / `theme_color` / `twitter_handle`, plus JSON-LD `Organization.sameAs` socials (Twitter, Instagram, Facebook, TikTok, YouTube, Pinterest, LinkedIn, Threads — unknown platforms land in `social_other`).

The first run writes `.scraper_state.sqlite` locally to track `ETag` / `Last-Modified` per feed. Subsequent runs skip unchanged feeds via `304 Not Modified`. The same sqlite file also tracks consecutive failures per feed — after 5 in a row, the feed is auto-disabled until you `--reset-failures` it.

## How it maps RSS → `fs_news`

| Column | Source |
|---|---|
| `title` | `entry.title` |
| `slug` | `{domain}-{slug-of-title}-{8-hex-hash-of-url}` — deterministic, globally unique |
| `excerpt` | cleaned `entry.summary`, truncated to 500 chars |
| `body` | full-text extracted from article URL via trafilatura (skipped for paywalled feeds) |
| `cover_image_url` | `media:content` / `media:thumbnail` / enclosure / first `<img>` in summary; falls back to `og:image` / `twitter:image` from fetched article HTML |
| `author_name` | `entry.author` |
| `category` | from `feeds.yaml` (`industry_news`, `editorial`, `streetwear_drops`, `events`, `trade`, `jobs`) |
| `tags` | `entry.tags[*].term` |
| `published_at` | `entry.published_parsed` → ISO-8601 UTC |
| `fetched_at` | now (UTC) |
| `source` | `'rss'` |
| `source_external_id` | `entry.id` / `entry.guid` / `entry.link` (fallback: SHA-1 of link) |
| `source_domain` | feed config `domain` — stable dedupe key |
| `source_url` | `entry.link` |
| `source_feed_url` | feed config `url` |
| `language`, `country`, `paywalled` | feed config |
| `raw_payload` | full RSS entry as JSON — for reprocessing without re-scraping |
| `is_published` | `false` — you review before flipping to true |
| `status` | `'imported'` |

## Safety defaults

- `is_published = false` on every row. Nothing goes live until you explicitly publish. To flip published after review:
  ```sql
  UPDATE fs_news SET is_published = true, status = 'published'
  WHERE status = 'imported' AND created_at >= now() - interval '24 hours';
  ```
- Paywalled sources (`BoF`, `WWD`, `Drapers`, `TextilWirtschaft`) get `body = NULL` — only title/summary/link are stored. This keeps you out of copyright trouble.
- Dedupe is enforced at the DB level via `UNIQUE (source_domain, source_external_id)`. Re-running the scraper does NOT create duplicates; it upserts.
- `slug` uses a URL-hash suffix, so two publishers covering the same story never collide on the `slug` unique constraint.

## Adding / removing feeds

Edit `feeds.yaml`. Each feed needs:

```yaml
- url: https://example.com/feed
  name: Example Publisher
  domain: example.com
  category: editorial        # industry_news | editorial | streetwear_drops | events | jobs | trade
  country: US                # ISO alpha-2, or GLOBAL
  language: en               # ISO 639-1
  paywalled: false           # true → skip full-text extraction
  # extract_body: false      # optional override
  # user_agent: browser      # optional — "browser" for sites that 403 on bot UAs
```

To retire a feed, comment it out. Existing rows from that publisher remain.

## Scheduling

### GitHub Actions (recommended)

Two workflows are checked in under `.github/workflows/`:

| Workflow | Schedule (UTC) | Script | Purpose |
|---|---|---|---|
| `discover.yml` | `0 19 * * *` (daily) | `discover_feeds.py --only both` | Walk `fs_brands` + `fs_stores` for new RSS feeds, register into `fs_feeds` + `fs_feed_{brands,stores}`. Also records blog-only sites into `fs_discovered_blogs`. |
| `ingest.yml` | `5 */2 * * *` (every 2h) | `fashion_scraper.py --feeds-from-db` | Poll every active feed and upsert articles into `fs_news`. |

Both have `workflow_dispatch` triggers so you can run them manually from the Actions tab with parameter overrides.

Both need two repo secrets set:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

Set them via the UI at `Settings → Secrets and variables → Actions`, or via `gh`:
```bash
echo -n "https://YOUR-REF.supabase.co" | gh secret set SUPABASE_URL
echo -n "YOUR_SERVICE_ROLE_KEY"         | gh secret set SUPABASE_SERVICE_ROLE_KEY
```

**State persistence**: `ingest.yml` uses `actions/cache@v4` to persist `.scraper_state.sqlite` (ETag + consecutive-failure counter) across runs. Without this, every scheduled run re-fetches every feed fully instead of getting 304s.

### Local cron (alternative)

Run every 30 minutes:

```cron
*/30 * * * *  cd /path/to/fashion-scraper && /path/to/.venv/bin/python fashion_scraper.py >> scraper.log 2>&1
```

Or on systemd:

```ini
# /etc/systemd/system/fashion-scraper.timer
[Timer]
OnCalendar=*:0/30
Persistent=true

[Install]
WantedBy=timers.target
```

## Troubleshooting

- **A feed keeps 404'ing** → run `--verify-feeds` to see the current status of every URL without ingesting. After 5 consecutive failures a feed auto-disables; inspect with `--list-disabled` and clear with `--reset-failures`.
- **403 on a specific publisher** → some sites block bot-shaped UAs. Add `user_agent: browser` to the feed's entry in `feeds.yaml`.
- **`bozo` warnings** → feedparser recovered from a malformed feed. Usually fine. If items are missing, inspect the raw XML.
- **`supabase upsert` errors about `on_conflict`** → check the unique index `uq_news_source_external` still exists on `(source_domain, source_external_id)`.
- **`supabase upsert` errors about duplicate `slug` / `source_url`** → usually two feeds emitting the same canonical URL (e.g. a UK site 301'ing to the US feed). The scraper now catches these and logs `skip conflicting row`; investigate by running that feed in isolation and checking DB for pre-existing rows with the same `source_url`.
- **Nothing new after re-run** → that's correct, the cache saw 304s. Delete `.scraper_state.sqlite` to force a full re-poll.
- **Rate-limited / bot-blocked** (SSENSE, GOAT, Cloudflare sites) → these don't have RSS anyway; they'd need a headless-browser pipeline which this scraper deliberately doesn't implement.

## Operational notes

- Global HTTP concurrency: 6 (`MAX_CONCURRENT` in the script)
- Full-text fetches capped at 25 per feed per run
- Body text truncated to 60 KB
- Upserts are batched: one Supabase call per feed (chunked at `UPSERT_CHUNK_SIZE = 100` for feeds that emit huge entry counts)
- English-only by default; `--all-languages` opts back into the multi-lingual feeds
- Transient network / 5xx errors are retried up to 3 times with exponential backoff (`MAX_RETRIES`, `BACKOFF_BASE`). 4xx are not retried.
- Feeds with 5 consecutive failures are auto-disabled (`FAIL_DISABLE_THRESHOLD`); they reset automatically on the next successful fetch.
- `trafilatura.extract` runs in a worker thread so the event loop stays responsive.
- Default `User-Agent` is `YunikonFashionBot`. Per-feed `user_agent: browser` override is available for sites that block bot UAs.
- Always honors feeds that emit 304 responses

## What this does NOT do

- Headless scraping for JS-rendered retailer editorials (SSENSE, GOAT, Farfetch, Net-a-Porter) — use a separate Playwright pipeline
- Brand press releases from non-RSS sources — add via `prnewswire.com` / `businesswire.com` wire feeds
- Job listings — BoF Careers and FashionJobs don't expose RSS; add a separate scraper with their job boards' APIs
