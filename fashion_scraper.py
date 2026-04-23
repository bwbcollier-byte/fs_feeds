"""
Fashion news RSS scraper → Supabase `fs_news`.

Run:
    python fashion_scraper.py                 # all feeds
    python fashion_scraper.py --feed wwd.com  # single publisher (domain match)
    python fashion_scraper.py --dry-run       # fetch + parse, no writes
    python fashion_scraper.py --limit 5       # max 5 items per feed (for testing)
    python fashion_scraper.py --verify-feeds  # probe URLs + exit, no ingest
    python fashion_scraper.py --include-disabled  # ignore the auto-disable list
    python fashion_scraper.py --all-languages # include non-English feeds
                                              # (English-only is the default)
    python fashion_scraper.py --feeds-from-db # read feed list from fs_feeds
    python fashion_scraper.py --refresh-feed-metadata
        # walk fs_feeds, populate channel metadata + favicon + JSON-LD socials

Design notes
------------
- Idempotent: upserts on (source_domain, source_external_id). Safe to re-run.
- Conditional GETs: stores ETag / Last-Modified per feed in `.scraper_state.sqlite`.
- Full-text extraction: uses trafilatura for non-paywalled sources.
- Polite: global semaphore caps concurrent HTTP to MAX_CONCURRENT.
- Never blocks on one broken feed — each feed is try/except isolated.
- Cover images: tries media:content / enclosure / <img> in feed summary first,
  then falls back to og:image / twitter:image from the fetched article HTML
  so publishers with image-less RSS still get thumbnails.
- Batched upserts: each feed's rows are sent in a single Supabase call
  (chunked at UPSERT_CHUNK_SIZE for very large feeds).
- English-only by default. Pass --all-languages to include the fr/it/de/es/ja
  feeds in feeds.yaml.
- Retries transient network errors (connection, timeout, 5xx) with exponential
  backoff. Does NOT retry 4xx (those are permanent config problems).
- Auto-disable: a feed that fails FAIL_DISABLE_THRESHOLD times in a row is
  skipped on subsequent runs. Override with --include-disabled or reset with
  --reset-failures.
- Robust upsert: slug / source_url uniques are caught and fall back to a
  per-row insert that skips conflicts, so one bad row never loses a whole feed.

Columns written to fs_news:
    title, slug, body, excerpt, cover_image_url,
    author_name, category, tags,
    published_at, fetched_at,
    source='rss', source_external_id, source_domain, source_url, source_feed_url,
    language, country, paywalled, raw_payload,
    is_published=False, status='imported', is_active=True
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import feedparser  # type: ignore
import httpx
import trafilatura  # type: ignore
import yaml  # type: ignore
from dotenv import load_dotenv
from supabase import Client, create_client  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).parent
FEEDS_FILE = ROOT / "feeds.yaml"
# State DB can be overridden; default to a local writable path so the scraper
# still works when ROOT is on a filesystem that doesn't support SQLite locking
# (NFS/FUSE mounts, some container volumes).
STATE_DB = Path(os.environ.get("SCRAPER_STATE_DB", str(ROOT / ".scraper_state.sqlite")))

USER_AGENT = "YunikonFashionBot/1.0 (+https://yunikon.com/bot; contact=labs@yunikon.com)"
# Some publishers 403 anything that looks bot-shaped. Feeds with
# `user_agent: browser` in feeds.yaml send this UA instead.
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 30.0
MAX_CONCURRENT = 6             # global concurrent HTTP requests
MAX_BODY_FETCH_PER_FEED = 25   # cap full-text fetches per feed per run
MAX_BODY_CHARS = 60_000        # truncate extracted body
EXCERPT_CHARS = 500
MAX_RETRIES = 3                # transient-error retries on feed fetch
BACKOFF_BASE = 1.5             # seconds, multiplied by 2**attempt
FAIL_DISABLE_THRESHOLD = 5     # consecutive failures before auto-disable
# Rows per Supabase upsert call. We already batch all rows from a single
# feed into one call; this cap only kicks in for feeds that produce huge
# entry counts (>100) so PostgREST's request-size limit isn't a cliff.
UPSERT_CHUNK_SIZE = 100

load_dotenv(ROOT / ".env")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")


# ──────────────────────────────────────────────────────────────────────────────
# Data shapes
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class FeedConfig:
    url: str
    name: str
    domain: str
    category: str
    country: str
    language: str
    paywalled: bool = False
    extract_body: bool | None = None  # None → default (not paywalled)
    user_agent: str | None = None     # "browser" or a literal UA; None → USER_AGENT
    # fs_feeds.id when the feed was loaded from the DB (or backfilled
    # after load from the yaml). Flows into fs_news.feed_id and is the
    # anchor for syncing per-feed state back to fs_feeds.
    id: str | None = None

    def effective_ua(self) -> str:
        if not self.user_agent:
            return USER_AGENT
        if self.user_agent.lower() == "browser":
            return BROWSER_UA
        return self.user_agent


@dataclass
class ArticleRow:
    title: str
    slug: str
    body: str | None
    excerpt: str | None
    cover_image_url: str | None
    author_name: str | None
    category: str
    tags: list[str]
    published_at: str | None          # ISO 8601
    fetched_at: str
    source: str
    source_external_id: str
    source_domain: str
    source_url: str
    source_feed_url: str
    language: str
    country: str
    paywalled: bool
    raw_payload: dict[str, Any]
    is_published: bool = False
    status: str = "imported"
    is_active: bool = True
    # FK to fs_feeds.id. NULL when fs_feeds hasn't been seeded yet or
    # the feed url has no matching row; harmless in that case.
    feed_id: str | None = None


@dataclass
class FeedResult:
    feed: FeedConfig
    fetched: int = 0
    new: int = 0
    updated: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────────────
# Local state (ETag / Last-Modified cache)
# ──────────────────────────────────────────────────────────────────────────────

def _state_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(STATE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS feed_cache (
            feed_url TEXT PRIMARY KEY,
            etag TEXT,
            last_modified TEXT,
            last_fetched TEXT
        )
    """)
    # Failure tracking is kept in a separate table so resetting it (on user
    # request) doesn't also blow away the ETag cache.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS feed_failures (
            feed_url TEXT PRIMARY KEY,
            consecutive_failures INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            last_failed_at TEXT
        )
    """)
    return conn


def get_cache(feed_url: str) -> tuple[str | None, str | None]:
    with _state_conn() as c:
        row = c.execute(
            "SELECT etag, last_modified FROM feed_cache WHERE feed_url=?",
            (feed_url,),
        ).fetchone()
    return (row[0], row[1]) if row else (None, None)


def set_cache(feed_url: str, etag: str | None, last_modified: str | None) -> None:
    with _state_conn() as c:
        c.execute(
            """
            INSERT INTO feed_cache(feed_url, etag, last_modified, last_fetched)
            VALUES(?,?,?,?)
            ON CONFLICT(feed_url) DO UPDATE SET
                etag=excluded.etag,
                last_modified=excluded.last_modified,
                last_fetched=excluded.last_fetched
            """,
            (feed_url, etag, last_modified, datetime.now(timezone.utc).isoformat()),
        )


def get_failure_count(feed_url: str) -> int:
    with _state_conn() as c:
        row = c.execute(
            "SELECT consecutive_failures FROM feed_failures WHERE feed_url=?",
            (feed_url,),
        ).fetchone()
    return row[0] if row else 0


def record_failure(feed_url: str, error: str) -> int:
    """Increment the consecutive-failure counter. Returns the new count."""
    with _state_conn() as c:
        c.execute(
            """
            INSERT INTO feed_failures(feed_url, consecutive_failures, last_error, last_failed_at)
            VALUES(?, 1, ?, ?)
            ON CONFLICT(feed_url) DO UPDATE SET
                consecutive_failures = feed_failures.consecutive_failures + 1,
                last_error = excluded.last_error,
                last_failed_at = excluded.last_failed_at
            """,
            (feed_url, error[:500], datetime.now(timezone.utc).isoformat()),
        )
        row = c.execute(
            "SELECT consecutive_failures FROM feed_failures WHERE feed_url=?",
            (feed_url,),
        ).fetchone()
    return row[0] if row else 1


def reset_failure(feed_url: str) -> None:
    with _state_conn() as c:
        c.execute("DELETE FROM feed_failures WHERE feed_url=?", (feed_url,))


def list_failures() -> list[tuple[str, int, str | None, str | None]]:
    with _state_conn() as c:
        return c.execute(
            "SELECT feed_url, consecutive_failures, last_error, last_failed_at "
            "FROM feed_failures ORDER BY consecutive_failures DESC"
        ).fetchall()


# ──────────────────────────────────────────────────────────────────────────────
# Parsing helpers
# ──────────────────────────────────────────────────────────────────────────────

_SLUG_RX = re.compile(r"[^a-z0-9]+")
_HTML_RX = re.compile(r"<[^>]+>")


def slugify(text: str, max_len: int = 80) -> str:
    text = (text or "").lower()
    text = _SLUG_RX.sub("-", text).strip("-")
    return text[:max_len] or "untitled"


def html_to_text(s: str | None) -> str:
    if not s:
        return ""
    return _HTML_RX.sub("", s).replace("&nbsp;", " ").strip()


def truncate(s: str, n: int) -> str:
    return s if len(s) <= n else s[: n - 1].rstrip() + "…"


def _first(entry: dict, *keys: str) -> Any:
    for k in keys:
        v = entry.get(k)
        if v:
            return v
    return None


def parse_published(entry: dict) -> str | None:
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        t = entry.get(attr)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc).isoformat()
            except (TypeError, ValueError):
                continue
    return None


def extract_image(entry: dict) -> str | None:
    # feedparser exposes media_content / media_thumbnail / enclosures
    for key in ("media_content", "media_thumbnail"):
        items = entry.get(key) or []
        for item in items:
            url = item.get("url")
            if url:
                return url
    for enc in entry.get("enclosures", []) or []:
        url = enc.get("url") or enc.get("href")
        if url and (enc.get("type", "").startswith("image") or url.endswith((".jpg", ".jpeg", ".png", ".webp"))):
            return url
    # <img> in summary/content fallback
    for field_name in ("summary", "content"):
        raw = entry.get(field_name)
        if isinstance(raw, list):
            raw = raw[0].get("value") if raw else ""
        m = re.search(r'<img[^>]+src=["\']([^"\']+)', raw or "")
        if m:
            return m.group(1)
    return None


def extract_tags(entry: dict) -> list[str]:
    tags = entry.get("tags") or []
    out: list[str] = []
    for t in tags:
        term = (t.get("term") or t.get("label") or "").strip()
        if term:
            out.append(term)
    # dedupe preserving order
    seen = set()
    return [t for t in out if not (t in seen or seen.add(t))]


def external_id(entry: dict, fallback_url: str) -> str:
    for key in ("id", "guid", "link"):
        v = entry.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return hashlib.sha1(fallback_url.encode("utf-8")).hexdigest()


def make_slug(title: str, source_url: str, domain: str) -> str:
    """
    Deterministic, globally-unique slug:
      {domain-no-tld}-{title-slug}-{8-hex-of-url}
    Guarantees no collision across publishers covering the same headline.
    """
    short_domain = domain.split(".")[0][:20]
    url_hash = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:8]
    return f"{short_domain}-{slugify(title, 60)}-{url_hash}"


# ──────────────────────────────────────────────────────────────────────────────
# Full-text extraction
# ──────────────────────────────────────────────────────────────────────────────

_OG_IMAGE_RX = re.compile(
    r'<meta[^>]+(?:property|name)=["\'](?:og:image(?::secure_url)?|twitter:image(?::src)?)["\'][^>]*?content=["\']([^"\']+)',
    re.IGNORECASE,
)
_OG_IMAGE_REV_RX = re.compile(
    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]*?(?:property|name)=["\'](?:og:image(?::secure_url)?|twitter:image(?::src)?)["\']',
    re.IGNORECASE,
)
_LINK_IMAGE_SRC_RX = re.compile(
    r'<link[^>]+rel=["\']image_src["\'][^>]*?href=["\']([^"\']+)',
    re.IGNORECASE,
)


def extract_image_from_html(html: str) -> str | None:
    """Pull a cover image from a fetched article HTML body.

    Priority: og:image → og:image:secure_url → twitter:image → link rel=image_src.
    Handles both attribute orderings (content first or property first).
    """
    if not html:
        return None
    for rx in (_OG_IMAGE_RX, _OG_IMAGE_REV_RX, _LINK_IMAGE_SRC_RX):
        m = rx.search(html)
        if m:
            return m.group(1)
    return None


async def fetch_article_body(
    client: httpx.AsyncClient, url: str
) -> tuple[str | None, str | None]:
    """Returns (body_text, cover_image_url).

    Both come from the same HTTP fetch — no extra request for the image.
    Either or both may be None.
    """
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        if r.status_code != 200 or not r.text:
            return None, None
        html = r.text
        # Grab the OG image off the raw HTML before trafilatura strips it.
        image = extract_image_from_html(html)
        # trafilatura.extract is CPU-bound; offload so it doesn't block
        # the event loop while other feed fetches are pending.
        body = await asyncio.to_thread(
            trafilatura.extract,
            html,
            include_comments=False,
            include_tables=False,
            favor_precision=True,
        )
        body = truncate(body, MAX_BODY_CHARS) if body else None
        return body, image
    except Exception as e:  # noqa: BLE001
        log.debug("body fetch failed for %s: %s", url, e)
        return None, None


# ──────────────────────────────────────────────────────────────────────────────
# Feed fetching
# ──────────────────────────────────────────────────────────────────────────────

async def fetch_feed(client: httpx.AsyncClient, feed: FeedConfig) -> tuple[bytes | None, str | None, str | None]:
    """Returns (body, etag, last_modified). body is None if 304.

    Retries transient errors (connection errors, timeouts, 5xx) with
    exponential backoff. Does NOT retry 4xx — those are permanent
    (bad URL, blocked, gone). Raises RuntimeError on final failure.
    """
    etag, last_modified = get_cache(feed.url)
    headers = {
        "User-Agent": feed.effective_ua(),
        "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.5",
    }
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            r = await client.get(
                feed.url,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
                follow_redirects=True,
            )
        except (httpx.TimeoutException, httpx.TransportError, httpx.NetworkError) as e:
            last_err = e
            delay = BACKOFF_BASE * (2 ** attempt)
            log.warning("[%s] transient error (attempt %d/%d): %s — retrying in %.1fs",
                        feed.domain, attempt + 1, MAX_RETRIES, e, delay)
            await asyncio.sleep(delay)
            continue
        except httpx.HTTPError as e:
            # Other HTTPError subclasses — treat as non-retryable.
            raise RuntimeError(f"network error: {e}") from e

        if r.status_code == 304:
            log.info("[%s] not modified (304)", feed.domain)
            return None, etag, last_modified
        if 500 <= r.status_code < 600 and attempt < MAX_RETRIES - 1:
            delay = BACKOFF_BASE * (2 ** attempt)
            log.warning("[%s] HTTP %d (attempt %d/%d) — retrying in %.1fs",
                        feed.domain, r.status_code, attempt + 1, MAX_RETRIES, delay)
            await asyncio.sleep(delay)
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code}")

        return r.content, r.headers.get("etag"), r.headers.get("last-modified")

    raise RuntimeError(f"network error after {MAX_RETRIES} retries: {last_err}")


def parse_entries(feed: FeedConfig, raw: bytes) -> list[dict]:
    parsed = feedparser.parse(raw)
    if parsed.bozo and parsed.bozo_exception:
        log.warning("[%s] feedparser warning: %s", feed.domain, parsed.bozo_exception)
    return [dict(e) for e in parsed.entries]


# ──────────────────────────────────────────────────────────────────────────────
# Row building + writing
# ──────────────────────────────────────────────────────────────────────────────

def build_row(
    feed: FeedConfig,
    entry: dict,
    body: str | None,
    fallback_image: str | None = None,
) -> ArticleRow | None:
    title = html_to_text(_first(entry, "title")) or ""
    link = _first(entry, "link") or ""
    if not title or not link:
        return None

    summary_raw = _first(entry, "summary", "description") or ""
    if isinstance(summary_raw, list):
        summary_raw = summary_raw[0].get("value", "") if summary_raw else ""
    excerpt = truncate(html_to_text(summary_raw), EXCERPT_CHARS) or None

    author = _first(entry, "author", "dc_creator") or None
    if isinstance(author, dict):
        author = author.get("name")

    # strip volatile fields before persisting raw_payload to keep it small
    raw = {k: v for k, v in entry.items() if k not in ("summary_detail", "content")}
    # coerce to JSON-serialisable
    raw = json.loads(json.dumps(raw, default=str))

    domain = (urlparse(link).netloc or feed.domain).lower().removeprefix("www.")

    # Prefer images the feed itself advertised (media:content etc.) — they're
    # the most authoritative for that entry. Fall back to anything pulled from
    # the article HTML (og:image, etc.) so publishers that don't emit images
    # in their RSS (fashionnetwork, highsnobiety, neo2, etc.) still get one.
    cover_image = extract_image(entry) or fallback_image

    return ArticleRow(
        title=title,
        slug=make_slug(title, link, domain),
        body=body,
        excerpt=excerpt,
        cover_image_url=cover_image,
        author_name=author,
        category=feed.category,
        tags=extract_tags(entry),
        published_at=parse_published(entry),
        fetched_at=datetime.now(timezone.utc).isoformat(),
        source="rss",
        source_external_id=external_id(entry, link),
        source_domain=feed.domain,  # use feed config domain for stable dedupe
        source_url=link,
        source_feed_url=feed.url,
        language=feed.language,
        country=feed.country,
        paywalled=feed.paywalled,
        raw_payload=raw,
        feed_id=feed.id,
    )


_UNIQUE_VIOLATION_HINTS = (
    "fs_news_slug_key",
    "uq_news_source_url",
    "duplicate key value",
    "unique constraint",
    "23505",  # Postgres unique_violation SQLSTATE
    "21000",  # cardinality_violation — two rows with same ON CONFLICT key in one batch
    "cannot affect row a second time",
)


def _is_unique_violation(err: Exception) -> bool:
    # supabase-py's APIError exposes .code; fall back to string match for
    # other exception shapes. PostgREST surfaces the Postgres SQLSTATE in
    # the message, so the string check catches it too.
    code = getattr(err, "code", None)
    if code == "23505":
        return True
    msg = str(err).lower()
    return any(h.lower() in msg for h in _UNIQUE_VIOLATION_HINTS)


def _upsert_batch(client: Client, items: list[dict]) -> int:
    """One batched Supabase call. Returns the count of rows returned."""
    res = client.table("fs_news").upsert(
        items,
        on_conflict="source_domain,source_external_id",
    ).execute()
    return len(res.data or [])


def upsert_rows(client: Client, rows: list[ArticleRow]) -> tuple[int, int]:
    """Upsert rows to fs_news with minimum API calls.

    Fast path: chunk the rows into batches of UPSERT_CHUNK_SIZE and fire
    one Supabase call per chunk (usually one call per feed).

    Slow path: if a batch hits a unique-constraint violation on slug or
    source_url (e.g. two feeds emitting the same canonical URL), fall
    back to per-row upserts for that chunk only, counting rows skipped
    instead of losing the whole feed.

    Returns (written, skipped).
    """
    if not rows:
        return 0, 0

    # Within-batch dedup by the ON CONFLICT key. Postgres rejects the entire
    # batch (SQLSTATE 21000 "ON CONFLICT DO UPDATE command cannot affect row
    # a second time") if any two rows in a single upsert share
    # (source_domain, source_external_id). Collapse duplicates here so the
    # batch is always legal. Last occurrence wins — typically the most recent
    # fetch of the same URL.
    by_key: dict[tuple[str, str], ArticleRow] = {}
    for r in rows:
        by_key[(r.source_domain, r.source_external_id)] = r
    deduped = list(by_key.values())
    dropped_in_batch = len(rows) - len(deduped)
    if dropped_in_batch:
        log.info(
            "within-batch dedup: collapsed %d duplicate (source_domain, source_external_id) pairs",
            dropped_in_batch,
        )
    rows = deduped

    payload = [asdict(r) for r in rows]
    written = 0
    skipped = dropped_in_batch

    for chunk_start in range(0, len(payload), UPSERT_CHUNK_SIZE):
        chunk = payload[chunk_start : chunk_start + UPSERT_CHUNK_SIZE]
        chunk_rows = rows[chunk_start : chunk_start + UPSERT_CHUNK_SIZE]
        try:
            written += _upsert_batch(client, chunk)
            continue
        except Exception as e:  # noqa: BLE001
            if not _is_unique_violation(e):
                raise
            log.warning(
                "batch upsert hit a unique violation — falling back to per-row for %d rows: %s",
                len(chunk), e,
            )

        # Per-row fallback for this chunk only.
        for row, item in zip(chunk_rows, chunk):
            try:
                written += _upsert_batch(client, [item])
            except Exception as e:  # noqa: BLE001
                if _is_unique_violation(e):
                    log.info(
                        "skip conflicting row (%s, %s): %s",
                        row.source_domain, row.source_url, e,
                    )
                    skipped += 1
                    continue
                raise
    return written, skipped


# ──────────────────────────────────────────────────────────────────────────────
# fs_feeds registry: read + state sync
# ──────────────────────────────────────────────────────────────────────────────

def load_feed_id_map(sb: Client) -> dict[str, str]:
    """Return {feed_url: feed_id} for every row in fs_feeds. One call."""
    try:
        res = sb.table("fs_feeds").select("id,url").execute()
        return {r["url"]: r["id"] for r in (res.data or [])}
    except Exception as e:  # noqa: BLE001
        log.warning("could not load fs_feeds id map (feed_id will be NULL on rows): %s", e)
        return {}


def load_feeds_from_db(
    sb: Client,
    only: str | None,
    english_only: bool,
    include_disabled: bool,
) -> list[FeedConfig]:
    """Load FeedConfig list from fs_feeds. Respects is_active + auto_disabled."""
    q = sb.table("fs_feeds").select(
        "id,url,name,domain,category,country,language,"
        "paywalled,extract_body,user_agent,is_active,auto_disabled"
    )
    res = q.execute()
    rows = res.data or []

    feeds: list[FeedConfig] = []
    dropped_inactive = dropped_disabled = 0
    for r in rows:
        if not r.get("is_active", True):
            dropped_inactive += 1
            continue
        if r.get("auto_disabled") and not include_disabled:
            dropped_disabled += 1
            continue
        feeds.append(FeedConfig(
            id=r["id"],
            url=r["url"],
            name=r["name"],
            domain=r["domain"],
            category=r["category"],
            country=r["country"],
            language=r["language"],
            paywalled=bool(r.get("paywalled")),
            extract_body=r.get("extract_body"),
            user_agent=r.get("user_agent"),
        ))

    if only:
        feeds = [f for f in feeds if only in f.domain or only in f.url]
    if english_only:
        non_en = [f.name for f in feeds if f.language != "en"]
        feeds = [f for f in feeds if f.language == "en"]
        if non_en:
            log.info("english-only filter dropped %d non-en feeds from DB", len(non_en))
    if dropped_inactive:
        log.info("skipped %d inactive feeds in fs_feeds", dropped_inactive)
    if dropped_disabled:
        log.info("skipped %d auto-disabled feeds (use --include-disabled to probe)",
                 dropped_disabled)
    return feeds


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sync_feed_success(
    sb: Client, feed_id: str, etag: str | None, last_modified: str | None
) -> None:
    """Called after a feed fetched 200 OK. Clears failure state, stores ETag."""
    now = _now_iso()
    try:
        sb.table("fs_feeds").update({
            "last_fetched_at": now,
            "last_successful_fetch_at": now,
            "etag": etag,
            "last_modified": last_modified,
            "consecutive_failures": 0,
            "last_error": None,
            "auto_disabled": False,
            "disabled_reason": None,
        }).eq("id", feed_id).execute()
    except Exception as e:  # noqa: BLE001
        log.debug("sync_feed_success failed for %s: %s", feed_id, e)


def sync_feed_not_modified(sb: Client, feed_id: str) -> None:
    """Called after a feed 304'd. 304 is a successful response — clear failures."""
    now = _now_iso()
    try:
        sb.table("fs_feeds").update({
            "last_fetched_at": now,
            "last_304_at": now,
            "last_successful_fetch_at": now,
            "consecutive_failures": 0,
            "last_error": None,
        }).eq("id", feed_id).execute()
    except Exception as e:  # noqa: BLE001
        log.debug("sync_feed_not_modified failed for %s: %s", feed_id, e)


def sync_feed_failure(
    sb: Client, feed_id: str, error: str, new_count: int
) -> None:
    """Called after a feed errored. Bumps counters, sets auto_disabled at threshold."""
    updates: dict[str, Any] = {
        "last_fetched_at": _now_iso(),
        "consecutive_failures": new_count,
        "last_error": error[:500],
    }
    if new_count >= FAIL_DISABLE_THRESHOLD:
        updates["auto_disabled"] = True
        updates["disabled_reason"] = f"{new_count} consecutive failures: {error[:200]}"
    try:
        sb.table("fs_feeds").update(updates).eq("id", feed_id).execute()
    except Exception as e:  # noqa: BLE001
        log.debug("sync_feed_failure failed for %s: %s", feed_id, e)


# ──────────────────────────────────────────────────────────────────────────────
# Homepage metadata extraction (for --refresh-feed-metadata)
# ──────────────────────────────────────────────────────────────────────────────

_SOCIAL_HOSTS: dict[str, str] = {
    "twitter.com": "twitter", "x.com": "twitter",
    "instagram.com": "instagram",
    "facebook.com": "facebook", "fb.com": "facebook",
    "tiktok.com": "tiktok",
    "youtube.com": "youtube", "youtu.be": "youtube",
    "pinterest.com": "pinterest", "pinterest.co.uk": "pinterest", "pinterest.de": "pinterest",
    "linkedin.com": "linkedin",
    "threads.net": "threads", "threads.com": "threads",
}


def _social_platform_from_url(url: str) -> str | None:
    try:
        host = urlparse(url).netloc.lower().removeprefix("www.")
    except Exception:  # noqa: BLE001
        return None
    return _SOCIAL_HOSTS.get(host)


_JSONLD_RX = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.+?)</script>',
    re.IGNORECASE | re.DOTALL,
)


def _extract_jsonld_social(html: str) -> tuple[dict[str, str], list[str]]:
    """Scan <script type=application/ld+json> for Organization.sameAs URLs.

    Returns (platform_to_url, other_urls). `platform_to_url` keys are
    short names ('twitter', 'instagram', ...); `other_urls` are URLs
    whose host didn't match a known platform.
    """
    matched: dict[str, str] = {}
    others: list[str] = []

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            t = obj.get("@type", "")
            if isinstance(t, list):
                t = " ".join(str(x) for x in t)
            is_org = any(k in t for k in ("Organization", "NewsMediaOrganization", "WebSite"))
            if is_org and isinstance(obj.get("sameAs"), list):
                for url in obj["sameAs"]:
                    if not isinstance(url, str):
                        continue
                    platform = _social_platform_from_url(url)
                    if platform:
                        matched.setdefault(platform, url)
                    else:
                        others.append(url)
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    for m in _JSONLD_RX.finditer(html):
        raw = m.group(1).strip()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        walk(data)
    return matched, others


# Attribute-order-agnostic meta/link regexes.
_META_NC_RX = re.compile(
    r'<meta[^>]+(?:name|property)=["\']([^"\']+)["\'][^>]*?content=["\']([^"\']+)',
    re.IGNORECASE,
)
_META_CN_RX = re.compile(
    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]*?(?:name|property)=["\']([^"\']+)',
    re.IGNORECASE,
)
_LINK_RH_RX = re.compile(
    r'<link[^>]+rel=["\']([^"\']+)["\'][^>]*?href=["\']([^"\']+)',
    re.IGNORECASE,
)
_LINK_HR_RX = re.compile(
    r'<link[^>]+href=["\']([^"\']+)["\'][^>]*?rel=["\']([^"\']+)',
    re.IGNORECASE,
)


def _absolutize(url: str | None, base: str) -> str | None:
    if not url:
        return url
    if url.startswith(("http://", "https://")):
        return url
    if url.startswith("//"):
        return "https:" + url
    try:
        pb = urlparse(base)
    except Exception:  # noqa: BLE001
        return url
    if url.startswith("/"):
        return f"{pb.scheme}://{pb.netloc}{url}"
    return f"{pb.scheme}://{pb.netloc}/{url}"


def extract_homepage_metadata(html: str, base_url: str) -> dict[str, Any]:
    """Parse og:*, theme-color, favicon variants, webmanifest, sitemap, and
    JSON-LD Organization.sameAs from homepage HTML. Returns a dict of
    only-populated fields ready to UPDATE into fs_feeds."""
    out: dict[str, Any] = {}

    metas: dict[str, str] = {}
    for m in _META_NC_RX.finditer(html):
        metas.setdefault(m.group(1).lower(), m.group(2))
    for m in _META_CN_RX.finditer(html):
        metas.setdefault(m.group(2).lower(), m.group(1))

    out["site_name"] = metas.get("og:site_name")
    out["site_description"] = metas.get("og:description") or metas.get("description")
    out["site_image_url"] = _absolutize(metas.get("og:image"), base_url)
    out["theme_color"] = metas.get("theme-color")

    tw = metas.get("twitter:site") or metas.get("twitter:creator")
    if tw and tw.startswith("@"):
        out["twitter_handle"] = tw

    links: dict[str, str] = {}
    for m in _LINK_RH_RX.finditer(html):
        for rel in m.group(1).lower().split():
            links.setdefault(rel, m.group(2))
    for m in _LINK_HR_RX.finditer(html):
        for rel in m.group(2).lower().split():
            links.setdefault(rel, m.group(1))

    out["favicon_url"]          = _absolutize(links.get("icon") or links.get("shortcut"), base_url)
    out["apple_touch_icon_url"] = _absolutize(links.get("apple-touch-icon"), base_url)
    out["webmanifest_url"]      = _absolutize(links.get("manifest"), base_url)
    out["sitemap_url"]           = _absolutize(links.get("sitemap"), base_url)

    socials, others = _extract_jsonld_social(html)
    for platform, url in socials.items():
        out[f"social_{platform}"] = url
    if others:
        out["social_other"] = others

    # strip Nones so .update() doesn't clobber existing values
    return {k: v for k, v in out.items() if v}


# ──────────────────────────────────────────────────────────────────────────────
# Per-feed driver
# ──────────────────────────────────────────────────────────────────────────────

async def process_feed(
    feed: FeedConfig,
    client: httpx.AsyncClient,
    sb: Client | None,
    args: argparse.Namespace,
    sem: asyncio.Semaphore,
) -> FeedResult:
    result = FeedResult(feed=feed)

    # Skip feeds that have tripped the auto-disable threshold, unless
    # the user passed --include-disabled or --reset-failures.
    if not getattr(args, "include_disabled", False):
        fails = get_failure_count(feed.url)
        if fails >= FAIL_DISABLE_THRESHOLD:
            msg = (f"auto-disabled after {fails} consecutive failures "
                   f"(use --include-disabled to force, or --reset-failures to clear)")
            result.errors.append(msg)
            log.warning("[%s] skipped: %s", feed.domain, msg)
            return result

    log.info("[%s] fetching %s", feed.domain, feed.url)

    try:
        async with sem:
            raw, etag, last_mod = await fetch_feed(client, feed)
    except Exception as e:  # noqa: BLE001
        new_count = record_failure(feed.url, str(e))
        result.errors.append(f"feed fetch: {e}")
        log.error("[%s] %s (consecutive failures: %d)", feed.domain, e, new_count)
        if sb is not None and feed.id:
            sync_feed_failure(sb, feed.id, str(e), new_count)
        return result

    if raw is None:
        reset_failure(feed.url)
        if sb is not None and feed.id:
            sync_feed_not_modified(sb, feed.id)
        return result  # 304

    entries = parse_entries(feed, raw)
    if args.limit:
        entries = entries[: args.limit]
    log.info("[%s] parsed %d entries", feed.domain, len(entries))

    rows: list[ArticleRow] = []
    wants_body = feed.extract_body if feed.extract_body is not None else (not feed.paywalled)
    body_budget = MAX_BODY_FETCH_PER_FEED

    for entry in entries:
        result.fetched += 1
        link = _first(entry, "link")
        if not link:
            result.skipped += 1
            continue

        body: str | None = None
        fallback_image: str | None = None

        # Always fetch the article HTML when:
        #   - we want the body (non-paywalled feeds), OR
        #   - we want the body-less og:image fallback (feed didn't include
        #     an image itself, paywalled or not)
        # og:image is a public social-sharing meta tag, not paywalled
        # content — safe to extract even for BoF/WWD/Drapers/etc.
        entry_image = extract_image(entry)
        need_article_fetch = wants_body or entry_image is None

        if need_article_fetch and body_budget > 0:
            async with sem:
                body, fallback_image = await fetch_article_body(client, link)
            body_budget -= 1
            if not wants_body:
                # Paywalled feed — keep the og:image, discard the body text.
                body = None

        row = build_row(feed, entry, body, fallback_image=fallback_image)
        if row is None:
            result.skipped += 1
            continue
        rows.append(row)

    if args.dry_run or sb is None:
        result.new = len(rows)
        log.info("[%s] dry-run: would upsert %d rows", feed.domain, len(rows))
        # IMPORTANT: do NOT persist the ETag cache or reset failures on
        # dry-runs — otherwise a subsequent real run gets 304s and silently
        # skips everything, and we'd lose visibility into flaky feeds.
        return result

    try:
        written, skipped = upsert_rows(sb, rows)
        result.new = written
        result.skipped += skipped
        if skipped:
            log.info("[%s] upserted %d rows (%d skipped on unique violation)",
                     feed.domain, written, skipped)
        else:
            log.info("[%s] upserted %d rows", feed.domain, written)
    except Exception as e:  # noqa: BLE001
        new_count = record_failure(feed.url, f"upsert: {e}")
        result.errors.append(f"supabase upsert: {e}")
        log.error("[%s] upsert failed: %s", feed.domain, e)
        if feed.id:
            sync_feed_failure(sb, feed.id, f"upsert: {e}", new_count)
        return result

    set_cache(feed.url, etag, last_mod)
    reset_failure(feed.url)
    if feed.id:
        sync_feed_success(sb, feed.id, etag, last_mod)
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

_FEED_CONFIG_FIELDS = {
    "url", "name", "domain", "category", "country", "language",
    "paywalled", "extract_body", "user_agent",
}


def load_feeds(
    path: Path,
    only: str | None,
    english_only: bool = True,
) -> list[FeedConfig]:
    with path.open() as f:
        data = yaml.safe_load(f) or {}
    feeds: list[FeedConfig] = []
    for i, d in enumerate(data.get("feeds", [])):
        unknown = set(d) - _FEED_CONFIG_FIELDS
        if unknown:
            log.warning("feeds.yaml entry #%d (%s): unknown keys ignored: %s",
                        i, d.get("name", "?"), sorted(unknown))
            d = {k: v for k, v in d.items() if k in _FEED_CONFIG_FIELDS}
        feeds.append(FeedConfig(**d))
    if only:
        feeds = [f for f in feeds if only in f.domain or only in f.url]
    if english_only:
        dropped = [f.name for f in feeds if f.language != "en"]
        feeds = [f for f in feeds if f.language == "en"]
        if dropped:
            log.info("english-only filter dropped %d non-en feeds: %s",
                     len(dropped), ", ".join(dropped))
    return feeds


def summarise(results: Iterable[FeedResult]) -> None:
    total_new = total_fetched = total_err = total_skipped = 0
    print("\n" + "─" * 88)
    print(f"{'Source':<36}{'Fetched':>10}{'Written':>10}{'Skipped':>10}{'Errors':>10}")
    print("─" * 88)
    # Sort: errors first (most urgent), then by written desc
    ordered = sorted(results, key=lambda r: (not r.errors, -r.new, r.feed.name))
    for r in ordered:
        print(f"{r.feed.name[:35]:<36}{r.fetched:>10}{r.new:>10}"
              f"{r.skipped:>10}{len(r.errors):>10}")
        total_new += r.new
        total_fetched += r.fetched
        total_err += len(r.errors)
        total_skipped += r.skipped
    print("─" * 88)
    print(f"{'TOTAL':<36}{total_fetched:>10}{total_new:>10}"
          f"{total_skipped:>10}{total_err:>10}")
    print("─" * 88 + "\n")

    # Surface per-feed errors at the end so they're easy to spot in logs.
    failing = [r for r in ordered if r.errors]
    if failing:
        print("FAILURES:")
        for r in failing:
            for err in r.errors:
                print(f"  [{r.feed.domain}] {err}")
        print()


async def verify_feeds(feeds: list[FeedConfig]) -> int:
    """Probe every configured feed URL and print status without ingesting.

    Honors per-feed `user_agent`. Does NOT touch the state cache or the
    failure counter — safe to run anytime.
    """
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    async def probe(feed: FeedConfig, client: httpx.AsyncClient) -> dict[str, Any]:
        async with sem:
            try:
                r = await client.get(
                    feed.url,
                    headers={"User-Agent": feed.effective_ua()},
                    timeout=REQUEST_TIMEOUT,
                    follow_redirects=True,
                )
                looks_xml = (
                    ("xml" in (r.headers.get("content-type") or "").lower())
                    or r.text[:200].lstrip().startswith("<?xml")
                    or r.text[:200].lstrip().startswith("<rss")
                    or r.text[:400].lstrip().startswith("<feed")
                )
                item_count = 0
                if looks_xml:
                    parsed = feedparser.parse(r.content)
                    item_count = len(parsed.entries)
                return {
                    "domain": feed.domain,
                    "name": feed.name,
                    "url": feed.url,
                    "status": r.status_code,
                    "final_url": str(r.url),
                    "content_type": r.headers.get("content-type", "?"),
                    "looks_xml": looks_xml,
                    "items": item_count,
                }
            except Exception as e:  # noqa: BLE001
                return {
                    "domain": feed.domain,
                    "name": feed.name,
                    "url": feed.url,
                    "status": 0,
                    "error": str(e),
                }

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, http2=False) as client:
        rows = await asyncio.gather(*(probe(f, client) for f in feeds))

    print("\n" + "─" * 110)
    print(f"{'Source':<36}{'Status':>8}{'Items':>8}  {'Final URL'}")
    print("─" * 110)
    ok = bad = 0
    for r in sorted(rows, key=lambda x: (x.get("status", 0) != 200, x.get("name", ""))):
        status = r.get("status", 0)
        ok_row = status == 200 and r.get("looks_xml") and r.get("items", 0) > 0
        ok += ok_row
        bad += not ok_row
        marker = " " if ok_row else "!"
        final = r.get("final_url", "") or r.get("error", "")
        print(f"{marker}{r['name'][:34]:<35}"
              f"{status:>8}{r.get('items', 0):>8}  {final[:70]}")
    print("─" * 110)
    print(f"healthy: {ok}    problem: {bad}    total: {len(rows)}\n")
    return 0 if bad == 0 else 1


async def run(args: argparse.Namespace) -> int:
    # Supabase client is needed whenever we're not a pure dry-run OR when the
    # user asked to load feeds from fs_feeds. Create it once up front so we
    # can pass it into load_feeds_from_db too.
    sb: Client | None = None
    need_sb = (not args.dry_run) or args.feeds_from_db
    if need_sb:
        if not (SUPABASE_URL and SUPABASE_KEY):
            log.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
            return 2
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.feeds_from_db:
        assert sb is not None
        feeds = load_feeds_from_db(
            sb, args.feed,
            english_only=not args.all_languages,
            include_disabled=args.include_disabled,
        )
        log.info("loaded %d feeds from fs_feeds", len(feeds))
    else:
        feeds = load_feeds(FEEDS_FILE, args.feed, english_only=not args.all_languages)
        # Backfill feed.id from the DB so the scraper can write feed_id onto
        # fs_news rows and sync per-feed state into fs_feeds, even when the
        # yaml is the source of truth for config.
        if sb is not None:
            id_map = load_feed_id_map(sb)
            linked = 0
            for f in feeds:
                if f.url in id_map:
                    f.id = id_map[f.url]
                    linked += 1
            if linked:
                log.info("linked %d/%d feeds to fs_feeds rows", linked, len(feeds))
            missing = [f.name for f in feeds if not f.id]
            if missing:
                log.info("no fs_feeds row for %d feeds (feed_id will be NULL): %s",
                         len(missing), ", ".join(missing[:5]) + ("..." if len(missing) > 5 else ""))
    if not feeds:
        log.error("no feeds matched")
        return 2
    log.info("loaded %d feeds", len(feeds))

    if args.verify_feeds:
        return await verify_feeds(feeds)

    sem = asyncio.Semaphore(MAX_CONCURRENT)
    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT,
        http2=False,
    ) as client:
        tasks = [process_feed(f, client, sb, args, sem) for f in feeds]
        # return_exceptions=True so one rogue exception doesn't kill the
        # whole batch — process_feed catches its own errors, but this is
        # a belt-and-braces guard for anything unexpected.
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[FeedResult] = []
    for feed, r in zip(feeds, raw_results):
        if isinstance(r, BaseException):
            log.error("[%s] unexpected task exception: %r", feed.domain, r)
            fr = FeedResult(feed=feed)
            fr.errors.append(f"unhandled: {r!r}")
            results.append(fr)
        else:
            results.append(r)

    summarise(results)
    return 0 if all(not r.errors for r in results) else 1


async def refresh_feed_metadata(
    sb: Client, only: str | None
) -> int:
    """Walk every fs_feeds row, fetch the RSS for channel metadata (title,
    description, link, logo, ttl, generator) and the publisher homepage for
    og:*, favicon, apple-touch-icon, webmanifest, sitemap, and JSON-LD
    Organization.sameAs social profiles. Writes back into fs_feeds.
    Safe to re-run; updates only populate fields we successfully extracted.
    """
    res = sb.table("fs_feeds").select("id,url,name,domain").execute()
    feeds = [r for r in (res.data or [])
             if not only or only in r["url"] or only in r["domain"]]
    if not feeds:
        log.error("no feeds matched in fs_feeds")
        return 2
    log.info("refreshing metadata for %d feeds", len(feeds))

    sem = asyncio.Semaphore(MAX_CONCURRENT)
    stats = {"updated": 0, "empty": 0, "errors": 0}

    async def refresh_one(feed_row: dict, client: httpx.AsyncClient) -> None:
        fid = feed_row["id"]
        name = feed_row.get("name") or feed_row["url"]
        updates: dict[str, Any] = {}

        # 1. Fetch RSS for channel-level metadata.
        try:
            async with sem:
                r = await client.get(
                    feed_row["url"],
                    headers={"User-Agent": USER_AGENT},
                    timeout=REQUEST_TIMEOUT,
                    follow_redirects=True,
                )
            if r.status_code == 200 and r.content:
                ch = (feedparser.parse(r.content).feed or {})
                updates["feed_title"]        = ch.get("title")
                updates["feed_description"]  = ch.get("description") or ch.get("subtitle")
                updates["feed_link"]         = ch.get("link")
                updates["feed_generator"]    = ch.get("generator")
                updates["feed_copyright"]    = ch.get("rights")
                img = ch.get("image")
                if isinstance(img, dict):
                    updates["feed_image_url"] = img.get("href") or img.get("url")
                elif isinstance(img, str):
                    updates["feed_image_url"] = img
                ttl = ch.get("ttl")
                if ttl:
                    try:
                        updates["feed_ttl_minutes"] = int(str(ttl))
                    except (TypeError, ValueError):
                        pass
                upd = ch.get("updated_parsed") or ch.get("published_parsed")
                if upd:
                    try:
                        updates["feed_last_build_date"] = datetime(
                            *upd[:6], tzinfo=timezone.utc
                        ).isoformat()
                    except (TypeError, ValueError):
                        pass
        except Exception as e:  # noqa: BLE001
            log.warning("[%s] channel metadata fetch failed: %s", name, e)
            stats["errors"] += 1

        # 2. Fetch homepage (feed_link) for site metadata + socials.
        homepage = updates.get("feed_link")
        if homepage:
            try:
                async with sem:
                    r2 = await client.get(
                        homepage,
                        headers={"User-Agent": BROWSER_UA},
                        timeout=REQUEST_TIMEOUT,
                        follow_redirects=True,
                    )
                if r2.status_code == 200 and r2.text:
                    site = extract_homepage_metadata(r2.text, str(r2.url))
                    updates.update(site)
                    if not updates.get("favicon_url"):
                        pb = urlparse(str(r2.url))
                        if pb.scheme and pb.netloc:
                            updates["favicon_url"] = f"{pb.scheme}://{pb.netloc}/favicon.ico"
            except Exception as e:  # noqa: BLE001
                log.warning("[%s] homepage metadata fetch failed: %s", name, e)

        # Drop Nones so we never overwrite good data with NULL.
        updates = {k: v for k, v in updates.items() if v not in (None, "", [], {})}

        if updates:
            try:
                sb.table("fs_feeds").update(updates).eq("id", fid).execute()
                stats["updated"] += 1
                log.info("[%s] updated %d fields", name, len(updates))
            except Exception as e:  # noqa: BLE001
                stats["errors"] += 1
                log.error("[%s] DB update failed: %s", name, e)
        else:
            stats["empty"] += 1
            log.info("[%s] no metadata extracted", name)

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, http2=False) as client:
        await asyncio.gather(
            *(refresh_one(f, client) for f in feeds),
            return_exceptions=True,
        )

    print(f"\nmetadata refresh: {stats['updated']} updated, "
          f"{stats['empty']} empty, {stats['errors']} errors\n")
    return 0 if stats["errors"] == 0 else 1


def _cmd_list_disabled() -> int:
    rows = list_failures()
    if not rows:
        print("No feeds have recorded failures.")
        return 0
    print(f"\n{'Feed URL':<70}{'Fails':>7}  Last error")
    print("─" * 110)
    for url, count, err, at in rows:
        status = " (disabled)" if count >= FAIL_DISABLE_THRESHOLD else ""
        err_short = (err or "")[:60].replace("\n", " ")
        print(f"{url[:70]:<70}{count:>7}  {err_short}{status}")
    print()
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Fashion RSS → Supabase fs_news")
    ap.add_argument("--feed", help="filter by domain or URL substring")
    ap.add_argument("--dry-run", action="store_true", help="parse but do not write")
    ap.add_argument("--limit", type=int, default=0, help="max entries per feed")
    ap.add_argument("--verify-feeds", action="store_true",
                    help="probe every feed URL and print status; no ingest, no state changes")
    ap.add_argument("--include-disabled", action="store_true",
                    help="ignore the auto-disable list and probe all feeds")
    ap.add_argument("--reset-failures", action="store_true",
                    help="clear recorded failure counts for all feeds (or --feed match) and exit")
    ap.add_argument("--list-disabled", action="store_true",
                    help="print feeds with recorded consecutive failures and exit")
    ap.add_argument("--all-languages", action="store_true",
                    help="include non-English feeds (default: English-only)")
    ap.add_argument("--feeds-from-db", action="store_true",
                    help="load feeds from fs_feeds instead of feeds.yaml")
    ap.add_argument("--refresh-feed-metadata", action="store_true",
                    help="walk fs_feeds, enrich each row with RSS channel + "
                         "homepage metadata (title/description/logo/favicon/"
                         "og/JSON-LD socials), then exit")
    args = ap.parse_args()

    if args.list_disabled:
        sys.exit(_cmd_list_disabled())
    if args.reset_failures:
        rows = list_failures()
        match = args.feed or ""
        cleared = 0
        for (url, _c, _e, _at) in rows:
            if not match or match in url:
                reset_failure(url)
                cleared += 1
        print(f"cleared failure counters for {cleared} feed(s)")
        sys.exit(0)

    if args.refresh_feed_metadata:
        if not (SUPABASE_URL and SUPABASE_KEY):
            log.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
            sys.exit(2)
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        try:
            sys.exit(asyncio.run(refresh_feed_metadata(sb, args.feed)))
        except KeyboardInterrupt:
            log.warning("interrupted")
            sys.exit(130)

    try:
        sys.exit(asyncio.run(run(args)))
    except KeyboardInterrupt:
        log.warning("interrupted")
        sys.exit(130)


if __name__ == "__main__":
    main()
