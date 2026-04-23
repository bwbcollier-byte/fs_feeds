"""
discover_feeds.py — scan fs_brands and fs_stores for RSS feeds and register
the ones that exist into fs_feeds + the fs_feed_{brands,stores} join tables.

Discovery strategy per entity:
  1. Normalise the website column ("example.com" → "https://example.com").
  2. GET the homepage with a browser UA.
  3. Parse <link rel="alternate" type="application/rss+xml"> (+ atom+xml) tags.
     Autodiscovery URLs are the source of truth when present.
  4. If no autodiscovery, try common feed paths one at a time until one
     validates (WordPress, Shopify, Ghost, Hugo, Blogger, generic).
  5. Validate every candidate: must parse as RSS/Atom *and* contain at
     least one <item>/<entry> with a non-empty link.
  6. Pick the highest-priority validated candidate, extract channel
     metadata (title, description, link, image, generator, ttl), insert
     into fs_feeds (ON CONFLICT (url) DO NOTHING), and link the source
     entity via fs_feed_brands or fs_feed_stores. For brands we also
     write the discovered URL back onto fs_brands.rss_feed_url.

Usage:
    python discover_feeds.py                              # both tables, no limit
    python discover_feeds.py --only stores --limit 100    # first 100 stores
    python discover_feeds.py --only brands                # brands only
    python discover_feeds.py --dry-run                    # discover + report only
    python discover_feeds.py --concurrency 10             # default is 6
    python discover_feeds.py --no-skip-linked             # re-check entities
                                                          # that already have a
                                                          # feed linked

The script is idempotent: run it again after adding more stores and it
won't create duplicate fs_feeds rows (URL uniqueness enforced) or
duplicate link rows (primary key on (feed_id, entity_id)).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import feedparser  # type: ignore
import httpx
from dotenv import load_dotenv
from supabase import Client, create_client  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).parent
load_dotenv(ROOT / ".env")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 15.0       # lower than the main scraper — we're probing lots
DEFAULT_CONCURRENCY = 6
PAGE_SIZE = 1000             # rows per Supabase query page

# Tried in order if homepage autodiscovery finds nothing. We kept this list
# short on purpose — every path here costs one HTTP roundtrip per entity, and
# hammering a host with 10+ requests back-to-back tends to trigger WAF rate
# limits (429s) that can then mask real feeds. These five cover WordPress,
# generic XML, Shopify, and static-site generators — which is ≥90% of the
# fashion / e-commerce CMSes we'll encounter.
FALLBACK_PATHS: tuple[str, ...] = (
    "/feed/",                 # WordPress (most common)
    "/rss",                   # generic
    "/rss.xml",               # generic XML
    "/blogs/news.atom",       # Shopify
    "/atom.xml",              # Hugo / Jekyll / misc
)

# How many 429s we'll tolerate from a single host before giving up on it.
# Once a host is rate-limiting us, further probes are just noise.
MAX_429_PER_HOST = 1

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("discover")
# Silence httpx's per-request INFO chatter — our own progress + per-hit log
# is enough, and the default output drowned real signal when we probe
# thousands of stores.
logging.getLogger("httpx").setLevel(logging.WARNING)


# ──────────────────────────────────────────────────────────────────────────────
# Data shapes
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class Entity:
    kind: str            # "brand" or "store"
    id: str
    name: str
    website: str         # raw value from the row


@dataclass
class FoundFeed:
    url: str
    channel: dict[str, Any]   # parsed feedparser channel fields
    source: str               # "autodiscovery" | path string that worked


# ──────────────────────────────────────────────────────────────────────────────
# URL normalisation + feed parsing
# ──────────────────────────────────────────────────────────────────────────────

def normalise_website(raw: str | None) -> str | None:
    """Coerce a stored website value to a fetchable https:// URL.

    Handles bare domains, stripped protocols, trailing whitespace, and
    paths. Returns None if we can't make sense of it.
    """
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None
    if "://" not in s:
        s = "https://" + s
    try:
        p = urlparse(s)
    except Exception:  # noqa: BLE001
        return None
    if not p.netloc:
        return None
    # Keep scheme https; some rows store "http://" which breaks on modern sites.
    return urlunparse((p.scheme or "https", p.netloc, p.path or "/", "", "", ""))


_LINK_RX = re.compile(
    r'<link[^>]+rel=["\']alternate["\'][^>]*?>',
    re.IGNORECASE,
)
_ATTR_RX = re.compile(r'(\w+)=["\']([^"\']+)', re.IGNORECASE)
_FEED_TYPES = ("application/rss+xml", "application/atom+xml", "application/rdf+xml")


def parse_autodiscovery(html: str, base_url: str) -> list[str]:
    """Return every <link rel='alternate' type='application/rss+xml'> href
    from the homepage HTML, absolute-ified against base_url. Order preserved
    so callers probe them in the order the site declared them."""
    out: list[str] = []
    for m in _LINK_RX.finditer(html):
        attrs = {k.lower(): v for k, v in _ATTR_RX.findall(m.group(0))}
        if attrs.get("type", "").lower() not in _FEED_TYPES:
            continue
        href = attrs.get("href")
        if not href:
            continue
        out.append(absolutise(href, base_url))
    # dedupe preserving order
    seen: set[str] = set()
    return [u for u in out if not (u in seen or seen.add(u))]


def absolutise(url: str, base: str) -> str:
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
    return f"{pb.scheme}://{pb.netloc}/{url.lstrip('/')}"


def looks_like_feed(body: bytes) -> bool:
    """Cheap test before paying the feedparser.parse() cost. Checks the
    first 400 bytes for RSS/Atom/RDF roots."""
    head = body[:400].lstrip().lower()
    return (
        head.startswith(b"<?xml")
        or head.startswith(b"<rss")
        or head.startswith(b"<feed")
        or head.startswith(b"<rdf")
    )


def validate_feed(body: bytes) -> dict[str, Any] | None:
    """Parse as RSS/Atom. Returns the channel dict if it has ≥1 entry with
    a non-empty link, else None."""
    if not body or not looks_like_feed(body):
        return None
    try:
        parsed = feedparser.parse(body)
    except Exception:  # noqa: BLE001
        return None
    entries = parsed.get("entries") or []
    if not entries:
        return None
    if not any((e.get("link") or "").strip() for e in entries):
        return None
    return dict(parsed.get("feed") or {})


@dataclass
class ProbeResult:
    channel: dict[str, Any] | None
    rate_limited: bool = False
    redirected_to_homepage: bool = False


def _same_page(a: str, b: str) -> bool:
    """True if two URLs point at the same (origin, path-normalised) page —
    used to detect feed probes that 302 back to the homepage."""
    try:
        pa, pb = urlparse(a), urlparse(b)
    except Exception:  # noqa: BLE001
        return False
    norm = lambda p: p.rstrip("/") or "/"
    return pa.netloc == pb.netloc and norm(pa.path) == norm(pb.path)


async def try_feed_url(
    client: httpx.AsyncClient,
    url: str,
    homepage_final: str | None = None,
) -> ProbeResult:
    """Fetch one candidate URL. Returns ProbeResult with:
      - `channel`: parsed feed channel if URL is a real feed
      - `rate_limited`: True if the host 429'd us
      - `redirected_to_homepage`: True if the request followed a redirect
        back to the homepage (very common on SPAs that catchall-route
        unknown paths to `/`); means the host doesn't expose this path
        and further probes are unlikely to yield anything better."""
    try:
        r = await client.get(
            url,
            headers={"User-Agent": BROWSER_UA,
                     "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.5"},
            timeout=REQUEST_TIMEOUT,
            follow_redirects=True,
        )
    except Exception:  # noqa: BLE001
        return ProbeResult(channel=None)

    if r.status_code == 429:
        return ProbeResult(channel=None, rate_limited=True)
    if r.status_code != 200 or not r.content:
        return ProbeResult(channel=None)

    # If we landed on the homepage (URL redirected back, or the response is
    # obviously an HTML landing page), treat as "no feed here" and signal
    # the caller to skip the rest of the fallbacks.
    if homepage_final and _same_page(str(r.url), homepage_final):
        return ProbeResult(channel=None, redirected_to_homepage=True)

    channel = validate_feed(r.content)
    return ProbeResult(channel=channel)


async def discover_for_website(
    client: httpx.AsyncClient, homepage: str, sem: asyncio.Semaphore
) -> FoundFeed | None:
    """Try autodiscovery first, then common fallback paths. Returns the
    first candidate that validates, or None.

    Aborts early on a single host when:
      - the host returned 429 (rate limiting — further probes make it worse)
      - a fallback probe resolved back to the homepage (the host's
        catchall router is swallowing unknown paths, so subsequent
        fallbacks will too)"""
    # 1. Homepage fetch for autodiscovery.
    try:
        async with sem:
            r = await client.get(
                homepage,
                headers={"User-Agent": BROWSER_UA},
                timeout=REQUEST_TIMEOUT,
                follow_redirects=True,
            )
    except Exception:  # noqa: BLE001
        return None
    if r.status_code == 429:
        log.debug("[%s] homepage 429 — skipping", homepage)
        return None
    if r.status_code != 200 or not r.text:
        return None

    final_base = str(r.url)
    candidates: list[tuple[str, str]] = []  # (url, source_label)
    for href in parse_autodiscovery(r.text, final_base):
        candidates.append((href, "autodiscovery"))

    # 2. Fallback paths from the final_base origin.
    pb = urlparse(final_base)
    origin = f"{pb.scheme}://{pb.netloc}"
    for path in FALLBACK_PATHS:
        candidates.append((origin + path, path))

    seen: set[str] = set()
    rate_limit_count = 0
    for url, source in candidates:
        if url in seen:
            continue
        seen.add(url)
        async with sem:
            probe = await try_feed_url(client, url, homepage_final=final_base)
        if probe.channel is not None:
            return FoundFeed(url=url, channel=probe.channel, source=source)
        if probe.redirected_to_homepage:
            # Any further fallback path will also get caught by the same
            # catchall. No point continuing.
            log.debug("[%s] %s redirected back to homepage, aborting probes",
                      origin, source)
            return None
        if probe.rate_limited:
            rate_limit_count += 1
            if rate_limit_count > MAX_429_PER_HOST:
                log.debug("[%s] too many 429s, aborting probes", origin)
                return None
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Supabase reads + writes
# ──────────────────────────────────────────────────────────────────────────────

def load_brands(sb: Client, limit: int, skip_linked: bool) -> list[Entity]:
    """Pull brands that have *any* url-ish column populated. Prefer rss_feed_url
    / blog_feed_url if the brand already has one recorded; fall back to
    website_url. Skip any brand already linked via fs_feed_brands unless
    skip_linked=False."""
    linked_ids: set[str] = set()
    if skip_linked:
        res = sb.table("fs_feed_brands").select("brand_id").execute()
        linked_ids = {row["brand_id"] for row in (res.data or [])}

    q = sb.table("fs_brands").select(
        "id,name,website_url,blog_feed_url,rss_feed_url"
    )
    res = q.execute()
    out: list[Entity] = []
    for row in (res.data or []):
        if skip_linked and row["id"] in linked_ids:
            continue
        # Prefer an already-known feed URL; otherwise try the website
        site = (row.get("rss_feed_url")
                or row.get("blog_feed_url")
                or row.get("website_url"))
        if not site:
            continue
        out.append(Entity(kind="brand", id=row["id"],
                          name=row["name"] or row["id"], website=site))
        if limit and len(out) >= limit:
            break
    return out


def load_stores(sb: Client, limit: int, skip_linked: bool) -> list[Entity]:
    """Paginated read of fs_stores.website with skip_linked filtering.
    fs_stores has ~15k rows with websites so we page at PAGE_SIZE."""
    linked_ids: set[str] = set()
    if skip_linked:
        res = sb.table("fs_feed_stores").select("store_id").execute()
        linked_ids = {row["store_id"] for row in (res.data or [])}

    out: list[Entity] = []
    offset = 0
    while True:
        q = (sb.table("fs_stores")
             .select("id,name,website")
             .not_.is_("website", "null")
             .neq("website", "")
             .order("id")
             .range(offset, offset + PAGE_SIZE - 1))
        res = q.execute()
        rows = res.data or []
        if not rows:
            break
        for row in rows:
            if skip_linked and row["id"] in linked_ids:
                continue
            out.append(Entity(kind="store", id=row["id"],
                              name=row["name"] or row["id"],
                              website=row["website"]))
            if limit and len(out) >= limit:
                return out
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return out


def upsert_fs_feed(sb: Client, found: FoundFeed, entity: Entity) -> str | None:
    """Insert into fs_feeds on first sighting, return feed_id. If the URL
    already exists (another brand/store shares the same feed), fetch the
    existing id instead."""
    ch = found.channel
    domain = urlparse(found.url).netloc.lower().removeprefix("www.")

    # Best-effort channel metadata at insert time so we don't have to
    # re-fetch during --refresh-feed-metadata later.
    payload: dict[str, Any] = {
        "url": found.url,
        "name": (ch.get("title") or entity.name or domain)[:300],
        "domain": domain,
        "category": "industry_news",    # placeholder; caller can edit later
        "country": "GLOBAL",
        "language": (ch.get("language") or "en").split("-")[0].lower()[:5],
        "paywalled": False,
        "is_active": True,
        "feed_title": ch.get("title"),
        "feed_description": ch.get("description") or ch.get("subtitle"),
        "feed_link": ch.get("link"),
        "feed_generator": ch.get("generator"),
        "feed_copyright": ch.get("rights"),
        "notes": f"auto-discovered via {found.source} from {entity.kind} {entity.name}",
    }
    img = ch.get("image")
    if isinstance(img, dict):
        payload["feed_image_url"] = img.get("href") or img.get("url")
    elif isinstance(img, str):
        payload["feed_image_url"] = img
    payload = {k: v for k, v in payload.items() if v not in (None, "")}

    try:
        res = sb.table("fs_feeds").upsert(
            payload, on_conflict="url", ignore_duplicates=True,
        ).execute()
        if res.data:
            return res.data[0]["id"]
    except Exception as e:  # noqa: BLE001
        log.warning("upsert fs_feeds failed for %s: %s", found.url, e)

    # Duplicate URL → ignore_duplicates returned nothing. Look up the row.
    try:
        res = sb.table("fs_feeds").select("id").eq("url", found.url).limit(1).execute()
        if res.data:
            return res.data[0]["id"]
    except Exception as e:  # noqa: BLE001
        log.warning("lookup fs_feeds failed for %s: %s", found.url, e)
    return None


def link_entity(sb: Client, feed_id: str, entity: Entity) -> None:
    table = "fs_feed_brands" if entity.kind == "brand" else "fs_feed_stores"
    id_col = "brand_id" if entity.kind == "brand" else "store_id"
    try:
        sb.table(table).upsert(
            {"feed_id": feed_id, id_col: entity.id, "relation": "publisher_of"},
            on_conflict=f"feed_id,{id_col}", ignore_duplicates=True,
        ).execute()
    except Exception as e:  # noqa: BLE001
        log.warning("link %s for %s failed: %s", table, entity.id, e)


def writeback_brand_rss(sb: Client, brand_id: str, feed_url: str) -> None:
    try:
        sb.table("fs_brands").update({"rss_feed_url": feed_url}).eq("id", brand_id).execute()
    except Exception as e:  # noqa: BLE001
        log.warning("writeback fs_brands.rss_feed_url failed for %s: %s", brand_id, e)


# ──────────────────────────────────────────────────────────────────────────────
# Main driver
# ──────────────────────────────────────────────────────────────────────────────

async def process_entity(
    entity: Entity,
    client: httpx.AsyncClient,
    sb: Client | None,
    sem: asyncio.Semaphore,
    counters: dict[str, int],
    dry_run: bool,
) -> None:
    homepage = normalise_website(entity.website)
    if not homepage:
        counters["invalid_url"] += 1
        return

    found = await discover_for_website(client, homepage, sem)
    if not found:
        counters["no_feed"] += 1
        return

    counters["found"] += 1
    log.info("[%s:%s] feed found via %s: %s",
             entity.kind, entity.name[:40], found.source, found.url)

    if dry_run or sb is None:
        counters["dry_skip_write"] += 1
        return

    feed_id = upsert_fs_feed(sb, found, entity)
    if not feed_id:
        counters["write_failed"] += 1
        return

    link_entity(sb, feed_id, entity)
    if entity.kind == "brand":
        writeback_brand_rss(sb, entity.id, found.url)
    counters["written"] += 1


async def run(args: argparse.Namespace) -> int:
    if not (SUPABASE_URL and SUPABASE_KEY):
        log.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
        return 2

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    if args.dry_run:
        log.info("DRY RUN — no DB writes")

    entities: list[Entity] = []
    if args.only in ("both", "brands"):
        brands = load_brands(sb, args.limit if args.only == "brands" else 0, args.skip_linked)
        log.info("loaded %d brand(s) with a usable website", len(brands))
        entities.extend(brands)
    if args.only in ("both", "stores"):
        remaining = args.limit - len(entities) if args.limit else 0
        limit = remaining if args.limit and args.only == "both" else args.limit
        stores = load_stores(sb, limit, args.skip_linked)
        log.info("loaded %d store(s) with a usable website", len(stores))
        entities.extend(stores)

    if not entities:
        log.info("nothing to do")
        return 0

    log.info("probing %d entit(ies) with concurrency=%d", len(entities), args.concurrency)

    sem = asyncio.Semaphore(args.concurrency)
    counters = {"found": 0, "no_feed": 0, "invalid_url": 0,
                "written": 0, "write_failed": 0, "dry_skip_write": 0}
    start = datetime.now(timezone.utc)

    async with httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT,
        http2=False,
        headers={"User-Agent": BROWSER_UA},
    ) as client:
        tasks = [
            process_entity(e, client, sb, sem, counters, args.dry_run)
            for e in entities
        ]
        # Chunk progress every 25 so long runs don't look stuck.
        done = 0
        for i in range(0, len(tasks), 25):
            chunk = tasks[i : i + 25]
            await asyncio.gather(*chunk, return_exceptions=True)
            done += len(chunk)
            log.info("progress: %d / %d  (found=%d, no_feed=%d, written=%d)",
                     done, len(entities),
                     counters["found"], counters["no_feed"], counters["written"])

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print()
    print("─" * 72)
    print(f"  Entities probed          : {len(entities)}")
    print(f"  Feeds discovered         : {counters['found']}")
    print(f"  No feed / unreachable    : {counters['no_feed']}")
    print(f"  Invalid website column   : {counters['invalid_url']}")
    if args.dry_run:
        print(f"  Would-write (dry-run)    : {counters['dry_skip_write']}")
    else:
        print(f"  New fs_feeds rows written: {counters['written']}")
        print(f"  Write failures           : {counters['write_failed']}")
    hit_rate = (100.0 * counters['found'] / len(entities)) if entities else 0
    print(f"  Hit rate                 : {hit_rate:.1f}%")
    print(f"  Elapsed                  : {elapsed:.1f}s")
    print("─" * 72)
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Discover RSS feeds for fs_brands / fs_stores")
    ap.add_argument("--only", choices=("brands", "stores", "both"), default="both",
                    help="which table(s) to scan (default: both)")
    ap.add_argument("--limit", type=int, default=0,
                    help="max entities to probe (0 = no limit)")
    ap.add_argument("--dry-run", action="store_true",
                    help="discover and report, no DB writes")
    ap.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                    help="concurrent HTTP requests (default 6)")
    ap.add_argument("--no-skip-linked", dest="skip_linked", action="store_false",
                    help="re-probe entities that already have a feed linked "
                         "(default: skip ones already in fs_feed_brands/stores)")
    ap.set_defaults(skip_linked=True)
    args = ap.parse_args()

    try:
        sys.exit(asyncio.run(run(args)))
    except KeyboardInterrupt:
        log.warning("interrupted")
        sys.exit(130)


if __name__ == "__main__":
    main()
