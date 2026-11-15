#!/usr/bin/env python3
"""
Trend detector for the tech industry: AI, software, development, SaaS, cybersecurity, marketing.
Detection only. Anything off-topic (pizza, flights, ...) or low-value (jobs, courses, "what is X")
is filtered out.

  python main.py                     all countries (US UK DK DE RO MD), last 7 days
  python main.py --countries US RO   only some countries
  python main.py --rss-only          quick test, skips pytrends
  python main.py --workers 2         fewer parallel keywords (use if you get rate limited)
  python main.py --no-enrich         skip velocity/region/topic enrichment (fewer requests)

Install: pip install pytrends pandas feedparser
Output:  out/<date>/themes.json   themes, ranked
         out/<date>/trends.csv    every kept term (raw rows + gprop + news meta)
         out/<date>/dropped.csv   every term the filter removed (check it now and then)
"""
import argparse
import datetime as dt
import json
import math
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import feedparser
import pandas as pd
from dotenv import load_dotenv
from pytrends.exceptions import ResponseError
from pytrends.request import TrendReq

load_dotenv()  # reads .env: R2_* keys, OPENCODE_KEY, ...

# Country code -> Google geo code (UK is GB in Google)
GEOS = {"US": "US", "UK": "GB", "DK": "DK", "DE": "DE", "RO": "RO", "MD": "MD"}

# Starting points for pytrends (it returns queries related to these)
SEEDS = ["artificial intelligence", "generative AI", "ChatGPT", "AI agents", "LLM",
         "software", "software development", "DevOps", "SaaS", "cybersecurity",
         "digital marketing", "social media marketing", "SEO"]

# Anchors for platform-specific (gprop) pull — small on purpose to save quota
GPROP_ANCHORS = ["artificial intelligence", "software development", "cybersecurity"]
GPROPS = ["youtube", "news"]

# ======================= WHAT COUNTS AS "ON TOPIC" (edit these lists) =======================
# A term is kept only if it contains one of these whole words ...
TOPIC_WORDS = {
    # AI
    "ai", "ki", "gpt", "llm", "llms", "chatgpt", "openai", "anthropic", "claude", "gemini",
    "copilot", "perplexity", "mistral", "llama", "deepseek", "grok", "midjourney", "sora",
    "fable", "mythos", "agentic", "genai", "mcp", "ml", "nlp", "prompt", "prompts", "chatbot",
    "chatbots", "nvidia",
    # software / development / cloud / security
    "saas", "api", "apis", "sdk", "devops", "docker", "kubernetes", "aws", "azure", "gcp",
    "cloud", "python", "javascript", "typescript", "react", "nextjs", "rust", "github",
    "gitlab", "backend", "frontend", "fullstack", "coding", "developer", "developers",
    "programming", "sql", "startup", "startups", "infosec", "ransomware", "malware",
    "encryption", "crm", "erp", "automation", "nocode", "lowcode",
    # marketing
    "seo", "geo", "sem", "ppc", "ads", "advertising", "analytics", "funnel", "funnels",
    "branding", "influencer", "hubspot", "salesforce", "shopify", "wordpress", "mailchimp",
    "zapier", "notion", "figma", "stripe", "ecommerce", "commerce",
}
# ... or a word that STARTS with one of these (catches artificial/artificială, software/softwareentwicklung ...)
TOPIC_STEMS = ("artificial", "intellig", "inteligen", "cyber", "algorithm", "software", "marketing",
               "robot", "quantum", "blockchain", "datab", "dataset", "kunstig", "künstlich",
               "искусственн")
# Terms containing any of these words are dropped even if on topic (evergreen / buyer / consumer noise)
NOISE_WORDS = {
    "near", "nearby", "salary", "salaries", "tips", "course", "courses", "class",
    "certification", "certificate", "tutorial", "training", "login", "signin", "download",
    "free", "gratis", "meaning", "definition", "agency", "agencies", "bureau", "freelancer",
    "resume", "interview", "cheap", "cheapest", "price",
    # consumer hardware / drivers
    "logitech", "razer", "moza", "aula", "hator", "nero", "ps4", "ps5", "playstation", "xbox",
    "tv", "camera", "mouse", "keyboard", "printer", "driver", "drivers", "firmware", "iphone",
    "amd", "vw", "faa", "rtx",
    # names that contain a topic word by accident (e.g. Korean actors called "Seo ...")
    "kang", "joon", "yea", "ji", "eve", "duet",
    # local-intent cities seen in results (add more when you notice them)
    "london", "bournemouth", "brighton", "leicester", "orlando", "frankfurt",
}
# Evergreen question / listicle searches: "what is saas", "how to ...", "best seo company"
NOISE_START = re.compile(r"^(what|how|why|who|when|best|top)\b")
NOISE_END = re.compile(r"\bjobs?( description)?$")          # "cybersecurity jobs", "... job description"
# ============================================================================================

# Ignored only when deciding whether two terms are about the same thing
GENERIC = {"the", "and", "for", "with", "new", "from", "your", "online", "app", "software",
           "marketing", "digital", "social", "media", "seo", "cybersecurity", "artificial",
           "intelligence", "saas", "security", "cloud", "ai"}


# ---------------------------------------------------------------- R2 storage
def week_folder(today=None):
    y, w, _ = (today or dt.date.today()).isocalendar()
    return f"{y}-W{w:02d}"


def r2_client():
    try:
        import boto3
    except ImportError:
        raise SystemExit("boto3 not installed. Run: pip install boto3")
    endpoint = os.getenv("R2_ENDPOINT_URL")
    key = os.getenv("R2_ACCESS_KEY_ID")
    secret = os.getenv("R2_SECRET_ACCESS_KEY")
    if not endpoint or not key or not secret:
        raise SystemExit("Missing R2_ENDPOINT_URL / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY in .env")
    return boto3.client("s3", endpoint_url=endpoint,
                        aws_access_key_id=key, aws_secret_access_key=secret,
                        region_name="auto")


def upload_blobs(blobs, week=None):
    """Upload in-memory (name, bytes, content_type) to R2 bucket/<week>/.

    Overwrites existing keys. No local files touched.
    Returns list of keys.
    """
    bucket = os.getenv("R2_BUCKET", "devalon-marketing-weekly-trends")
    week = week or week_folder()
    s3 = r2_client()
    # fail fast if bucket is wrong / unreachable
    s3.head_bucket(Bucket=bucket)
    keys = []
    for name, body, ctype in blobs:
        key = f"{week}/{name}"
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=ctype)
        print(f"  R2 s3://{bucket}/{key} (overwritten if existed)")
        keys.append(key)
    return keys


def public_url(key):
    base = os.getenv("R2_PUBLIC_URL",
                     "https://pub-0120355bea2f413bb0aa438ed986ba73.r2.dev").rstrip("/")
    return f"{base}/{key}"


def tokens(text):
    return re.findall(r"\w+", str(text).lower())


def is_relevant(text):
    t = str(text).lower().strip()
    ws = tokens(t)
    if not ws or NOISE_START.match(t) or NOISE_END.search(t) or any(w in NOISE_WORDS for w in ws):
        return False
    if any(int(y) < dt.date.today().year for y in re.findall(r"\b(?:19|20)\d{2}\b", t)):
        return False                                   # old-year evergreen, e.g. "... trends 2024"
    return any(w in TOPIC_WORDS or w.startswith(TOPIC_STEMS) for w in ws)


_local = threading.local()


def client():
    """pytrends is not thread-safe, so every worker thread gets its own client."""
    if not hasattr(_local, "pt"):
        # NOTE: no retries/backoff here — pytrends 4.9.2 passes
        # method_whitelist to urllib3 v2 which removed it -> TypeError.
        _local.pt = TrendReq(hl="en-US", tz=-120, timeout=(10, 30))
    return _local.pt


# ---------------------------------------------------------------- 1. collect
def rss_trends(country):
    """Daily trends (last ~24h) + full news/image metadata (free, no quota)."""
    feed = feedparser.parse(f"https://trends.google.com/trending/rss?geo={GEOS[country]}")
    rows = []
    for e in feed.entries:
        traffic = re.sub(r"\D", "", e.get("ht_approx_traffic", "")) or "100"
        rows.append(dict(
            term=e.title.lower().strip(), kind="rss", value=float(traffic),
            story=e.get("ht_news_item_title", ""),
            news_url=e.get("ht_news_item_url", ""),
            news_source=e.get("ht_news_item_source", ""),
            news_picture=e.get("ht_news_item_picture", ""),
            picture=e.get("ht_picture", ""),
            gprop="", timeframe="rss",
        ))
    return rows


def related_queries(keyword, geo, timeframe, gprop=""):
    """Rising + top queries for one keyword. Returns [] if Google has no data."""
    pt = client()
    time.sleep(random.uniform(1, 4))                   # stagger the parallel requests
    for attempt in range(3):
        try:
            pt.build_payload([keyword], timeframe=timeframe, geo=geo, gprop=gprop)
            data = pt.related_queries().get(keyword) or {}
            break
        except IndexError:                             # Google returned an empty list = no data
            return []
        except ResponseError:                          # HTTP 429 = too many requests, wait, retry
            time.sleep(30 * (attempt + 1))
        except Exception as e:
            print(f"   skipped ({type(e).__name__})")
            return []
    else:
        return []
    rows = []
    for kind in ("rising", "top"):
        df = data.get(kind)
        if df is not None and not df.empty:
            rows += [dict(term=str(r["query"]).lower().strip(), kind=kind,
                          value=float(r["value"]), story="",
                          news_url="", news_source="", news_picture="", picture="",
                          gprop=gprop, timeframe=timeframe)
                     for _, r in df.iterrows()]
    return rows


# ------------------------------------------------------- 1b. enrichment (quota-light)
def _batch(lst, n=5):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch_velocity(terms, timeframe="now 7-d"):
    """interest_over_time per 5-term batch -> {term: (velocity, sparkline)}.

    velocity = (last - first) / max(first, 1). sparkline = last ~14 points.
    """
    vel, spark = {}, {}
    if not terms:
        return vel, spark
    pt = client()
    for batch in _batch(terms, 5):
        try:
            time.sleep(random.uniform(8, 15))  # calm: enrichment runs after ~500 base calls
            pt.build_payload(batch, timeframe=timeframe, geo="")
            df = pt.interest_over_time()
            if df is None or df.empty:
                continue
            for kw in batch:
                col = kw if kw in df.columns else None
                if col is None:
                    # multi-geo columns not expected here (geo=""), skip
                    continue
                s = pd.to_numeric(df[col], errors="coerce").dropna()
                if s.empty:
                    continue
                pts = [int(x) for x in s.tail(14).tolist()]
                spark[kw.lower()] = pts
                first, last = float(s.iloc[0]), float(s.iloc[-1])
                vel[kw.lower()] = round((last - first) / max(first, 1.0), 3)
        except Exception as e:
            print(f"   velocity skipped {batch}: {type(e).__name__}")
            continue
    return vel, spark


def fetch_regions(terms):
    """interest_by_region per 5-term batch -> {term: [top 3 countries]}."""
    out = {}
    if not terms:
        return out
    pt = client()
    for batch in _batch(terms, 5):
        try:
            time.sleep(random.uniform(8, 15))  # calm: Google already throttled the base run
            pt.build_payload(batch, timeframe="now 7-d", geo="")
            df = pt.interest_by_region(resolution="COUNTRY", inc_low_vol=True)
            if df is None or df.empty:
                continue
            for kw in batch:
                if kw not in df.columns:
                    continue
                s = pd.to_numeric(df[kw], errors="coerce").fillna(0)
                top = s[s > 0].sort_values(ascending=False).head(3).index.tolist()
                if top:
                    out[kw.lower()] = top
        except Exception as e:
            print(f"   regions skipped {batch}: {type(e).__name__}")
            continue
    return out


def fetch_topics(terms, timeframe="now 7-d", geo=""):
    """related_topics for top terms -> {term: [{title, type, value}]}."""
    out = {}
    if not terms:
        return out
    pt = client()
    for kw in terms[:6]:  # hard cap: topics are expensive
        try:
            time.sleep(random.uniform(8, 15))
            pt.build_payload([kw], timeframe=timeframe, geo=geo)
            data = pt.related_topics().get(kw) or {}
            ents = []
            for kind in ("rising", "top"):
                tdf = data.get(kind)
                if tdf is not None and not tdf.empty:
                    for _, r in tdf.head(3).iterrows():
                        ents.append(dict(title=str(r.get("topic_title", "")),
                                         type=str(r.get("topic_type", "")),
                                         value=int(r.get("value", 0))))
            if ents:
                out[kw.lower()] = ents
        except Exception as e:
            print(f"   topics skipped {kw}: {type(e).__name__}")
            continue
    return out


# ---------------------------------------------------------------- 2. score + group
def score(kind, value):
    if kind == "rising":
        return math.log1p(value)                       # value = % growth
    if kind == "rss":
        return math.log1p(value) * 1.2                 # value = approx. searches today
    if kind in ("topic_rising",):
        return math.log1p(value) * 0.8
    return value / 100 * 0.25                          # "top" queries matter less


def build_themes(df, vel=None, sparks=None, regions=None, topics=None):
    """Group similar terms. A theme gets a bonus for every country where it is RISING."""
    vel, sparks, regions, topics = vel or {}, sparks or {}, regions or {}, topics or {}
    df = df.copy()
    # story column may hold dicts after enrichment; keep a text version for grouping
    terms = (df.groupby("term")
               .agg(score=("score", "sum"),
                    seen=("country", lambda s: set(s)),
                    story=("story", lambda s: next((x for x in s if x), "")),
                    news_url=("news_url", lambda s: next((x for x in s if x), "")) if "news_url" in df.columns else ("story", lambda s: ""),
                    news_source=("news_source", lambda s: next((x for x in s if x), "")) if "news_source" in df.columns else ("story", lambda s: ""),
                    picture=("picture", lambda s: next((x for x in s if x), "")) if "picture" in df.columns else ("story", lambda s: ""))
               .sort_values("score", ascending=False))
    hot = df[df.kind != "top"].groupby("term").country.agg(set)    # countries where it is rising
    themes = []
    for term, r in terms.iterrows():
        w = {x for x in tokens(term) if len(x) > 2 and x not in GENERIC} or set(tokens(term))
        if not w:
            continue
        h = hot.get(term, set())
        for t in themes:
            if len(w & t["words"]) / len(w | t["words"]) >= 0.34:   # similar enough
                t["terms"].append(term)
                t["score"] += r.score
                t["seen"] |= r.seen
                t["hot"] |= h
                t["words"] |= w
                if r.story:
                    t["stories"].append({"title": r.story, "source": r.news_source,
                                        "url": r.news_url})
                if not t["image"] and r.picture:
                    t["image"] = r.picture
                break
        else:
            themes.append(dict(name=term, words=w, terms=[term], score=r.score, seen=set(r.seen),
                               hot=set(h),
                               stories=[{"title": r.story, "source": r.news_source,
                                         "url": r.news_url}] if r.story else [],
                               image=r.picture or ""))
    out = []
    for t in themes:
        boost = 1 + 0.4 * (max(1, len(t["hot"])) - 1)
        member_vel = [vel.get(x, 0) for x in t["terms"] if x in vel]
        avg_vel = round(sum(member_vel) / len(member_vel), 3) if member_vel else 0
        top_regs = sorted({rg for x in t["terms"] for rg in regions.get(x, [])})
        ents = [e for x in t["terms"] for e in topics.get(x, [])][:5]
        spark = []
        for x in t["terms"]:
            if x in sparks:
                spark = sparks[x]
                break
        out.append(dict(name=t["name"], score=round(t["score"] * boost, 1),
                        velocity_7d=avg_vel,
                        rising_in=sorted(t["hot"]), seen_in=sorted(t["seen"]),
                        top_regions=top_regs,
                        terms=t["terms"][:10],
                        entities=ents,
                        stories=t["stories"][:3],
                        image=t["image"],
                        sparkline=spark))
    return sorted(out, key=lambda t: t["score"], reverse=True)


# ---------------------------------------------------------------- run
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--countries", nargs="+", default=list(GEOS))
    ap.add_argument("--timeframe", default="now 7-d")
    ap.add_argument("--rss-only", action="store_true", help="skip pytrends")
    ap.add_argument("--workers", type=int, default=3, help="keywords processed in parallel")
    ap.add_argument("--no-enrich", action="store_true", help="skip velocity/region/topic enrichment")
    ap.add_argument("--enrich-top", type=int, default=8, help="how many top rising terms to enrich (default 8, keep low: Google 429s fast)")
    ap.add_argument("--no-upload", action="store_true", help="skip R2 upload, keep local out/ files")
    ap.add_argument("--keep-local", action="store_true", help="keep local out/ files after R2 upload")
    a = ap.parse_args()

    rows = []
    for c in a.countries:                              # countries: one after another
        found = rss_trends(c)
        print(f"{c} :: RSS -> {len(found)} trends")
        rows += [dict(country=c, **r) for r in found]
        if a.rss_only:
            continue
        with ThreadPoolExecutor(max_workers=a.workers) as pool:     # keywords: in parallel
            results = pool.map(lambda kw: related_queries(kw, GEOS[c], a.timeframe), SEEDS)
            for kw, found in zip(SEEDS, results):
                print(f"{c} :: {kw} -> {len(found)} results")
                rows += [dict(country=c, **r) for r in found]
        # platform-specific pull for anchors (youtube + news) — quota-light, high value
        if not a.no_enrich:
            with ThreadPoolExecutor(max_workers=a.workers) as pool:
                jobs = [(kw, gp) for kw in GPROP_ANCHORS for gp in GPROPS]
                results = pool.map(lambda j: related_queries(j[0], GEOS[c], a.timeframe, gprop=j[1]), jobs)
                for (kw, gp), found in zip(jobs, results):
                    print(f"{c} :: {kw} [{gp}] -> {len(found)} results")
                    rows += [dict(country=c, **r) for r in found]
        time.sleep(random.uniform(5, 10))              # short break before the next country

    if not rows:
        raise SystemExit("No data collected. Check your connection or try --rss-only.")

    df = pd.DataFrame(rows)
    for col in ("story", "news_url", "news_source", "news_picture", "picture",
                "gprop", "timeframe"):
        if col not in df.columns:
            df[col] = ""
    keep = pd.Series([is_relevant(f"{t} {s}") for t, s in zip(df.term, df.story)], index=df.index)
    dropped, df = df[~keep], df[keep].copy()
    print(f"\nOn-topic: {len(df)} rows kept, {len(dropped)} dropped by the filter")
    if df.empty:
        raise SystemExit("Nothing on-topic found. Check TOPIC_WORDS / NOISE_WORDS at the top of main.py.")

    # ---- enrichment: velocity + regions + entities for top rising terms only
    vel, sparks, regions, topics_map = {}, {}, {}, {}
    if not a.no_enrich and not a.rss_only:
        top_terms = (df[df.kind == "rising"].groupby("term")["value"].max()
                       .sort_values(ascending=False).head(a.enrich_top).index.tolist())
        print(f"\nEnriching top {len(top_terms)} rising terms (velocity/regions/topics)...")
        vel, sparks = fetch_velocity(top_terms, timeframe="now 7-d")
        print(f"  velocity: {len(vel)} ok")
        regions = fetch_regions(top_terms)
        print(f"  regions: {len(regions)} ok")
        topics_map = fetch_topics(top_terms)
        print(f"  topics: {len(topics_map)} ok")
        # topics as extra rows so they survive in trends.csv
        extra = []
        for term, ents in topics_map.items():
            for e in ents:
                if e["title"] and is_relevant(e["title"]):
                    extra.append(dict(country="*", term=e["title"].lower().strip(),
                                      kind="topic_rising", value=float(e["value"] or 50),
                                      story=f"{e['type']}", news_url="", news_source="",
                                      news_picture="", picture="", gprop="",
                                      timeframe=a.timeframe))
        if extra:
            df = pd.concat([df, pd.DataFrame(extra)], ignore_index=True)
            print(f"  +{len(extra)} topic rows")

    df["score"] = [score(k, v) for k, v in zip(df.kind, df.value)]
    themes = build_themes(df, vel, sparks, regions, topics_map)

    print("\nTOP THEMES  (score, vel, rising in, name)")
    for t in themes[:10]:
        print(f"  {t['score']:>6}  v{t['velocity_7d']:>+6}  {','.join(t['rising_in']) or '-':<14} {t['name']}")

    review = (dropped[dropped.kind != "top"].sort_values("value", ascending=False)
              .drop_duplicates("term").head(10))
    print("\nDROPPED but fast-rising (if a real tech term is here, add its word to TOPIC_WORDS):")
    for _, r in review.iterrows():
        print(f"  {r.country}  {r.term}")

    # ---- build file contents in memory (no local files unless asked)
    blobs = [
        ("trends.csv", df.to_csv(index=False).encode("utf-8"), "text/csv"),
        ("dropped.csv", dropped.to_csv(index=False).encode("utf-8"), "text/csv"),
        ("themes.json", json.dumps(themes[:30], indent=2, ensure_ascii=False).encode("utf-8"),
         "application/json"),
    ]

    if a.no_upload:
        outdir = f"out/{dt.date.today()}"
        os.makedirs(outdir, exist_ok=True)
        for name, body, _ in blobs:
            with open(f"{outdir}/{name}", "wb") as f:
                f.write(body)
        print(f"\nSaved locally in {outdir}/ (R2 upload skipped)")
        return

    week = week_folder()
    print(f"\nUploading to R2 ({week}/)...")
    try:
        keys = upload_blobs(blobs, week)
        print(f"Saved in R2 bucket {os.getenv('R2_BUCKET')} : {', '.join(keys)}")
        print("Public URLs:")
        for k in keys:
            print(f"  {public_url(k)}")
        urls = dict(zip([b[0] for b in blobs], (public_url(k) for k in keys)))
        try:
            from db import upsert_week_trending
            row_id = upsert_week_trending(
                week,
                urls.get("themes.json"),
                urls.get("trends.csv"),
                urls.get("dropped.csv"),
                source_prefix=f"s3://{os.getenv('R2_BUCKET')}/{week}/",
            )
            print(f"week_trendings upserted (id={row_id})")
        except Exception as e:
            print(f"DB upsert FAILED ({type(e).__name__}: {e}). R2 files are safe.")
    except Exception as e:
        outdir = f"out/{dt.date.today()}"
        os.makedirs(outdir, exist_ok=True)
        for name, body, _ in blobs:
            with open(f"{outdir}/{name}", "wb") as f:
                f.write(body)
        print(f"R2 upload FAILED ({type(e).__name__}: {e}). Saved locally in {outdir}/ instead.")
        return
    if a.keep_local:
        outdir = f"out/{dt.date.today()}"
        os.makedirs(outdir, exist_ok=True)
        for name, body, _ in blobs:
            with open(f"{outdir}/{name}", "wb") as f:
                f.write(body)
        print(f"Local copy kept in {outdir}/.")


if __name__ == "__main__":
    main()
