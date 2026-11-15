#!/usr/bin/env python3
"""
Weekly SMM agent for Devalon: reads the 3 trend files, picks the strongest
theme for the business, and writes channel-ready copy (text + hashtags only).

  python smm_agent.py --src https://pub-....r2.dev/2026-W46
  python smm_agent.py                      # current week under $R2_PUBLIC_URL
  python smm_agent.py --src out/2026-11-14 --dry-run

Model: OpenCode Go inference API (stateless, no sessions, no cache).
Env: OPENCODE_API_KEY (or OPENCODE_KEY).

Input: week_trendings row (--week-label or latest) or --src / explicit URLs.
Output: posts table in Turso (db-only, versioned per week, no local files).
"""
import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import os
import re
import sys
import time
import urllib.request
from typing import TypedDict

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, START, END
from pydantic import BaseModel, Field

load_dotenv()

# ---------------------------------------------------------------- config
BASE_URL = "https://opencode.ai/inference/go/openai/v1"
DEFAULT_MODEL = "gemini-4-argon"
UA = "devalon-smm-agent/1.0"
SITE = "https://devalon.dev"

CHANNELS = {
    "ig_company": dict(
        label="Instagram — company",
        min_chars=220, max_chars=480, max_lines=5, tags=(10, 20), max_emoji=2, bullets=False,
        voice=(
            "3-4 punchy short lines (NO bullet list), fast contrast between what owners buy vs "
            "what still happens, ends with a low-friction keyword trigger CTA like "
            "'Comment \"AUTO\" and we will send you a 3-step breakdown.'"
        ),
    ),
    "fb_company": dict(
        label="Facebook — company",
        min_chars=200, max_chars=470, max_lines=5, tags=(10, 20), max_emoji=1, bullets=False,
        voice=(
            "short conversational mini-scene in 3-4 flowing sentences (NO bullet list), "
            "sounds like a real owner post in a business group, ends with an easy question"
        ),
    ),
    "li_company": dict(
        label="LinkedIn — company",
        min_chars=300, max_chars=650, max_lines=7, tags=(10, 20), max_emoji=1, bullets=True,
        voice=(
            "'we' voice, structured B2B breakdown: 1 trend line + 3 '•' outcome bullets + "
            "unified proof line + direct CTA to devalon.dev"
        ),
    ),
    "li_personal": dict(
        label="LinkedIn — personal (Marius, founder)",
        min_chars=340, max_chars=720, max_lines=6, tags=(10, 20), max_emoji=0, bullets=False,
        voice=(
            "first-person founder micro-story in flowing paragraphs (ZERO bullet lists, no '•' or '-'): "
            "starts with a real client audit/call scene Marius sees, the lesson for owners, "
            "and a warm personal close ('Send me a message here if...')"
        ),
    ),
}

UNIFIED_PROOF = "20+ systems shipped across 4 countries, always starting with a fixed-price first version."

BRAND = dict(
    name="Devalon",
    site=SITE,
    positioning="We build the software that runs your business — AI assistants, internal tools and apps — so your team stops doing repetitive work by hand.",
    services={
        "ai_agents": (
            "AI assistants that answer customers 24/7, pull data out of PDFs and invoices, "
            "update your CRM and handle routine requests — with safety limits so they never "
            "go off-script or run up a surprise bill."
        ),
        "digital_solutions": (
            "Your own internal system (CRM, booking, inventory, client portal, reporting) "
            "built around how your team actually works — replaces six spreadsheets and "
            "per-seat software fees."
        ),
        "cloud_infra": (
            "Fast, reliable hosting with daily backups, uptime alerts and one direct person "
            "to call when something breaks — no support ticket queue."
        ),
        "apps": (
            "iOS, Android and web apps your customers actually enjoy using — payments, "
            "notifications and App Store launch handled end to end."
        ),
    },
    proof=["20+ projects shipped", "9 case studies", "5+ years", "4 countries (US, DK, RO, MD)", "10+ industries"],
    icp=(
        "Non-technical business owners, founders and COOs with budget (US, Denmark, Romania, Moldova) "
        "who are losing hours to manual copy-paste, slow lead follow-up, spreadsheet chaos, "
        "or software subscriptions that don't fit how their company works."
    ),
    differentiators=[
        "Built for real daily use, not a flashy demo",
        "One direct person who knows your system",
        "Fixed-price first version so you know the cost before you start",
        "20+ systems shipped across 10+ industries",
    ],
)

VISUAL_REFS = ["swipe", "carousel", "slide", "see the image", "screenshot",
               "watch the video", "in the reel", "as you can see in"]

BANNED = ["revolutionary", "game-changer", "game changer", "10x", "disrupt",
          "unlock the power", "in today's fast-paced", "elevate your", "synergy",
          "delve", "supercharge", "cutting-edge"]

# Engineer plumbing AND "how-it-works lecture" phrases that make an owner scroll past.
TECH_JARGON = [
    "schema", "payload", "5xx", "4xx", "429", "500 error", "status code",
    "retry policy", "backoff", "stack trace", "trace replay", "ci/cd",
    "git commit", "pull request", "repo ", "endpoint", "webhook", "kubernetes",
    "k8s", "docker", "node.js", "nodejs", "emulator", "sandbox", "eval harness",
    "eval set", "llmops", "mlops", "token ceiling", "context window", "latency",
    "malformed", "json", "yaml", "sdk", "devops", "microservice", "serverless",
    "vector db", "rag ", "prompt injection", "adversarial prompt", "idempotent",
    "spending cap", "spending limit", "human approval", "write down the rules",
    "browser tab", "chat window", "daily backups", "ticket queue",
    "google trends", "google search", "on google", "per google",
]

SERVICE_MAP = {
    "ai_agents": [" ai ", "artificial intelligence", "ai agent", "ai agents", "agentic",
                  "autonomous", "llm", "gpt", "chatgpt", "openai", "claude", "gemini",
                  "prompt", "rag", "mcp", "chatbot", "copilot"],
    "digital_solutions": ["crm", "erp", "saas", "portal", "booking", "inventory", "admin",
                          "workflow", "automation", "b2b", "dashboard", "back office"],
    "cloud_infra": ["devops", "kubernetes", "docker", "aws", "azure", "cloud", "pipeline",
                    "infrastructure", "terraform", "backend", "node.js", "node", "api",
                    "ci/cd", "serverless", "database", "cybersecurity", "cyber security",
                    "infosec", "ransomware", "malware", "security"],
    "apps": [" app", "ios", "android", "mobile", "pwa", "flutter", "react native"],
}
AUDIENCE_TERMS = ["b2b", "saas", "startup", "startups", "founder", "founders", "company",
                  "companies", "enterprise", "pricing", "cost", "roi", "workflow", "teams",
                  "agency", "business", "small business", "customer", "sales", "support",
                  "crm", "booking", "invoice", "accounting", "operations", "manager"]
DEV_NOISE_TERMS = ["emulator", "sdk", "framework", "frameworks", "node.js", "python",
                   "rust", "typescript", "github", "leaderboard", "arena", "benchmark",
                   "bootcamp", "degree", "uninstall", "tutorial", "for beginners"]
# monetising these is tone-deaf or brand-risky
SAFETY = ["north korea", "war", "shooting", "died", "death", "killed", "terror",
          "movie", "film", "trailer", "celebrity", "actor", "actress", "xbox",
          "playstation", "gta", "fable", "recipe", "pizza", "near me", " vs ", "match",
          "league", "standings", "lottery", "casino", "betting", "sues ", "lawsuit",
          "bahu", "achaar", "kyunki"]

BRAND_NUMBERS = {20, 9, 5, 4, 10, 50}


# ---------------------------------------------------------------- data loading
def week_folder(today=None):
    y, w, _ = (today or dt.date.today()).isocalendar()
    return f"{y}-W{w:02d}"


def read_any(target, retries=2):
    """Read a URL, file:// URL or local path. Returns bytes."""
    last = None
    for attempt in range(retries + 1):
        try:
            if target.startswith("http://") or target.startswith("https://"):
                req = urllib.request.Request(target, headers={"User-Agent": UA})
                with urllib.request.urlopen(req, timeout=25) as r:
                    data = r.read()
                    if len(data) > 12 * 1024 * 1024:
                        raise ValueError("file larger than 12MB")
                    return data
            path = target[7:] if target.startswith("file://") else target
            with open(path, "rb") as f:
                return f.read()
        except Exception as e:                      # noqa: BLE001 - surface verbatim
            last = e
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
    raise SystemExit(f"Cannot read {target}: {type(last).__name__}: {last}")


def parse_themes(raw_bytes, target):
    try:
        data = json.loads(raw_bytes.decode("utf-8"))
    except Exception as e:                          # noqa: BLE001
        raise SystemExit(f"themes.json is not valid JSON ({target}): {e}. "
                         f"First bytes: {raw_bytes[:160]!r}")
    if not isinstance(data, list):
        raise SystemExit(f"themes.json must be a list, got {type(data).__name__} ({target})")
    if data and not all(isinstance(x, dict) and "name" in x for x in data[:3]):
        raise SystemExit(f"themes.json items missing 'name' ({target})")
    return data


def parse_csv(raw_bytes, target, required):
    text = raw_bytes.decode("utf-8", errors="replace")
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        raise SystemExit(f"{target} is empty")
    missing = [c for c in required if c not in rows[0]]
    if missing:
        raise SystemExit(f"{target} missing columns {missing}; found {list(rows[0])}")
    return rows


def resolve_inputs(src, week, themes_url, trends_url, dropped_url, no_dropped):
    """--src may be a full week prefix or a bucket root; derive the 3 file targets."""
    if not src:
        src = os.getenv("R2_PUBLIC_URL", "").rstrip("/")
        if not src:
            raise SystemExit("No --src and no R2_PUBLIC_URL in .env")
        src = f"{src}/{week}"
    src = src.rstrip("/")
    m = re.search(r"(\d{4}-W\d{2})$", src)
    local_has_files = (not src.startswith("http")
                       and os.path.exists(os.path.join(src, "themes.json")))
    if m:
        week = m.group(1)
    elif not re.search(r"\d{4}-W\d{2}", src) and not local_has_files:
        src = f"{src}/{week}"

    targets = {
        "themes": themes_url or f"{src}/themes.json",
        "trends": trends_url or f"{src}/trends.csv",
        "dropped": None if no_dropped else (dropped_url or f"{src}/dropped.csv"),
    }
    return src, week, targets


PLATFORM_MAP = {
    "ig_company": ("instagram", "company"),
    "fb_company": ("facebook", "company"),
    "li_company": ("linkedin", "company"),
    "li_personal": ("linkedin", "personal"),
}


def resolve_week_row(week_label=None):
    """Return (week_id, week_label, targets) from week_trendings.

    Explicit label wins, otherwise the most recent row. Targets come from
    the row's stored R2 URLs.
    """
    from db import SessionLocal
    from models import WeekTrending

    sess = SessionLocal()
    try:
        if week_label:
            row = sess.query(WeekTrending).filter_by(week_label=week_label).first()
            if not row:
                raise SystemExit(f"No week_trendings row for {week_label!r}")
        else:
            row = sess.query(WeekTrending).order_by(WeekTrending.id.desc()).first()
            if not row:
                raise SystemExit("week_trendings is empty; run main.py first "
                                 "or pass --src / --week-label")
        return (row.id, row.week_label,
                dict(themes=row.themes_url, trends=row.trends_url,
                     dropped=row.dropped_url))
    finally:
        sess.close()


# ---------------------------------------------------------------- scoring (no LLM)
def term_service(term):
    t = f" {term.lower()} "
    best, hits = None, 0
    for svc, keys in SERVICE_MAP.items():
        n = sum(1 for k in keys if k in t)
        if n > hits:
            best, hits = svc, n
    return best or "digital_solutions"


def build_candidates(trends, themes):
    by_term = {}
    for r in trends:
        term = (r.get("term") or "").strip().lower()
        if not term:
            continue
        try:
            value = float(r.get("value") or 0)
        except ValueError:
            value = 0.0
        c = by_term.setdefault(term, dict(term=term, value=0.0, kinds=set(), sources=set(),
                                         countries=set(), rows=[]))
        c["value"] = max(c["value"], value)
        c["kinds"].add(r.get("kind") or "")
        c["sources"].add((r.get("gprop") or "web") + "/" + (r.get("country") or "?"))
        c["countries"].add(r.get("country") or "?")
        c["rows"].append(r)

    theme_score, theme_terms = {}, {}
    for t in themes:
        t_terms = [str(x).lower() for x in (t.get("terms") or [t.get("name", "")]) if x]
        for term in t_terms:
            sc = float(t.get("score") or 0)
            if sc >= theme_score.get(term, 0):
                theme_score[term] = sc
                theme_terms[term] = t_terms

    out = []
    for term, c in by_term.items():
        low = term.lower()
        safety = [w for w in SAFETY if w in low]
        svc = term_service(term)
        audience = sum(1 for w in AUDIENCE_TERMS if w in low)
        dev_noise = sum(1 for w in DEV_NOISE_TERMS if w in low)
        corroboration = len({s.split("/")[0] for s in c["sources"]})
        country_spread = len(c["countries"])
        rising = 1.0 if any(k in ("rising", "rss", "topic_rising") for k in c["kinds"]) else 0.0
        score = (
            1.0 * (c["value"] and __import__("math").log1p(c["value"]) or 0)
            + 1.6 * corroboration
            + 1.2 * country_spread
            + 2.2 * rising
            + 2.4 * audience
            - 3.0 * dev_noise
            + 0.06 * theme_score.get(term, 0)
            - 6.0 * len(safety)
        )
        out.append(dict(
            term=term, score=round(score, 2), service=svc, value=c["value"],
            kinds=sorted(k for k in c["kinds"] if k),
            sources=sorted(c["sources"]), countries=sorted(c["countries"]),
            theme_score=round(theme_score.get(term, 0), 1),
            theme_terms=theme_terms.get(term, [term]),
            safety_flags=safety,
            evidence=[
                f"{r.get('country')}|{r.get('kind')}|{r.get('value')}"
                + (f"|{r.get('gprop')}" if r.get("gprop") else "")
                for r in sorted(c["rows"], key=lambda x: -float(x.get("value") or 0))[:4]
            ],
        ))
    out.sort(key=lambda x: -x["score"])
    return [c for c in out if not c["safety_flags"]]


def allowed_numbers(trends):
    nums = set()
    for r in trends:
        try:
            nums.add(float(r.get("value") or 0))
        except ValueError:
            pass
    return {n for n in nums if n > 0}


# ---------------------------------------------------------------- LLM
class Selection(BaseModel):
    chosen_index: int = Field(description="Index from the candidate list")
    chosen_term: str
    service: str = Field(description="ai_agents | digital_solutions | cloud_infra | apps")
    why: str = Field(description="2-4 sentences: why this wins for Devalon this week")
    angle_hint: str = Field(description="The sharpest business angle to build content on")
    backups: list[str] = Field(default_factory=list)
    rejected_reason: str = Field(description="Why the obvious alternatives lost")


class ChannelAngle(BaseModel):
    hook: str
    lever: str = Field(description="specificity | loss-aversion | curiosity | proof | urgency")
    promise: str


class Angles(BaseModel):
    channels: dict[str, ChannelAngle]


class Post(BaseModel):
    channel: str
    headline: str = Field(description="Section 1: punchy 1-line title/hook, max 12 words, no hashtags")
    body: str = Field(description="Section 2: short main content + 1-line CTA at end, no hashtags")
    hashtags: list[str] = Field(description="Section 3: 10-20 bare CamelCase tags without '#', grounded in theme/content")


class PostSet(BaseModel):
    posts: list[Post]


class Issue(BaseModel):
    channel: str
    severity: str = Field(description="error | warn")
    problem: str
    fix: str


class Critique(BaseModel):
    issues: list[Issue] = Field(default_factory=list)
    verdict: str


def api_key():
    key = os.getenv("OPENCODE_API_KEY")
    if not key:
        raise SystemExit("Missing OPENCODE_API_KEY in .env")
    return key


def make_llm(model, max_tokens=4000, session=None):
    return ChatOpenAI(
        base_url=BASE_URL,
        api_key=api_key(),
        model=model,
        max_tokens=max_tokens,
        timeout=240,
        max_retries=2,
        default_headers={"User-Agent": UA,
                         "x-opencode-session": session or week_folder()},
    )


def _extract_json(text):
    if not text:
        return None
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end <= start:
        return None
    try:
        return json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return None


SKELETONS = {
    "Selection": (
        '{"chosen_index": 0, "chosen_term": "<exact term from list>", '
        '"service": "ai_agents|digital_solutions|cloud_infra|apps", '
        '"why": "<2-4 sentences>", "angle_hint": "<one sharp angle>", '
        '"backups": ["<term>", "<term>"], "rejected_reason": "<why alternatives lost>"}'
    ),
    "Angles": (
        '{"channels": {"ig_company": {"hook": "<15 words max>", "lever": '
        '"specificity|loss-aversion|curiosity|proof|urgency", "promise": "<reader benefit>"}, '
        '"fb_company": {...}, "li_company": {...}, "li_personal": {...}}}'
    ),
    "PostSet": (
        '{"posts": [{"channel": "ig_company", "headline": "<1-line title, max 12 words>", '
        '"body": "<3-5 short lines + 1-line CTA at end, NO hashtags inside body>", '
        '"hashtags": ["Tag1FromTheme", "Tag2FromTheme", "Tag3Service", "... 10-20 total"]}, ...one object per channel...]}'
    ),
    "Critique": (
        '{"issues": [{"channel": "ig_company", "severity": "error|warn", '
        '"problem": "<what is wrong>", "fix": "<exact instruction>"}], '
        '"verdict": "<one sentence>"}'
    ),
}


def ask_json(llm, system, user, schema, skeleton):
    """Structured output with graceful degradation and a reformat repair pass.

    The Go gateway ignores OpenAI response_format schemas, so the JSON contract is
    also stated in the prompt and any stray key naming gets repaired once.
    """
    msgs = [("system", system + "\n\nJSON contract:\n" + skeleton),
            ("human", user + "\n\nJSON contract:\n" + skeleton)]
    last_err, last_data = None, None
    for method in ("json_schema", "json_mode", None):
        try:
            if method:
                out = llm.with_structured_output(schema, method=method).invoke(msgs)
                if isinstance(out, schema):
                    return out
                if isinstance(out, dict):
                    last_data = out
                    return schema.model_validate(out)
                last_err = ValueError(f"unexpected structured output: {type(out)}")
                continue
            raw = llm.invoke(msgs)
            content = getattr(raw, "content", "") or ""
            data = _extract_json(content)
            if data is None:
                last_err = ValueError(f"no JSON in reply: {content[:200]!r}")
                continue
            last_data = data
            return schema.model_validate(data)
        except Exception as e:                      # noqa: BLE001
            last_err = e
            if isinstance(e, Exception) and "model_validate" in str(e):
                pass
            time.sleep(1.0)

    # last resort: ask the model to reformat whatever JSON it produced
    if last_data is not None:
        try:
            fix = llm.invoke([("system", "You convert JSON into a target schema. Output JSON only."),
                              ("human", "Reformat this JSON to match the contract exactly "
                                        "(rename keys, fill missing fields from context):\n"
                                        + json.dumps(last_data)[:4000]
                                        + "\n\nContract:\n" + skeleton)])
            data = _extract_json(getattr(fix, "content", "") or "")
            if data is not None:
                return schema.model_validate(data)
        except Exception as e:                      # noqa: BLE001
            last_err = e
    raise SystemExit(f"LLM call failed: {type(last_err).__name__}: {last_err}")


# ---------------------------------------------------------------- prompts
SYSTEM = (
    "You are a senior social-media and B2B sales writer for Devalon (devalon.dev). "
    "Devalon builds AI assistants, internal business tools (CRMs, booking, inventory, portals) "
    "and custom apps for business owners and founders. "
    "CRITICAL RULE: you are writing for NON-TECHNICAL business owners and executives with budget — "
    "people who do not code and do not care how software works under the hood. "
    "They care about: wasted payroll hours, slow replies to leads, staff copying data between "
    "spreadsheets, expensive software seats nobody uses, errors that embarrass them in front of "
    "clients, and projects that drag on. "
    "Speak in plain everyday business language (customers, leads, invoices, bookings, hours, "
    "payroll, margin, peace of mind). NEVER use engineering jargon (no schema, payload, API, "
    "5xx/429, retry, backoff, trace, CI/CD, repo, Docker, Kubernetes, Node.js, emulator, sandbox, "
    "eval harness, LLMOps, latency, tokens, context window). "
    "If the underlying trend is technical, translate what it means for a business owner in plain words. "
    "Never invent numbers, client names or results. Every statistic must come from the trend data. "
    "Trend values with kind=rising are PERCENT GROWTH, not search counts — never call them searches "
    "or users unless kind=rss."
)


def brand_block():
    svc = "\n".join(f"- {k}: {v}" for k, v in BRAND["services"].items())
    return (f"Company: {BRAND['name']} ({BRAND['site']})\n"
            f"Positioning: {BRAND['positioning']}\nServices:\n{svc}\n"
            f"Proof: {', '.join(BRAND['proof'])}\nBuyer (ICP): {BRAND['icp']}\n"
            f"Why buyers pick us: {'; '.join(BRAND['differentiators'])}")


def candidates_block(cands, limit=40):
    lines = []
    for i, c in enumerate(cands[:limit]):
        lines.append(f"{i}. {c['term']} | score={c['score']} | svc={c['service']} "
                     f"| value={c['value']:.0f} | src={','.join(c['sources'])[:90]}")
        lines.append(f"   evidence: {'; '.join(c['evidence'])}")
    return "\n".join(lines)


def select_prompt(cands):
    user = (
        f"{brand_block()}\n\n"
        "This week's on-topic US trend candidates (already filtered for brand safety):\n"
        f"{candidates_block(cands)}\n\n"
        "Pick the ONE candidate that gives the strongest sales conversation with a NON-TECHNICAL "
        "business owner or founder this week. Prefer topics that connect directly to money, time, "
        "customers, operations or risk — or a technical trend you can cleanly translate into a "
        "business problem an owner recognizes immediately. Avoid developer-only inside baseball "
        "that means nothing to someone who signs checks.\n\n"
        "Return JSON only."
    )
    return user


def angle_prompt(sel, cand):
    user = (
        f"{brand_block()}\n\n"
        f"Chosen trend: {sel.chosen_term} (service: {sel.service})\n"
        f"Why: {sel.why}\nBusiness angle: {sel.angle_hint}\n"
        f"Evidence rows: {'; '.join(cand['evidence'])}\n\n"
        "Channels: ig_company, fb_company, li_company, li_personal (Marius, founder).\n"
        "For each channel give: hook (the first line), the persuasion lever used, and the promise "
        "the reader gets. Hooks must be 15 words max, written in plain business English a "
        "non-technical owner feels in their gut (time, money, staff, customers — zero tech jargon). "
        "Return JSON only."
    )
    return user


def write_prompt(sel, angles, cand):
    specs = []
    for key, ch in CHANNELS.items():
        lo, hi = ch["tags"]
        specs.append(f"- {key}: {ch['voice']}; body {ch['min_chars']}-{ch['max_chars']} chars "
                     f"(max {ch['max_lines']} lines); {lo}-{hi} hashtags; max {ch['max_emoji']} emoji")
    hooks = "\n".join(f"- {k}: headline={v.hook!r} lever={v.lever} promise={v.promise!r}"
                      for k, v in angles.channels.items())
    related = ", ".join(cand.get("theme_terms") or [sel.chosen_term])
    backups = ", ".join(sel.backups) if sel.backups else "none"
    user = (
        f"{brand_block()}\n\n"
        f"Chosen trend: {sel.chosen_term} (maps to service: {sel.service})\n"
        f"Related terms in this theme: {related}\n"
        f"Other rising themes considered: {backups}\n"
        f"Business angle: {sel.angle_hint}\n"
        f"Evidence (country|kind|value|gprop): {'; '.join(cand['evidence'])}\n"
        f"Family of related terms: {', '.join(cand['kinds'])}; sources: {', '.join(cand['sources'])}\n\n"
        f"Headlines already chosen:\n{hooks}\n\n"
        "Write the 4 posts for NON-TECHNICAL business owners. Every post has 3 sections:\n"
        "  1) headline — 1 punchy title line, max 12 words, no hashtags.\n"
        "  2) body — SHORT main description (3-5 short lines + 1-line CTA at the end). "
        "Do NOT repeat the headline inside body, and do NOT put any #hashtags inside body.\n"
        "  3) hashtags — 10-20 bare CamelCase tags without '#', generated by YOU specifically from the "
        f"chosen theme ({sel.chosen_term}), its related terms ({related}), the matched service "
        f"({sel.service}), and the software / AI / marketing / business angle of that specific post. "
        "Every channel's tag set (10-20 tags) must reflect its own angle and audience. "
        "Tag mix per post: ~5-7 exact theme tags from chosen term + related terms, "
        "~4-6 service/problem tags (what the post actually solves: invoices, CRM, leads, bookings), "
        "~3-7 audience/trending tags for owners/founders on that channel. "
        "Every tag must map to a word or phrase in that post's headline/body/theme — no generic filler "
        "(never use lone #Business, #Success, #Marketing, #AI, #Work). Prefer multi-word CamelCase "
        "(ChatgptForBusiness, InvoiceAutomation, LeadFollowUp, SmallBusinessOwners). "
        "No duplicates (case-insensitive), no company-name tags. "
        "Never include #Devalon or any company-name tag.\n\n"
        "Rules for body (must read in 5 seconds, and each channel MUST have a different shape):\n"
        "- Research framing rule: NEVER mention 'Google' or 'Google Trends'. Frame the weekly signal "
        "as Devalon's own weekly market research and buyer demand analysis (e.g. 'In our weekly market "
        "research across US buyers, demand for ... jumped 3,300% this week' or 'Every Monday our team "
        "analyzes what business owners are actively looking for...').\n"
        "- Channel-specific shapes (strictly enforced):\n"
        "  * ig_company: 3-4 short punchy lines, NO bullets ('•' or '-'). End with a keyword trigger CTA "
        "in quotes (e.g. Comment \"AUTO\" or DM \"FLOW\" and we'll send ...).\n"
        "  * fb_company: 3-4 conversational sentences telling a quick mini-scene, NO bullets ('•' or '-'). "
        "End with a simple question that invites comments.\n"
        "  * li_company: the ONLY channel that uses '•' bullet points (1 research insight line + 3 '•' outcome bullets "
        "+ proof line + devalon.dev CTA).\n"
        "  * li_personal: Marius (founder) in first person telling a quick micro-story from client calls "
        "and what our weekly research revealed, in flowing paragraphs — ZERO bullets ('•' or '-'). Conversational personal CTA.\n"
        f"- Unified proof anchor across all channels: '{UNIFIED_PROOF}' (keep the same numbers across "
        "company and personal posts: 20+ systems/projects across 4 countries, fixed-price first version).\n"
        "- Talk ONLY about business results, NEVER about how it works under the hood.\n"
        "- Forbidden words/phrases: " + ", ".join(TECH_JARGON) + ".\n"
        "- Use only numbers from the trend evidence (you may round: 16,500 -> 16.5k) or Devalon's "
        "proof points (20+ projects, 9 case studies, 5+ years, 4 countries, 10+ industries).\n"
        "- Never invent dollar amounts, hour counts, seat counts or user counts; never imply a named company is a client.\n"
        "- Text-only posts: never mention images, slides, carousels, videos or 'swipe'.\n"
        "- Do not start headline with '#'.\n"
        f"Format limits:\n{chr(10).join(specs)}\n\n"
        "Return JSON with a 'posts' array of 4 objects (channel, headline, body, hashtags)."
    )
    return user


def _fmt_posts(postset):
    return "\n\n".join(
        f"[{p.channel}]\nHEADLINE: {p.headline}\nBODY ({len(p.body)} chars):\n{p.body}\n"
        f"TAGS: {' '.join('#' + t for t in p.hashtags)}"
        for p in postset.posts
    )


def critique_prompt(postset):
    user = (
        f"{brand_block()}\n\nDrafts to review:\n{_fmt_posts(postset)}\n\n"
        "Review as a busy 45-year-old business owner who gives a post 5 seconds while walking "
        "between meetings. Mark severity=error if:\n"
        "- The post mentions 'Google' or 'Google Trends' instead of framing the insight as Devalon's own market research/analysis.\n"
        "- ig_company, fb_company, or li_personal uses bullet lists ('•' or '-'), or if li_company lacks '•' bullets.\n"
        "- ig_company lacks a quoted keyword trigger CTA (e.g. Comment \"AUTO\" / DM \"FLOW\").\n"
        "- Proof metrics contradict across channels (keep unified: 20+ systems/projects across 4 countries, fixed-price first version).\n"
        "- Any technical jargon, mechanism talk, or invented number appears.\n"
        "- The headline is weak/generic, starts with '#', body repeats the headline, or hashtags appear inside body.\n"
        "- Fewer than 10 or more than 20 hashtags, duplicate tags (case-insensitive), generic filler tags "
        "(#Business, #Success, #Marketing, lone #AI), company-name tags, or tags unrelated to the post's theme/angle.\n"
        "Return JSON only."
    )
    return user


def revise_prompt(postset, issues, verify_errors=None):
    bug = "\n".join(f"- {i.channel} ({i.severity}): {i.problem} -> {i.fix}" for i in issues)
    if verify_errors:
        bug += "\n" + "\n".join(f"- VERIFIER: {e}" for e in verify_errors)
    user = (
        f"{brand_block()}\n\nCurrent drafts:\n{_fmt_posts(postset)}\n\nIssues to fix:\n{bug}\n\n"
        "Rewrite into the 3-section format (headline, body, hashtags) for non-technical owners. "
        "Keep channel shapes distinct: bullets ONLY on li_company; flowing micro-story on li_personal; "
        "punchy lines + quoted keyword CTA on ig_company; conversational mini-scene + question on fb_company. "
        "Frame the % growth stat as Devalon's own weekly market research/analysis (NEVER mention Google or Google Trends), "
        "keep proof unified (20+ systems/projects across 4 countries, fixed-price first version), no hashtags inside body, "
        "rewrite hashtags to 10-20 content-grounded CamelCase tags (theme + service/problem + audience, no generic filler, no duplicates, no company tag), "
        "and strictly inside these body limits:\n"
        + "\n".join(f"- {k}: body {c['min_chars']}-{c['max_chars']} chars, max {c['max_lines']} lines, "
                    f"{c['tags'][0]}-{c['tags'][1]} hashtags"
                    for k, c in CHANNELS.items()) + "\n"
        "Return the full JSON post set."
    )
    return user


# ---------------------------------------------------------------- graph
class State(TypedDict, total=False):
    src: str
    week: str
    targets: dict
    provenance: dict
    themes: list
    trends: list
    dropped: list
    brand: dict
    candidates: list
    selection: dict
    angles: dict
    posts: list
    issues: list
    revisions: int
    verify_rounds: int
    verify_errors: list
    revise_target: str
    model: str
    week_id: int
    week_label: str
    saved_version: int
    dry_run: bool


def node_load(s: State):
    prov, data = {}, {}
    for key, target in s["targets"].items():
        if not target:
            data[key] = []
            prov[key] = dict(url=None, skipped=True)
            continue
        t0 = time.time()
        raw = read_any(target)
        if key == "themes":
            data[key] = parse_themes(raw, target)
        elif key == "trends":
            data[key] = parse_csv(raw, target, ["country", "term", "kind", "value"])
        else:
            data[key] = parse_csv(raw, target, ["term"])
        prov[key] = dict(url=target, bytes=len(raw), rows=len(data[key]),
                         sha256=hashlib.sha256(raw).hexdigest()[:16],
                         fetched_at=dt.datetime.now().isoformat(timespec="seconds"),
                         ms=int((time.time() - t0) * 1000))
        print(f"  {key}: {len(data[key])} rows <- {target}")
    return dict(themes=data["themes"], trends=data["trends"], dropped=data["dropped"],
                provenance=prov)


def node_brand(s: State):
    ok, title = False, ""
    try:
        req = urllib.request.Request(SITE, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="replace")
        title = (re.search(r"<title>(.*?)</title>", html, re.S) or [None, ""])[1].strip()
        ok = bool(title)
    except Exception as e:                          # noqa: BLE001
        print(f"  brand fetch failed ({type(e).__name__}), using built-in brief")
    brand = dict(BRAND)
    brand["site_reachable"] = ok
    brand["site_title"] = title
    return dict(brand=brand)


def node_score(s: State):
    cands = build_candidates(s["trends"], s["themes"])
    print(f"  candidates: {len(cands)} (after brand-safety filter)")
    return dict(candidates=cands)


def node_select(s: State):
    cands = s["candidates"]
    if not cands:
        raise SystemExit("No candidates survived filtering; check the source files.")
    llm = make_llm(s["model"], 8000, s["week"])
    sel = ask_json(llm, SYSTEM, select_prompt(cands), Selection, SKELETONS["Selection"])
    if not (0 <= sel.chosen_index < len(cands)):
        sel.chosen_index = 0
        sel.chosen_term = cands[0]["term"]
    chosen = cands[sel.chosen_index]
    if chosen["term"].lower() != sel.chosen_term.lower():
        sel.chosen_term = chosen["term"]
    print(f"  chosen: {sel.chosen_term} (svc={sel.service})")
    print(f"  why: {sel.why}")
    return dict(selection=sel.model_dump(), candidates=cands)


def node_angle(s: State):
    sel = Selection.model_validate(s["selection"])
    cand = next(c for c in s["candidates"] if c["term"] == sel.chosen_term)
    llm = make_llm(s["model"], 8000, s["week"])
    ang = ask_json(llm, SYSTEM, angle_prompt(sel, cand), Angles, SKELETONS["Angles"])
    missing = [k for k in CHANNELS if k not in ang.channels]
    if missing:
        raise SystemExit(f"angle node missing channels: {missing}")
    return dict(angles={k: v.model_dump() for k, v in ang.channels.items()})


def node_write(s: State):
    sel = Selection.model_validate(s["selection"])
    ang = Angles.model_validate({"channels": s["angles"]})
    cand = next(c for c in s["candidates"] if c["term"] == sel.chosen_term)
    llm = make_llm(s["model"], 8000, s["week"])
    posts = ask_json(llm, SYSTEM, write_prompt(sel, ang, cand), PostSet, SKELETONS["PostSet"])
    print(f"  wrote {len(posts.posts)} posts")
    return dict(posts=[p.model_dump() for p in posts.posts])


def node_critique(s: State):
    postset = PostSet.model_validate({"posts": s["posts"]})
    llm = make_llm(s["model"], 8000, s["week"])
    crit = ask_json(llm, SYSTEM, critique_prompt(postset), Critique, SKELETONS["Critique"])
    issues = [i.model_dump() for i in crit.issues]
    errs = [i for i in issues if i["severity"] == "error"]
    print(f"  critique: {len(issues)} issues ({len(errs)} errors) — {crit.verdict[:120]}")
    return dict(issues=issues)


def node_revise(s: State):
    postset = PostSet.model_validate({"posts": s["posts"]})
    issues = [Issue.model_validate(i) for i in s.get("issues", [])]
    llm = make_llm(s["model"], 8000, s["week"])
    fixed = ask_json(llm, SYSTEM, revise_prompt(postset, issues, s.get("verify_errors")),
                     PostSet, SKELETONS["PostSet"])
    print(f"  revised (round {s.get('revisions', 0) + 1})")
    return dict(posts=[p.model_dump() for p in fixed.posts],
                revisions=s.get("revisions", 0) + 1,
                verify_errors=[])


def normalize_number(token, suffix):
    try:
        val = float(token.replace(",", ""))
    except ValueError:
        return None
    if suffix and suffix.lower() == "k":
        val *= 1000
    elif suffix and suffix.lower() == "m":
        val *= 1_000_000
    return val


def node_verify(s: State):
    errors, warns = [], []
    allowed = allowed_numbers(s["trends"])
    allowed_all = sorted(allowed)

    def traceable(v):
        if v <= 12 or int(round(v)) in BRAND_NUMBERS:
            return True
        return any(abs(v - a) <= max(1.0, 0.03 * a) for a in allowed_all)

    for p in s["posts"]:
        ch = CHANNELS.get(p["channel"])
        if not ch:
            errors.append(f"unknown channel {p['channel']}")
            continue
        head = (p.get("headline") or "").strip()
        body = (p.get("body") or "").replace(" | ", "\n")
        body = re.sub(r"\s*•\s*", "\n• ", body).strip()
        p["body"] = body
        text = f"{head}\n{body}"
        if not head or len(head) > 100 or "\n" in head:
            errors.append(f"{p['channel']}: headline must be 1 line <= 100 chars (got {len(head)})")
        n = len(body)
        if n > ch["max_chars"]:
            errors.append(f"{p['channel']}: body {n} chars > {ch['max_chars']} — make it shorter")
        if n < ch["min_chars"]:
            warns.append(f"{p['channel']}: body only {n} chars (target {ch['min_chars']}+)")
        non_empty_lines = [ln for ln in body.splitlines() if ln.strip()]
        if len(non_empty_lines) > ch["max_lines"]:
            errors.append(f"{p['channel']}: body has {len(non_empty_lines)} lines (max {ch['max_lines']})")
        has_bullets = any(re.match(r"^\s*(?:[•\-\*]|\d+\.)\s+", ln) for ln in non_empty_lines)
        if ch.get("bullets") and not has_bullets:
            errors.append(f"{p['channel']}: must include '•' outcome bullet lines")
        if not ch.get("bullets") and has_bullets:
            errors.append(f"{p['channel']}: no bullet lists allowed on this channel — use flowing lines/paragraphs")
        if p["channel"] == "ig_company" and not re.search(r"(?:comment|dm)\s+[\"'“”‘’][A-Za-z0-9_-]+[\"'“”‘’]", body, re.I):
            errors.append("ig_company: must end with a quoted keyword trigger CTA (e.g. Comment \"AUTO\" or DM \"FLOW\")")
        if "google" in text.lower():
            errors.append(f"{p['channel']}: do not mention Google/Google Trends — frame as Devalon's own market research/analysis")
        if re.search(r"(?:^|\s)#[A-Za-z]", text):
            errors.append(f"{p['channel']}: do not put #hashtags inside headline or body")
        lo, hi = ch["tags"]
        if not (lo <= len(p["hashtags"]) <= hi):
            errors.append(f"{p['channel']}: {len(p['hashtags'])} hashtags (need {lo}-{hi})")
        seen_tags = set()
        GENERIC_TAGS = {"business", "success", "marketing", "ai", "work", "love",
                        "instagood", "viral", "trending", "explore"}
        for tag in p["hashtags"]:
            clean_tag = tag.strip("#").strip()
            if not re.fullmatch(r"[A-Za-z0-9]+", clean_tag):
                errors.append(f"{p['channel']}: bad hashtag #{tag}")
                continue
            low_tag = clean_tag.lower()
            if low_tag in seen_tags:
                errors.append(f"{p['channel']}: duplicate hashtag #{clean_tag}")
            seen_tags.add(low_tag)
            if clean_tag.lower() == "devalon":
                errors.append(f"{p['channel']}: do not include #Devalon — use only topic/industry tags")
            if low_tag in GENERIC_TAGS:
                errors.append(f"{p['channel']}: generic filler hashtag #{clean_tag} — use content-specific multi-word tag")
        low = text.lower()
        for b in BANNED:
            if b in low:
                errors.append(f"{p['channel']}: banned phrase '{b}'")
        for j in TECH_JARGON:
            if j in low:
                errors.append(f"{p['channel']}: tech jargon '{j.strip()}' — rewrite in plain business words")
        for v in VISUAL_REFS:
            if v in low:
                errors.append(f"{p['channel']}: references a visual ('{v}') but posts are text-only")
        if re.search(r"\[[^\]]+\]|TBD|TODO|XXXX", text):
            errors.append(f"{p['channel']}: placeholder text left in draft")
        if ch["max_emoji"] == 0 and re.search(r"[\U0001F300-\U0001FAFF]", text):
            errors.append(f"{p['channel']}: emoji not allowed")
        for m in re.finditer(r"(?<![\w.])(\d[\d,]*(?:\.\d+)?)\s?([kKmM])?(?![\w])", text):
            val = normalize_number(m.group(1), m.group(2))
            if val is None:
                continue
            if val >= 1000 and not traceable(val):
                errors.append(f"{p['channel']}: number {m.group(0)!r} not in trend data")

    for w in warns:
        print(f"  warn: {w}")
    for e in errors:
        print(f"  verify error: {e}")
    if not errors:
        print("  verify: clean")
    return dict(verify_errors=errors, verify_rounds=s.get("verify_rounds", 0) + 1)


def node_export(s: State):
    """Save the 4 verified posts to the DB only — no local files.

    Version auto-increments per week so reruns keep history (v1, v2, ...).
    """
    from sqlalchemy import func, select

    from db import SessionLocal
    from models import Post as PostRow

    week_id = s["week_id"]
    sess = SessionLocal()
    try:
        current = sess.execute(
            select(func.max(PostRow.version)).where(PostRow.week_id == week_id)
        ).scalar()
        version = (current or 0) + 1
        channels = {p["channel"]: p for p in s["posts"]}
        rows = []
        for key, (platform, account_type) in PLATFORM_MAP.items():
            p = channels.get(key)
            if not p:
                continue
            rows.append(PostRow(
                week_id=week_id,
                platform=platform,
                account_type=account_type,
                headline=p["headline"].strip().lstrip("#").strip(),
                body=re.sub(r"(?:\s+#[A-Za-z0-9_]+)+\s*$", "",
                            p["body"].strip()).strip(),
                hashtags=[t.lstrip("#") for t in p["hashtags"]],
                model=s.get("model"),
                version=version,
            ))
        sess.add_all(rows)
        sess.commit()
    finally:
        sess.close()

    print(f"\nSaved {len(rows)} posts for {s.get('week_label')} v{version} (db-only, no files)")
    return dict(saved_version=version)


# ---------------------------------------------------------------- routing
def route_after_critique(s: State):
    errs = [i for i in s.get("issues", []) if i["severity"] == "error"]
    if errs and s.get("revisions", 0) < 2:
        return "revise"
    return "verify"


def route_after_verify(s: State):
    if s.get("verify_errors") and s.get("verify_rounds", 0) < 3:
        return "revise"
    return "export"


def route_after_revise(s: State):
    return s.get("revise_target", "verify")


def build_graph():
    g = StateGraph(State)
    g.add_node("load", node_load)
    g.add_node("brand", node_brand)
    g.add_node("score", node_score)
    g.add_node("select", node_select)
    g.add_node("angle", node_angle)
    g.add_node("write", node_write)
    g.add_node("critique", node_critique)
    g.add_node("revise", node_revise)
    g.add_node("verify", node_verify)
    g.add_node("export", node_export)

    g.add_edge(START, "load")
    g.add_edge("load", "brand")
    g.add_edge("brand", "score")
    g.add_edge("score", "select")
    g.add_edge("select", "angle")
    g.add_edge("angle", "write")
    g.add_edge("write", "critique")
    g.add_conditional_edges("critique", route_after_critique,
                            {"revise": "revise", "verify": "verify"})
    g.add_conditional_edges("revise", route_after_revise,
                            {"critique": "critique", "verify": "verify"})
    g.add_conditional_edges("verify", route_after_verify,
                            {"revise": "revise", "export": "export"})
    g.add_edge("export", END)
    return g.compile()


# ---------------------------------------------------------------- CLI
def main():
    ap = argparse.ArgumentParser(description="Devalon weekly SMM agent")
    ap.add_argument("--src", help="week prefix: https://pub-.../2026-W46 or a local dir")
    ap.add_argument("--week", default=week_folder(), help="ISO week, used when --src is a bucket root")
    ap.add_argument("--themes-url", help="override themes.json target")
    ap.add_argument("--trends-url", help="override trends.csv target")
    ap.add_argument("--dropped-url", help="override dropped.csv target")
    ap.add_argument("--no-dropped", action="store_true")
    ap.add_argument("--model", default=os.getenv("SMM_MODEL", DEFAULT_MODEL))
    ap.add_argument("--week-label", help="week_trendings.week_label, e.g. 2026-W46; "
                                        "default: most recent row")
    ap.add_argument("--dry-run", action="store_true", help="load + score only, no LLM")
    a = ap.parse_args()

    week_id = None
    if a.dry_run:
        src, week, targets = resolve_inputs(a.src, a.week, a.themes_url, a.trends_url,
                                            a.dropped_url, a.no_dropped)
        print(f"Devalon SMM agent · week {week}\n  source: {src}")
    elif a.week_label or not (a.src or a.themes_url):
        week_id, week, targets = resolve_week_row(a.week_label)
        if a.themes_url:
            targets["themes"] = a.themes_url
        if a.trends_url:
            targets["trends"] = a.trends_url
        if a.dropped_url or a.no_dropped:
            targets["dropped"] = None if a.no_dropped else a.dropped_url
        src = f"db:week_trendings:{week}"
        print(f"Devalon SMM agent · week {week} (id={week_id})\n  source: {src}")
    else:
        src, week, targets = resolve_inputs(a.src, a.week, a.themes_url, a.trends_url,
                                            a.dropped_url, a.no_dropped)
        print(f"Devalon SMM agent · week {week}\n  source: {src}")
        from db import upsert_week_trending
        week_id = upsert_week_trending(
            week, targets["themes"], targets["trends"], targets.get("dropped"),
            source_prefix=f"s3://{os.getenv('R2_BUCKET')}/{week}/"
            if src.startswith("http") else None,
        )

    state: State = dict(src=src, week=week, targets=targets, model=a.model,
                        week_id=week_id, week_label=week, dry_run=a.dry_run,
                        revisions=0, verify_rounds=0, revise_target="verify")

    if a.dry_run:
        state.update(node_load(state))
        state.update(node_score(state))
        print("\nTop candidates:")
        for i, c in enumerate(state["candidates"][:12]):
            print(f"  {i:2}. {c['score']:>7}  {c['service']:<18} {c['term']}")
        return

    app = build_graph()
    out = app.invoke(state)

    if out.get("verify_errors"):
        print("\n⚠ Drafts exported with verify warnings above")
        return 1
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
