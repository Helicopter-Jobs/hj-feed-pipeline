"""
scrape.py — Helicopter-Jobs.com feed pipeline
Scrapes direct-employer career pages → extracts jobs via JSON-LD (free) or
OpenAI (fallback) → deduplicates → builds RSS/XML feed for JBoard.

Improvements over v1 (all from production design review):
  1. ATS-aware stable GUID extraction (prevents duplicate explosion)
  2. Atomic jobs.json write (prevents data loss on crash)
  3. JSON-LD extraction before LLM call (cuts cost ~30-40%)
  4. ATS-specific link patterns (stops following garbage links on portal pages)
  5. OpenAI retry with backoff + JSON parse safety net
  6. Allowlist-based helicopter signal filter (replaces fragile dual blocklist)
  7. pubDate RFC-2822 compliant; <br/> removed from CDATA (RSS spec fix)
  8. Per-request rate limiting + robots.txt check (reduces IP bans)
"""

import os
import re
import time
import json
import html
import random
import tempfile
from datetime import datetime, timezone, timedelta
from email.utils import formatdate
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from pdfminer.high_level import extract_text as pdf_extract_text


# ============================================================
# CONFIG
# ============================================================
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
if not OPENAI_API_KEY:
    raise SystemExit(
        "Missing OPENAI_API_KEY — set it under GitHub repo → "
        "Settings → Secrets and variables → Actions."
    )

SOURCES_FILE               = os.environ.get("SOURCES_FILE", "sources_easy.txt")
OUT_XML                    = os.environ.get("OUT_XML", "feed.xml")
STATE_JSON                 = os.environ.get("STATE_JSON", "jobs.json")

BROWSER_TIMEOUT_MS         = int(os.environ.get("BROWSER_TIMEOUT_MS", "45000"))
MAX_JOB_LINKS_PER_SOURCE   = int(os.environ.get("MAX_JOB_LINKS_PER_SOURCE", "50"))
MAX_TEXT_CHARS_TO_LLM      = int(os.environ.get("MAX_TEXT_CHARS_TO_LLM", "18000"))
RETENTION_DAYS             = int(os.environ.get("RETENTION_DAYS", "30"))
MAX_TOTAL_JOBS             = int(os.environ.get("MAX_TOTAL_JOBS", "500"))
REQUEST_DELAY_SECONDS      = float(os.environ.get("REQUEST_DELAY_SECONDS", "2.0"))
REQUEST_DELAY_JITTER       = float(os.environ.get("REQUEST_DELAY_JITTER", "1.0"))

# Hard cap on expensive gpt-4o fallback calls per run (cost guard)
GPT4O_FALLBACK_BUDGET      = int(os.environ.get("GPT4O_FALLBACK_BUDGET", "10"))

BOT_UA = "HelicopterJobsFeedBot/2.0 (+https://helicopter-jobs.com/bot)"
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

CATEGORY_ENUM = ["Pilot", "Maintenance", "Medical", "Dispatch", "Operations", "Other"]
PDF_EXT       = ".pdf"

# ============================================================
# EMPLOYER MAPS
# ============================================================
EMPLOYER_MAP: Dict[str, str] = {
    "castleair.co.uk":                  "Castle Air Aviation",
    "jobs.heliservice.de":              "HeliService",
    "sloanehelicopters.com":            "Sloane Helicopters",
    "nhv-group.jobtoolz.com":           "NHV Group",
    "bristow.wd1.myworkdayjobs.com":    "Bristow Group",
    "careers-quanta.icims.com":         "PJ Helicopters",
    "allcareers-quanta.icims.com":      "PJ Helicopters",
    "careers-chccrew.icims.com":        "CHC",
    "careers-chc.icims.com":            "CHC",
    "hillsboroaviation.com":            "Hillsboro Aviation",
    "jobs.geisinger.org":               "Geisinger",
    "apollomedflightcareers.com":       "Apollo MedFlight",
    "jobs.babcockinternational.com":    "Babcock International",
    "jobs.papillon.com":                "Papillon Grand Canyon Helicopters",
}

# Exact-prefix employer overrides for generic ATS domains
EMPLOYER_SOURCE_OVERRIDES: List[Tuple[str, str]] = [
    ("https://apply.workable.com/billings-flying-service/",    "Billings Flying Service"),
    ("https://gama-aviation.my.salesforce-sites.com/",         "Gama Aviation"),
    ("https://jobs.jobvite.com/ornge",                         "Ornge"),
    ("https://job-boards.greenhouse.io/lifelinkiii",           "Life Link III"),
    ("https://careers.smartrecruiters.com/CanadianHelicopters", "Canadian Helicopters"),
    ("https://jobs.geisinger.org/ems",                         "Geisinger"),
    ("https://www.apollomedflightcareers.com/current-positions", "Apollo MedFlight"),
]


# ============================================================
# IMPROVEMENT 1 — STABLE GUID EXTRACTION
# Extract a stable, ATS-native job ID from the URL so that
# employer title edits, URL slug changes, and stray query
# parameters don't create phantom duplicates in JBoard.
# ============================================================
ATS_ID_PATTERNS: Dict[str, re.Pattern] = {
    "icims.com":         re.compile(r"/jobs/(\d+)/"),
    "myworkdayjobs":     re.compile(r"/job/[^/]+/([A-Za-z0-9_-]{6,})"),
    "greenhouse.io":     re.compile(r"/jobs/(\d+)"),
    "jobvite.com":       re.compile(r"/job/([A-Za-z0-9]+)"),
    "smartrecruiters.com": re.compile(r"/[^/]+/([A-Za-z0-9]{8,})$"),
    "workable.com":      re.compile(r"/j/([A-Za-z0-9]+)"),
    "jobtoolz.com":      re.compile(r"/en/([^/?#]+)$"),
    "heliservice.de":    re.compile(r"[?&]id=([A-Za-z0-9]+)"),
    "salesforce-sites.com": re.compile(r"vacancyNo=([A-Za-z0-9]+)"),
    "adp.com":           re.compile(r"cid=([A-Za-z0-9_-]+)"),
}

def extract_stable_guid(url: str) -> str:
    """
    Return a stable, human-readable GUID for a job URL.
    For known ATS platforms: <netloc>::<native_job_id>
    For unknown URLs: normalized netloc + path (no query/fragment).
    """
    try:
        parsed = urlparse(url)
        netloc = (parsed.netloc or "").lower().replace("www.", "")
        for domain_key, pattern in ATS_ID_PATTERNS.items():
            if domain_key in netloc or domain_key in url:
                m = pattern.search(url)
                if m:
                    return f"{netloc}::{m.group(1)}"
        # Generic fallback: strip query + fragment, lowercase, no trailing slash
        path = (parsed.path or "").rstrip("/")
        return f"{netloc}{path}"
    except Exception:
        return url.split("?")[0].split("#")[0].rstrip("/")


# ============================================================
# IMPROVEMENT 4 — ATS-SPECIFIC LINK PATTERNS
# Only follow links that match the platform's job detail URL
# structure. Prevents chasing pagination, filters, and widgets.
# ============================================================
ATS_LINK_PATTERNS: Dict[str, re.Pattern] = {
    "icims.com":          re.compile(r"/jobs/\d+/.+/job"),
    "myworkdayjobs":      re.compile(r"/job/[^/]+/[A-Za-z0-9_-]{6,}"),
    "greenhouse.io":      re.compile(r"/jobs/\d+$"),
    "jobvite.com":        re.compile(r"/job/[A-Za-z0-9]+"),
    "workable.com":       re.compile(r"/j/[A-Za-z0-9]+"),
    "smartrecruiters.com": re.compile(r"jobs\.smartrecruiters\.com/[^/]+/[A-Za-z0-9_-]{8,}"),
    "jobtoolz.com":       re.compile(r"/en/[^/?#]{3,}$"),
    "heliservice.de":     re.compile(r"[?&]id=[A-Za-z0-9]"),
    "salesforce-sites.com": re.compile(r"vacancyNo=[A-Za-z0-9]"),
    "adp.com":            re.compile(r"cid=[A-Za-z0-9_-]"),
}

def _get_ats_link_pattern(base_url: str) -> Optional[re.Pattern]:
    bu = base_url.lower()
    for domain_key, pattern in ATS_LINK_PATTERNS.items():
        if domain_key in bu:
            return pattern
    return None


# ============================================================
# IMPROVEMENT 6 — HELICOPTER ALLOWLIST FILTER
# Only include jobs that contain at least one explicit
# helicopter-positive signal. Much safer than a dual blocklist
# that silently drops valid maintenance/EMS/crew roles.
# ============================================================
HELICOPTER_SIGNALS = [
    "helicopter", "rotary", "rotor", "rotorcraft", "heli",
    "vertical lift", "rotor wing", "rotor-wing",
    # Common types
    "aw139", "aw109", "aw119", "aw169", "aw189",
    "h125", "h130", "h135", "h145", "h175", "h160",
    "ec135", "ec145", "ec155", "ec175",
    "s-92", "s92", "s-76", "s76",
    "bell 2", "bell 4", "bell 5", "bell 6",
    "uh-1", "uh1", "uh-60", "uh60",
    "ch-47", "ch47", "chinook",
    "as350", "as355", "as365", "as332",
    "md500", "md902", "r44", "r66",
    "sikorsky", "airbus h",
    # Operations
    "offshore", "hoist", "hho", "longline", "sling load",
    "hems", "air medical", "medevac", "air ambulance",
    "ems pilot", "flight nurse", "b1.3", "b2 engineer",
    # Explicit employer signals (for fringe cases)
    "nhv", "bristow", "chc helicopter", "pj helicopters",
    "ornge", "life link", "apollo medflight", "heliservice",
    "sloane helicopter", "castle air", "hillsboro aviation",
    "billings flying", "canadian helicopters", "babcock aviation",
    "gama aviation", "papillon",
]

# Categories that are always considered valid if a helicopter signal is present
ALWAYS_ACCEPT_CATEGORIES = {"Pilot", "Maintenance", "Medical", "Dispatch"}

def has_helicopter_signal(title: str, description: str) -> bool:
    """Return True if title or description contains any helicopter-positive signal."""
    text = (title + " " + description).lower()
    return any(sig in text for sig in HELICOPTER_SIGNALS)

def should_include_job(job: Dict) -> bool:
    """
    Main inclusion gate. Replaces the old dual fixed-wing/corporate blocklist.
    A job is included if it contains at least one helicopter signal.
    Additionally hard-block obvious non-jobs (cookie policy pages, ATS help pages).
    """
    title = (job.get("title") or "").strip()
    desc  = (job.get("description") or "").strip()
    url   = (job.get("apply_url") or job.get("source_url") or "").lower()

    # Hard-block garbage pages that slip through (ATS help/cookie pages)
    HARD_BLOCK_TITLES = [
        "not specified", "cookie", "privacy policy", "terms", "page not found",
        "403", "404", "access denied",
    ]
    if title.lower() in HARD_BLOCK_TITLES or not title:
        return False

    # Hard-block known non-aviation roles from mixed employers
    # (receptionist, IT support, etc. from Billings Flying Service)
    HARD_BLOCK_ROLE_KEYWORDS = [
        "receptionist", "it support", "information technology", "software engineer",
        "web developer", "marketing manager", "social media", "accountant",
        "payroll", "legal counsel", "attorney", "paralegal",
    ]
    tl = title.lower()
    if any(k in tl for k in HARD_BLOCK_ROLE_KEYWORDS):
        return False

    return has_helicopter_signal(title, desc)


# ============================================================
# PDF BLOCK HINTS
# ============================================================
PDF_BLOCK_HINTS = [
    "modern-slavery", "slavery", "statement", "policy", "privacy",
    "cookie", "terms", "map", "base-map", "liskeard", "handbook",
    "environment", "t_c", "tc_", "_tc", "standard-conditions",
]

def is_bad_pdf(u: str) -> bool:
    ul = (u or "").lower()
    if not ul.endswith(PDF_EXT):
        return False
    return any(h in ul for h in PDF_BLOCK_HINTS)


# ============================================================
# IMPROVEMENT 8 — ROBOTS.TXT CHECK
# Cache one RobotFileParser per domain to avoid re-fetching.
# Fails open (allows) if robots.txt is unreachable.
# ============================================================
_robots_cache: Dict[str, RobotFileParser] = {}

def robots_allows(url: str) -> bool:
    try:
        parsed = urlparse(url)
        base   = f"{parsed.scheme}://{parsed.netloc}"
        if base not in _robots_cache:
            rp = RobotFileParser(f"{base}/robots.txt")
            rp.read()
            _robots_cache[base] = rp
        return _robots_cache[base].can_fetch(BOT_UA, url)
    except Exception:
        return True  # fail open


# ============================================================
# HELPERS
# ============================================================
def sleep_a_bit():
    time.sleep(max(0.3, REQUEST_DELAY_SECONDS + random.uniform(0, REQUEST_DELAY_JITTER)))

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def parse_iso(dt: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat((dt or "").replace("Z", "+00:00"))
    except Exception:
        return None

def normalize_space(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""

def employer_for_domain(dom: str) -> str:
    if dom in EMPLOYER_MAP:
        return EMPLOYER_MAP[dom]
    for k, v in EMPLOYER_MAP.items():
        if dom.endswith(k):
            return v
    return dom or "Unknown"

def titlecase_slug(slug: str) -> str:
    slug = (slug or "").strip().strip("/")
    if not slug:
        return ""
    return " ".join(p.capitalize() for p in slug.split("-") if p)

def employer_for_source(url: str) -> str:
    u = (url or "").strip()
    for prefix, name in EMPLOYER_SOURCE_OVERRIDES:
        if u.startswith(prefix):
            return name
    try:
        p    = urlparse(u)
        host = (p.netloc or "").lower().replace("www.", "")
        if host == "apply.workable.com":
            slug = (p.path or "/").strip("/").split("/")[0]
            name = titlecase_slug(slug)
            if name:
                return name
    except Exception:
        pass
    return employer_for_domain(domain_from_url(u))

def read_sources() -> List[str]:
    raw = open(SOURCES_FILE, "r", encoding="utf-8").read().strip()
    if not raw:
        raise SystemExit(f"{SOURCES_FILE} is empty.")
    lines = [ln.strip() for ln in raw.splitlines()
             if ln.strip() and not ln.strip().startswith("#")]
    urls: List[str] = []
    for ln in lines:
        ln = ln.strip().strip(",")
        if "http" in ln and " " in ln:
            for p in ln.split():
                if p.startswith("http"):
                    urls.append(p.strip().strip(","))
        else:
            urls.append(ln)
    seen: Set[str] = set()
    out:  List[str] = []
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

def extract_text_from_html(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "lxml")
    for tag in soup(["script", "style", "noscript", "svg", "canvas"]):
        tag.decompose()
    return normalize_space(soup.get_text("\n"))[:MAX_TEXT_CHARS_TO_LLM]

def fetch_pdf_text(url: str) -> str:
    r = requests.get(url, timeout=60, headers={"User-Agent": BROWSER_UA})
    r.raise_for_status()
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(r.content)
        tmp_path = tmp.name
    text = pdf_extract_text(tmp_path) or ""
    try:
        os.unlink(tmp_path)
    except Exception:
        pass
    return normalize_space(text)[:MAX_TEXT_CHARS_TO_LLM]

def rss_escape(s: str) -> str:
    return html.escape(s or "", quote=True)

def rfc2822(dt: datetime) -> str:
    """IMPROVEMENT 7 — correct RFC-2822 pubDate format for RSS."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    import calendar
    return formatdate(timeval=calendar.timegm(dt.timetuple()), localtime=False, usegmt=True)

def is_http_url(u: str) -> bool:
    try:
        return urlparse(u).scheme.lower() in ("http", "https")
    except Exception:
        return False

def dedupe_lines_keep_order(text: str) -> str:
    lines = [ln.strip() for ln in (text or "").splitlines()]
    out: List[str] = []
    seen: Set[str] = set()
    for ln in lines:
        if not ln or ln in seen:
            continue
        seen.add(ln)
        out.append(ln)
    return "\n".join(out).strip()

def format_description_for_jboard(desc: str) -> str:
    """
    IMPROVEMENT 7 — CDATA fix.
    Return raw text with newlines. Do NOT convert to <br/> — inside CDATA
    that renders as literal text in most RSS consumers including JBoard.
    If JBoard specifically requires HTML, switch to <p> wrapping below.
    """
    d = (desc or "").strip()
    if not d:
        return ""
    return d.replace("•", "-").replace("·", "-")


# ============================================================
# IMPROVEMENT 5 — JSON PARSE SAFETY NET
# ============================================================
def safe_parse_json(raw_text: str) -> Optional[dict]:
    """
    Parse JSON from LLM output that may have markdown fences,
    preamble, or truncation. Returns None on all failure paths.
    """
    s = (raw_text or "").strip()
    if not s:
        return None
    # Strip markdown code fences
    s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.MULTILINE)
    s = re.sub(r"\s*```$", "", s, flags=re.MULTILINE)
    s = s.strip()
    # Direct parse
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    # Extract first {...} block (handles preamble/postamble)
    m = re.search(r"\{.*\}", s, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    return None


# ============================================================
# IMPROVEMENT 3 — JSON-LD EXTRACTION (free, before LLM)
# Many ATS platforms (Greenhouse, Workable, many Workday pages)
# embed complete JobPosting schema. Parsing it is instant and
# more accurate than LLM extraction.
# ============================================================
def extract_jsonld_job(soup: BeautifulSoup, source_url: str, employer: str) -> Optional[Dict]:
    """
    Try to extract a JobPosting from JSON-LD embedded in the page.
    Returns a normalized job dict on success, None otherwise.
    """
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            raw = (script.string or "").strip()
            if not raw:
                continue
            data = json.loads(raw)
            # Handle array of schemas
            if isinstance(data, list):
                data = next(
                    (d for d in data if isinstance(d, dict) and d.get("@type") == "JobPosting"),
                    None,
                )
            if not data or not isinstance(data, dict):
                continue
            if data.get("@type") != "JobPosting":
                continue

            title = (data.get("title") or "").strip()
            if not title:
                continue

            # Location
            loc_raw = data.get("jobLocation", {})
            if isinstance(loc_raw, list):
                loc_raw = loc_raw[0] if loc_raw else {}
            addr = loc_raw.get("address", {}) if isinstance(loc_raw, dict) else {}
            if isinstance(addr, str):
                location = addr
            else:
                parts = [
                    addr.get("addressLocality", ""),
                    addr.get("addressRegion", ""),
                    addr.get("addressCountry", ""),
                ]
                location = ", ".join(p for p in parts if p).strip(", ") or "Not specified"

            # Remote
            job_loc_type = str(data.get("jobLocationType", "")).lower()
            remote = "remote" in job_loc_type or "telecommute" in job_loc_type

            # Salary
            salary_raw = data.get("baseSalary", "")
            salary_line = ""
            if isinstance(salary_raw, dict):
                val = salary_raw.get("value", {})
                if isinstance(val, dict):
                    mn = val.get("minValue", "")
                    mx = val.get("maxValue", "")
                    cur = salary_raw.get("currency", "")
                    if mn and mx:
                        salary_line = f"{cur} {mn}–{mx}".strip()
                    elif mn:
                        salary_line = f"{cur} {mn}".strip()
                elif val:
                    salary_line = str(val)
            elif salary_raw:
                salary_line = str(salary_raw)

            # Description
            desc_raw = data.get("description", "")
            # Strip HTML tags if description contains markup
            desc_soup = BeautifulSoup(desc_raw, "lxml")
            description = normalize_space(desc_soup.get_text("\n"))

            # Apply URL
            apply_url = (
                data.get("url") or
                data.get("applicationContact", {}).get("url", "") or
                source_url
            )

            if len(description) < 120:
                return None  # not enough content; fall through to LLM

            return {
                "title":       title,
                "employer":    employer,
                "location":    location or "Not specified",
                "remote":      remote,
                "apply_url":   str(apply_url).strip() or source_url,
                "category":    "Other",   # will be overridden by category_override()
                "description": description,
                "salary_line": salary_line,
                "source_url":  source_url,
                "guid":        extract_stable_guid(source_url),
            }
        except Exception:
            continue
    return None


# ============================================================
# CATEGORY NORMALIZATION
# ============================================================
def category_override(title: str, description: str, current: str) -> str:
    t = (title + " " + description).lower()
    if any(k in t for k in ["paramedic", "flight nurse", "registered nurse", " rn ", "emt", "medic", "critical care"]):
        return "Medical"
    if any(k in t for k in ["b1.3", "b2", "avionics", "a&p", "a and p", "mechanic", "licensed engineer",
                              "maintenance engineer", "part-145", "part 145", "airframe", "powerplant"]):
        return "Maintenance"
    if any(k in t for k in ["pilot", "captain", "co-pilot", "copilot", " sic ", " pic ", "first officer"]):
        return "Pilot"
    if any(k in t for k in ["dispatcher", "dispatch"]):
        return "Dispatch"
    if any(k in t for k in ["flight operations", "operations officer", "ops officer", "ground operations"]):
        return "Operations"
    return current if current in CATEGORY_ENUM else "Other"


# ============================================================
# IMPROVEMENT 5 (cont.) — OPENAI WITH RETRY + BACKOFF
# ============================================================
_gpt4o_fallback_used = 0

def openai_post_with_backoff(payload: dict, timeout_s: int = 90) -> dict:
    """POST to OpenAI with exponential backoff on 429 / 5xx."""
    backoff = 5
    for attempt in range(1, 8):
        try:
            resp = requests.post(
                "https://api.openai.com/v1/responses",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                data=json.dumps(payload),
                timeout=timeout_s,
            )
            if resp.status_code == 429:
                print(f"  OpenAI 429 rate-limit. Sleeping {backoff}s (attempt {attempt}/7) ...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
            if resp.status_code >= 500:
                print(f"  OpenAI {resp.status_code} server error. Sleeping {backoff}s ...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.Timeout:
            print(f"  OpenAI request timed out (attempt {attempt}/7). Sleeping {backoff}s ...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    # All retries exhausted — return empty shell so caller handles gracefully
    return {"output": [{"content": [{"type": "output_text", "text": ""}]}]}

def extract_output_text(data: dict) -> str:
    text_out = ""
    for item in data.get("output", []):
        for c in item.get("content", []):
            if c.get("type") == "output_text":
                text_out += c.get("text", "")
    return (text_out or "").strip()

def openai_extract_job(source_url: str, raw_text: str, employer: str) -> Optional[Dict]:
    global _gpt4o_fallback_used

    instructions = f"""
Return ONLY valid JSON with these exact keys:
  title        (string — the exact job title, never empty)
  employer     (string — MUST be exactly: "{employer}")
  location     (string — city/region as stated; "Not specified" if absent)
  remote       (boolean — true ONLY if explicitly stated Remote/Hybrid/Telecommute)
  apply_url    (string — direct application link if present, else use source_url)
  category     (string — one of exactly: {CATEGORY_ENUM})
  description  (string — the FULL job posting text verbatim, do not summarize)
  salary_line  (string — only if pay/salary explicitly stated, else "")

Rules:
- Do NOT guess or invent any value.
- employer MUST be exactly "{employer}" — do not change it.
- If title cannot be determined, return title as empty string.
- Return ONLY the JSON object. No markdown, no explanation.
"""
    payload = {
        "model": "gpt-4o-mini",
        "input": [
            {"role": "system", "content": "Extract structured job posting fields for a helicopter job board. Return ONLY valid JSON."},
            {"role": "user",   "content": f"source_url: {source_url}\n\n{instructions}\n\nJOB PAGE TEXT:\n{raw_text}"},
        ],
        "temperature": 0.0,
    }

    data = openai_post_with_backoff(payload, timeout_s=90)
    job  = safe_parse_json(extract_output_text(data))

    # Fallback to gpt-4o if mini failed, but only within budget
    if job is None:
        if _gpt4o_fallback_used >= GPT4O_FALLBACK_BUDGET:
            print(f"  gpt-4o fallback budget exhausted ({GPT4O_FALLBACK_BUDGET}). Skipping.")
            return None
        print(f"  gpt-4o-mini returned non-JSON. Falling back to gpt-4o ({_gpt4o_fallback_used + 1}/{GPT4O_FALLBACK_BUDGET}) ...")
        payload["model"] = "gpt-4o"
        data2 = openai_post_with_backoff(payload, timeout_s=120)
        job   = safe_parse_json(extract_output_text(data2))
        _gpt4o_fallback_used += 1

    if job is None:
        print(f"  Both models returned non-JSON for: {source_url}")
        return None

    # Normalise all string fields
    for k in ["title", "location", "apply_url", "description", "salary_line", "category"]:
        job[k] = str(job.get(k, "") or "").strip()

    job["employer"]   = employer
    job["source_url"] = source_url
    job["guid"]       = extract_stable_guid(source_url)

    if not isinstance(job.get("remote"), bool):
        job["remote"] = False

    job["category"]     = category_override(job.get("title", ""), job.get("description", ""), job.get("category", "Other"))
    job["description"]  = dedupe_lines_keep_order(job.get("description", ""))

    if not job.get("apply_url"):
        job["apply_url"] = source_url

    return job


# ============================================================
# JOB VALIDATION
# ============================================================
def is_valid_job(job: Dict) -> bool:
    title = (job.get("title") or "").strip()
    desc  = (job.get("description") or "").strip()
    link  = (job.get("apply_url") or job.get("source_url") or "").lower()
    if not title:
        return False
    if len(desc) < 120:
        return False
    if link.endswith(PDF_EXT) and is_bad_pdf(link):
        return False
    return True


# ============================================================
# LINK FILTERING
# ============================================================
BAD_LINK_WORDS = [
    "privacy", "cookie", "legal", "terms", "accessibility",
    "sustainability", "diversity", "community", "stories",
    "leadership", "culture", "history", "capabilities",
    "supplier", "veteran", "internship-program", "pay-benefits",
    "press", "news", "blog", "help", "support", "jobseeker",
]

def is_likely_job_link(url: str, base_url: str) -> bool:
    """
    IMPROVEMENT 4 — ATS-aware link filter.
    Uses ATS_LINK_PATTERNS for known platforms, generic heuristic otherwise.
    """
    u = (url or "").strip()
    if not u or not is_http_url(u):
        return False
    ul = u.lower()

    if ul.startswith(("mailto:", "tel:", "javascript:")):
        return False
    if any(w in ul for w in BAD_LINK_WORDS):
        return False
    if ul.endswith(PDF_EXT):
        return not is_bad_pdf(ul)

    # ATS-specific pattern match
    pattern = _get_ats_link_pattern(base_url)
    if pattern:
        return bool(pattern.search(u))

    # Generic: path depth >= 2, not a bare domain, has job-like segment
    try:
        path = urlparse(u).path or ""
        segments = [s for s in path.split("/") if s]
        if len(segments) < 1:
            return False
    except Exception:
        return False

    return any(x in ul for x in ["/job", "/jobs", "/career", "/careers",
                                   "requisition", "vacancy", "vacancies",
                                   "opening", "position"])

def collect_job_links(base_url: str, html_content: str) -> List[str]:
    soup  = BeautifulSoup(html_content, "lxml")
    links: List[str] = []

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(base_url, href)
        if is_likely_job_link(full, base_url):
            links.append(full)

    # SmartRecruiters: direct job links are often in JS, not <a> tags
    if "careers.smartrecruiters.com" in base_url.lower():
        for m in re.findall(
            r"https?://jobs\.smartrecruiters\.com/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+",
            html_content
        ):
            links.append(m)

    seen: Set[str] = set()
    out:  List[str] = []
    for l in links:
        if l not in seen:
            out.append(l)
            seen.add(l)

    return out[:MAX_JOB_LINKS_PER_SOURCE]


# ============================================================
# IMPROVEMENT 2 — ATOMIC STORE WRITE
# Uses os.replace() which is atomic on POSIX. A crash mid-write
# leaves the original file intact.
# ============================================================
def load_store() -> Dict[str, Dict]:
    if not os.path.exists(STATE_JSON):
        return {}
    try:
        with open(STATE_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception as e:
        print(f"Warning: could not load {STATE_JSON}: {e}. Starting with empty store.")
    return {}

def save_store(store: Dict[str, Dict]) -> None:
    """IMPROVEMENT 2 — atomic write via temp file + os.replace()."""
    tmp_path = STATE_JSON + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, STATE_JSON)   # atomic on POSIX; safe on Windows

def prune_store(store: Dict[str, Dict]) -> Dict[str, Dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    return {
        guid: job for guid, job in store.items()
        if (parse_iso(job.get("last_seen", "")) or
            parse_iso(job.get("first_seen", "")) or
            datetime.min.replace(tzinfo=timezone.utc)) >= cutoff
    }

def scrub_store(store: Dict[str, Dict]) -> Dict[str, Dict]:
    """Remove invalid jobs and jobs without helicopter signal."""
    cleaned = {}
    for guid, job in store.items():
        if not job.get("apply_url"):
            job["apply_url"] = job.get("source_url", guid)
        if not is_valid_job(job):
            continue
        if not should_include_job(job):
            continue
        cleaned[guid] = job
    return cleaned

def upsert_jobs(store: Dict[str, Dict], new_jobs: List[Dict]) -> Dict[str, Dict]:
    now = utc_now_iso()
    for j in new_jobs:
        guid = (j.get("guid") or "").strip()
        if not guid:
            continue
        if guid in store:
            existing = store[guid]
            # Preserve first_seen; update everything else
            first_seen = existing.get("first_seen", now)
            existing.update(j)
            existing["first_seen"] = first_seen
            existing["last_seen"]  = now
            store[guid] = existing
        else:
            j["first_seen"] = now
            j["last_seen"]  = now
            store[guid] = j
    return store


# ============================================================
# IMPROVEMENT 7 — RSS FEED BUILDER (RSS spec compliant)
# ============================================================
def build_feed(items: List[Dict]) -> str:
    out: List[str] = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0">',
        "  <channel>",
        "    <title>Helicopter-Jobs Aggregated Feed</title>",
        "    <link>https://helicopter-jobs.com</link>",
        "    <description>Direct-employer helicopter jobs</description>",
    ]

    for j in items:
        title    = rss_escape(j.get("title", ""))
        employer = rss_escape(j.get("employer", ""))
        link     = rss_escape(j.get("apply_url") or j.get("source_url", ""))
        guid     = rss_escape(j.get("guid") or j.get("source_url", ""))

        fs      = parse_iso(j.get("first_seen", ""))
        pubdate = rfc2822(fs) if fs else rfc2822(datetime.now(timezone.utc))

        category = rss_escape(j.get("category", "Other"))
        location = rss_escape(j.get("location") or "Not specified")
        remote   = "true" if j.get("remote") else "false"

        desc = (j.get("description") or "").strip()
        if j.get("salary_line"):
            desc = f"{j['salary_line']}\n\n{desc}".strip()
        desc_out = format_description_for_jboard(desc)

        out += [
            "    <item>",
            f"      <title>{title}</title>",
            f"      <employer>{employer}</employer>",
            f"      <link>{link}</link>",
            f'      <guid isPermaLink="false">{guid}</guid>',
            f"      <pubDate>{pubdate}</pubDate>",
            f"      <category>{category}</category>",
            f"      <location>{location}</location>",
            f"      <remote>{remote}</remote>",
            "      <description><![CDATA[",
            desc_out,
            "]]></description>",
            "    </item>",
        ]

    out += ["  </channel>", "</rss>"]
    return "\n".join(out)


# ============================================================
# MAIN
# ============================================================
def main():
    global _gpt4o_fallback_used
    _gpt4o_fallback_used = 0

    sources = read_sources()
    print(f"Sources file : {SOURCES_FILE}")
    print(f"Sources count: {len(sources)}")
    print(f"MAX_JOB_LINKS_PER_SOURCE={MAX_JOB_LINKS_PER_SOURCE}  "
          f"RETENTION_DAYS={RETENTION_DAYS}  MAX_TOTAL_JOBS={MAX_TOTAL_JOBS}")

    new_jobs: List[Dict] = []
    seen_job_urls: Set[str] = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=BROWSER_UA)
        page    = context.new_page()

        for src in sources:
            if len(new_jobs) >= MAX_TOTAL_JOBS:
                print(f"Reached MAX_TOTAL_JOBS={MAX_TOTAL_JOBS}. Stopping.")
                break

            employer = employer_for_source(src)
            print(f"\nSOURCE  : {src}")
            print(f"Employer: {employer}")

            # IMPROVEMENT 8 — robots.txt check
            if not robots_allows(src):
                print(f"  Skipping — robots.txt disallows this URL.")
                continue

            # Load listing page
            try:
                page.goto(src, timeout=BROWSER_TIMEOUT_MS, wait_until="domcontentloaded")
                sleep_a_bit()
                listing_html = page.content()
            except Exception as e:
                print(f"  Failed to load listing page: {e}")
                continue

            links = collect_job_links(src, listing_html)
            print(f"  Found {len(links)} candidate job links")
            if not links:
                # Treat the listing page itself as the job page (e.g. single-job career pages)
                links = [src]

            for job_url in links:
                if len(new_jobs) >= MAX_TOTAL_JOBS:
                    break
                if job_url in seen_job_urls:
                    continue
                seen_job_urls.add(job_url)

                if not is_http_url(job_url):
                    continue

                # IMPROVEMENT 8 — robots.txt per job URL
                if not robots_allows(job_url):
                    print(f"  Skipping {job_url} — robots.txt disallows.")
                    continue

                try:
                    # ---- PDF path ----
                    if job_url.lower().endswith(PDF_EXT):
                        if is_bad_pdf(job_url):
                            continue
                        raw_text   = fetch_pdf_text(job_url)
                        source_url = job_url
                        soup       = None
                        sleep_a_bit()
                    else:
                        # ---- Try lightweight requests fetch first ----
                        # (avoids Playwright overhead on static pages)
                        soup = None
                        raw_text = ""
                        try:
                            r = requests.get(
                                job_url, timeout=15,
                                headers={"User-Agent": BROWSER_UA},
                                allow_redirects=True,
                            )
                            body = r.text or ""
                            if len(body) > 1500 and "<noscript>" not in body[:800].lower():
                                soup     = BeautifulSoup(body, "lxml")
                                raw_text = extract_text_from_html(body)
                                source_url = job_url
                        except Exception:
                            pass

                        # Fallback to Playwright for JS-heavy pages
                        if not raw_text or len(raw_text) < 500:
                            page.goto(job_url, timeout=BROWSER_TIMEOUT_MS, wait_until="domcontentloaded")
                            sleep_a_bit()
                            page_html  = page.content()
                            soup       = BeautifulSoup(page_html, "lxml")
                            raw_text   = extract_text_from_html(page_html)
                            source_url = page.url  # use final URL after any redirects
                        else:
                            sleep_a_bit()
                            source_url = job_url

                    if len(raw_text) < 200:
                        continue

                    # Detect listing pages — skip if it looks like search results
                    listing_signals = [
                        "results found", "filter jobs", "sort by", "showing jobs",
                        "all jobs", "search results", "job listings",
                    ]
                    if any(sig in raw_text[:1000].lower() for sig in listing_signals) and len(raw_text) < 3000:
                        print(f"  Skipping apparent listing page: {job_url}")
                        continue

                    # IMPROVEMENT 3 — Try JSON-LD first (free, no LLM cost)
                    job = None
                    if soup:
                        job = extract_jsonld_job(soup, source_url, employer)
                        if job:
                            print(f"  [JSON-LD] {job.get('title','')[:80]}")

                    # Fall back to OpenAI extraction
                    if not job:
                        job = openai_extract_job(source_url, raw_text, employer)
                        if job:
                            print(f"  [OpenAI] {job.get('title','')[:80]}")

                    if not job:
                        continue

                    # Normalise category now that we have full job data
                    job["category"] = category_override(
                        job.get("title", ""), job.get("description", ""), job.get("category", "Other")
                    )

                    if not is_valid_job(job):
                        continue

                    # IMPROVEMENT 6 — helicopter signal gate
                    if not should_include_job(job):
                        print(f"  [FILTERED] {job.get('title','')[:80]}")
                        continue

                    new_jobs.append(job)
                    print(f"  + {job.get('title','(no title)')[:90]}")

                except Exception as e:
                    print(f"  - Failed: {job_url} | {str(e)[:160]}")
                    continue

        browser.close()

    # ---- Persist ----
    store = load_store()
    store = upsert_jobs(store, new_jobs)
    store = scrub_store(store)
    store = prune_store(store)
    save_store(store)   # IMPROVEMENT 2 — atomic write

    items = sorted(
        store.values(),
        key=lambda j: (parse_iso(j.get("first_seen", "")) or datetime.min.replace(tzinfo=timezone.utc)),
        reverse=True,
    )

    xml = build_feed(items)
    with open(OUT_XML, "w", encoding="utf-8") as f:
        f.write(xml)

    print(f"\nNew jobs scraped : {len(new_jobs)}")
    print(f"Store total      : {len(items)}")
    print(f"gpt-4o fallbacks : {_gpt4o_fallback_used}/{GPT4O_FALLBACK_BUDGET}")
    print(f"Feed written     : {OUT_XML}")


if __name__ == "__main__":
    main()
