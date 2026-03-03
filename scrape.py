import os
import re
import time
import json
import html
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from pdfminer.high_level import extract_text as pdf_extract_text


# =====================
# REQUIRED CONFIG
# =====================
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
if not OPENAI_API_KEY:
    raise SystemExit("Missing OPENAI_API_KEY (GitHub repo → Settings → Secrets and variables → Actions).")

SOURCES_FILE = os.environ.get("SOURCES_FILE", "sources_easy.txt")
OUT_XML = os.environ.get("OUT_XML", "feed.xml")
STATE_JSON = os.environ.get("STATE_JSON", "jobs.json")  # master merged store

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Timeouts + caps (override in workflow env if needed)
BROWSER_TIMEOUT_MS = int(os.environ.get("BROWSER_TIMEOUT_MS", "45000"))
MAX_JOB_LINKS_PER_SOURCE = int(os.environ.get("MAX_JOB_LINKS_PER_SOURCE", "50"))
MAX_TEXT_CHARS_TO_LLM = int(os.environ.get("MAX_TEXT_CHARS_TO_LLM", "18000"))
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "30"))
MAX_TOTAL_JOBS = int(os.environ.get("MAX_TOTAL_JOBS", "500"))

# Throttle (reduces blocks + rate-limits)
REQUEST_DELAY_SECONDS = float(os.environ.get("REQUEST_DELAY_SECONDS", "0.9"))
REQUEST_DELAY_JITTER = float(os.environ.get("REQUEST_DELAY_JITTER", "0.6"))

CATEGORY_ENUM = ["Pilot", "Maintenance", "Medical", "Dispatch", "Operations", "Other"]

EMPLOYER_MAP = {
    "castleair.co.uk": "Castle Air Aviation",
    "jobs.heliservice.de": "HeliService",
    "sloanehelicopters.com": "Sloane Helicopters",
    "nhv-group.jobtoolz.com": "NHV Group",
    "bristow.wd1.myworkdayjobs.com": "Bristow Group",
    "careers-quanta.icims.com": "PJ Helicopters",
    "allcareers-quanta.icims.com": "PJ Helicopters",
    "careers-chccrew.icims.com": "CHC",
    "careers-chc.icims.com": "CHC",
    "jobs.papillon.com": "Papillon",
    "hillsboroaviation.com": "Hillsboro Aviation",
}

# Source URL overrides: fixes generic ATS domains (Workable/Salesforce/Jobvite/Greenhouse etc.)
EMPLOYER_SOURCE_OVERRIDES: List[Tuple[str, str]] = [
    ("https://apply.workable.com/billings-flying-service/", "Billings Flying Service"),
    ("https://gama-aviation.my.salesforce-sites.com/", "Gama Aviation"),
    ("https://jobs.jobvite.com/ornge", "Ornge"),
    ("https://job-boards.greenhouse.io/lifelinkiii", "Life Link III"),
]

ATS_HINTS = [
    "myworkdayjobs.com",
    "icims.com",
    "jobtoolz.com",
    "smartrecruiters.com",
    "greenhouse.io",
    "lever.co",
    "workable.com",
    "salesforce-sites.com",
    "jobvite.com",
]

PDF_EXT = ".pdf"


# =====================
# BLOCKLISTS (block corp + safety/quality + fixed wing)
# =====================
CORPORATE_BLOCK_KEYWORDS = [
    # finance/accounting
    "finance", "financial", "accounting", "accountant", "payroll", "tax", "treasury",
    # HR / recruiting
    "hr", "human resources", "recruiter", "recruiting", "talent acquisition", "people operations",
    # marketing/sales
    "marketing", "brand", "social media", "content", "sales", "business development", "account executive",
    # legal
    "legal", "attorney", "paralegal", "counsel",
    # IT / software
    "information technology", "software", "developer", "data analyst", "product manager",
    # admin/customer service
    "admin", "administrator", "office manager", "administrative", "customer service", "call center",
    # safety/quality/compliance (blocked)
    "compliance", "compliant", "governance",
    "safety", "qhse", "hse", "qms", "quality", "sms", "risk", "audit", "assurance",
]

FIXED_WING_BLOCK_KEYWORDS = [
    "fixed wing", "fixed-wing", "airplane", "aeroplane", "jet", "turboprop",
    "airline", "part 121", "part121",
    "cessna", "citation", "gulfstream", "learjet", "king air", "beechcraft",
    "pilatus", "pc-12", "pc12", "pc-24", "pc24",
    "embraer", "phenom", "boeing", "airbus",
    "a320", "a321", "a330", "a350", "b737", "737", "b747", "b757", "b767", "b777", "b787",
    "flight attendant", "cabin crew", "ramp agent", "gate agent",
    "airframe and powerplant", "airframe & powerplant",
    "aircraft mechanic", "aircraft maintenance", "airframe", "powerplant",
    "fbo"
]

ROTOR_OVERRIDE_KEYWORDS = [
    "helicopter", "rotor", "rotary", "rotor-wing", "rotor wing", "rotorcraft", "vertical lift",
    "aw139", "aw109", "aw119", "aw169", "aw189",
    "h125", "h130", "h135", "h145", "ec135", "ec145",
    "s92", "s-92", "s76", "s-76",
    "bell 206", "bell 407", "bell 412", "bell 429",
    "uh-1", "uh1",
    "hems", "air medical", "medevac",
]

AVIATION_CORE_KEEP = [
    "pilot", "captain", "co-pilot", "copilot", "sic", "pic", "aircrew", "crew member", "hoist", "hho",
    "mechanic", "avionics", "a&p", "a and p", "b1.3", "b2", "maintenance", "part-145",
    "flight nurse", "paramedic", "emt", "medic",
    "dispatcher", "dispatch", "flight operations", "operations officer"
]

PDF_BLOCK_HINTS = [
    "modern-slavery", "slavery", "statement", "policy", "privacy", "cookie", "terms",
    "map", "base-map", "liskeard", "handbook", "environment"
]


# =====================
# HELPERS
# =====================
def sleep_a_bit():
    time.sleep(max(0.0, REQUEST_DELAY_SECONDS + random.uniform(0, REQUEST_DELAY_JITTER)))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(dt: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(dt.replace("Z", "+00:00"))
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
    parts = [p for p in slug.split("-") if p]
    return " ".join([p.capitalize() for p in parts])


def employer_for_source(url: str) -> str:
    u = (url or "").strip()

    # 1) exact prefix overrides
    for prefix, name in EMPLOYER_SOURCE_OVERRIDES:
        if u.startswith(prefix):
            return name

    # 2) Workable: derive employer from org slug (prevents "Workable" as employer)
    try:
        p = urlparse(u)
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
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip() and not ln.strip().startswith("#")]

    urls: List[str] = []
    for ln in lines:
        ln = ln.strip().strip(",")
        if "http" in ln and " " in ln:
            for p in ln.split():
                if p.startswith("http"):
                    urls.append(p.strip().strip(","))
        else:
            urls.append(ln)

    seen = set()
    out = []
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
    r = requests.get(url, timeout=60, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    tmp = "/tmp/job.pdf"
    with open(tmp, "wb") as f:
        f.write(r.content)
    text = pdf_extract_text(tmp) or ""
    return normalize_space(text)[:MAX_TEXT_CHARS_TO_LLM]


def rss_escape(s: str) -> str:
    return html.escape(s or "", quote=True)


def rfc2822(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


def safe_json_load(s: str) -> Optional[dict]:
    s = (s or "").strip()
    if not s:
        return None
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        s = s[start:end + 1]
    try:
        return json.loads(s)
    except Exception:
        return None


def is_http_url(u: str) -> bool:
    try:
        scheme = urlparse(u).scheme.lower()
        return scheme in ("http", "https")
    except Exception:
        return False


def strip_tracking(url: str) -> str:
    """Stable GUID: lowercase host, remove query + fragment, remove trailing slash."""
    try:
        p = urlparse(url)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower().replace("www.", "")
        path = (p.path or "").rstrip("/")
        return f"{scheme}://{netloc}{path}"
    except Exception:
        return (url or "").split("?")[0].split("#")[0].rstrip("/")


def is_bad_pdf(u: str) -> bool:
    u = (u or "").lower()
    if not u.endswith(PDF_EXT):
        return False
    return any(h in u for h in PDF_BLOCK_HINTS)


def dedupe_lines_keep_order(text: str) -> str:
    lines = [ln.strip() for ln in (text or "").splitlines()]
    out = []
    seen = set()
    for ln in lines:
        if not ln:
            continue
        if ln in seen:
            continue
        seen.add(ln)
        out.append(ln)
    return "\n".join(out).strip()


def format_description_for_jboard(desc: str) -> str:
    d = (desc or "").strip()
    if not d:
        return ""
    d = d.replace("•", "-").replace("·", "-")
    return d.replace("\n", "<br/>")


def is_fixed_wing_job(title: str, description: str) -> bool:
    t = f"{title}\n{description}".lower()
    if any(k in t for k in ROTOR_OVERRIDE_KEYWORDS):
        return False
    return any(k in t for k in FIXED_WING_BLOCK_KEYWORDS)


def is_blocked_corporate(title: str, description: str) -> bool:
    t = f"{title}\n{description}".lower()
    if any(k in t for k in AVIATION_CORE_KEEP):
        return False
    return any(k in t for k in CORPORATE_BLOCK_KEYWORDS)


def category_override(title: str, description: str, current: str) -> str:
    t = f"{title}\n{description}".lower()

    if any(k in t for k in ["paramedic", "flight nurse", "registered nurse", " rn ", "emt", "medic"]):
        return "Medical"
    if any(k in t for k in ["b1.3", "b2", "avionics", "a&p", "a and p", "mechanic", "engineer", "maintenance", "part-145"]):
        return "Maintenance"
    if any(k in t for k in ["pilot", "captain", "co-pilot", "copilot", "sic", "pic"]):
        return "Pilot"
    if any(k in t for k in ["dispatcher", "dispatch"]):
        return "Dispatch"
    if any(k in t for k in ["flight operations", "operations officer", "ops officer"]):
        return "Operations"

    return current if current in CATEGORY_ENUM else "Other"


# =====================
# LINK FILTERING
# =====================
BAD_WORDS = [
    "privacy", "cookie", "legal", "terms", "accessibility", "sustainability",
    "diversity", "community", "stories", "leadership", "culture", "history",
    "capabilities", "companies", "supplier", "veteran", "internship-program",
    "pay-benefits", "press", "news", "blog"
]


def is_likely_job_link(url: str) -> bool:
    u = (url or "").strip()
    if not u or not is_http_url(u):
        return False

    ul = u.lower()

    if ul.startswith(("mailto:", "tel:", "javascript:")):
        return False

    if ul.endswith(PDF_EXT):
        return not is_bad_pdf(ul)

    if any(w in ul for w in BAD_WORDS):
        return False

    # iCIMS: ONLY job detail pages
    if "icims.com" in ul:
        if "/jobs/search" in ul or "#icims_content_iframe" in ul:
            return False
        return bool(re.search(r"/jobs/\d+/.+/job", ul))

    # Workday
    if "myworkdayjobs.com" in ul:
        return "/job/" in ul

    # Jobtoolz
    if "jobtoolz.com" in ul:
        return "/en/" in ul and "cookie" not in ul and "privacy" not in ul

    # HeliService
    if "jobs.heliservice.de" in ul:
        return "id=" in ul

    # Other ATS are allowed but may require fallback
    if any(h in ul for h in ATS_HINTS):
        return True

    # As a safety default, only keep obvious job/career links
    return any(x in ul for x in ["/job", "/jobs", "/career", "/careers", "requisition", "vacancy", "vacancies"])


def collect_job_links_from_page(base_url: str, html_content: str) -> List[str]:
    soup = BeautifulSoup(html_content, "lxml")
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(base_url, href)
        if is_http_url(full) and is_likely_job_link(full):
            links.append(full)

    # unique preserve order
    seen = set()
    out = []
    for l in links:
        if l not in seen:
            out.append(l)
            seen.add(l)

    return out[:MAX_JOB_LINKS_PER_SOURCE]


# =====================
# OPENAI (fail-soft + backoff)
# =====================
def openai_post_with_backoff(payload: dict, timeout_s: int) -> dict:
    backoff = 5
    for attempt in range(1, 8):
        resp = requests.post(
            "https://api.openai.com/v1/responses",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=timeout_s,
        )
        if resp.status_code == 429:
            print(f"OpenAI 429 rate limit. Sleeping {backoff}s (attempt {attempt}/7)...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        resp.raise_for_status()
        return resp.json()

    return {"output": [{"content": [{"type": "output_text", "text": ""}]}]}


def extract_output_text(data: dict) -> str:
    text_out = ""
    for item in data.get("output", []):
        for c in item.get("content", []):
            if c.get("type") == "output_text":
                text_out += c.get("text", "")
    return (text_out or "").strip()


def openai_extract_job(source_url: str, raw_text: str, employer: str) -> Optional[Dict]:
    instructions = f"""
Return ONLY valid JSON with these keys:
title (string),
employer (string),
location (string),
remote (boolean),
apply_url (string),
category (one of {CATEGORY_ENUM}),
description (string),
salary_line (string or empty)

Rules:
- Do NOT guess. Use only the provided text.
- employer MUST be exactly: "{employer}"
- title must be the actual job title (not empty).
- location: if stated, use it; else "Not specified".
- remote: true only if explicitly stated Remote/Hybrid/Telecommute; else false.
- apply_url: use a clear apply link if present; otherwise use source_url.
- salary_line: only if pay is explicitly stated, otherwise "".
- description MUST be the FULL job posting text (do not summarize).
Return ONLY JSON.
"""

    payload = {
        "model": "gpt-4o-mini",
        "input": [
            {"role": "system", "content": "Extract structured job fields for a job board. Output ONLY JSON."},
            {"role": "user", "content": f"source_url: {source_url}\n\n{instructions}\n\nJOB PAGE TEXT:\n{raw_text}"},
        ],
        "temperature": 0.0,
    }

    data = openai_post_with_backoff(payload, timeout_s=90)
    job = safe_json_load(extract_output_text(data))

    if job is None:
        payload["model"] = "gpt-4o"
        data2 = openai_post_with_backoff(payload, timeout_s=120)
        job = safe_json_load(extract_output_text(data2))

    if job is None:
        print("OpenAI returned non-JSON/empty output for:", source_url)
        return None

    for k in ["title", "location", "apply_url", "description", "salary_line", "category"]:
        job[k] = str(job.get(k, "")).strip()

    job["employer"] = employer
    job["source_url"] = source_url
    job["guid"] = strip_tracking(source_url)

    if not isinstance(job.get("remote"), bool):
        job["remote"] = False

    job["category"] = category_override(job.get("title", ""), job.get("description", ""), job.get("category", "Other"))
    job["description"] = dedupe_lines_keep_order(job.get("description", ""))

    if not job.get("apply_url"):
        job["apply_url"] = source_url

    return job


# =====================
# STORE
# =====================
def is_valid_job(job: Dict) -> bool:
    title = (job.get("title") or "").strip()
    desc = (job.get("description") or "").strip()
    link = (job.get("apply_url") or job.get("source_url") or "").lower()

    if not title:
        return False
    if len(desc) < 120:
        return False
    if link.endswith(PDF_EXT) and is_bad_pdf(link):
        return False
    return True


def load_store() -> Dict[str, Dict]:
    if not os.path.exists(STATE_JSON):
        return {}
    try:
        data = json.load(open(STATE_JSON, "r", encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def save_store(store: Dict[str, Dict]) -> None:
    with open(STATE_JSON, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2)


def prune_store(store: Dict[str, Dict]) -> Dict[str, Dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    out = {}
    for guid, job in store.items():
        last_seen = parse_iso(job.get("last_seen", "")) or parse_iso(job.get("first_seen", ""))
        if last_seen and last_seen >= cutoff:
            out[guid] = job
    return out


def scrub_store(store: Dict[str, Dict]) -> Dict[str, Dict]:
    cleaned = {}
    for guid, job in store.items():
        if not job.get("apply_url"):
            job["apply_url"] = job.get("source_url", guid)
        if not is_valid_job(job):
            continue
        if is_fixed_wing_job(job.get("title", ""), job.get("description", "")):
            continue
        if is_blocked_corporate(job.get("title", ""), job.get("description", "")):
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
            existing.update(j)
            existing["last_seen"] = now
            store[guid] = existing
        else:
            j["first_seen"] = now
            j["last_seen"] = now
            store[guid] = j
    return store


# =====================
# RSS OUTPUT
# =====================
def build_feed(items: List[Dict]) -> str:
    out: List[str] = []
    out.append('<?xml version="1.0" encoding="UTF-8"?>')
    out.append('<rss version="2.0">')
    out.append("  <channel>")
    out.append("    <title>Helicopter-Jobs Aggregated Feed</title>")
    out.append("    <link>https://helicopter-jobs.com</link>")
    out.append("    <description>Direct-employer helicopter jobs</description>")

    for j in items:
        title = rss_escape(j.get("title", ""))
        employer = rss_escape(j.get("employer", ""))
        link = rss_escape(j.get("apply_url", j.get("source_url", "")))
        guid = rss_escape(j.get("guid", j.get("source_url", "")))

        fs = parse_iso(j.get("first_seen", ""))
        pubdate = rfc2822(fs) if fs else rfc2822(datetime.now(timezone.utc))

        category = rss_escape(j.get("category", "Other"))
        location = rss_escape(j.get("location", "Not specified") or "Not specified")
        remote = "true" if j.get("remote") else "false"

        desc = (j.get("description") or "").strip()
        if j.get("salary_line"):
            desc = f"{j['salary_line']}\n\n{desc}".strip()

        desc_html = format_description_for_jboard(desc)

        out.append("    <item>")
        out.append(f"      <title>{title}</title>")
        out.append(f"      <employer>{employer}</employer>")
        out.append(f"      <link>{link}</link>")
        out.append(f'      <guid isPermaLink="false">{guid}</guid>')
        out.append(f"      <pubDate>{pubdate}</pubDate>")
        out.append(f"      <category>{category}</category>")
        out.append(f"      <location>{location}</location>")
        out.append(f"      <remote>{remote}</remote>")
        out.append("      <description><![CDATA[")
        out.append(desc_html)
        out.append("]]></description>")
        out.append("    </item>")

    out.append("  </channel>")
    out.append("</rss>")
    return "\n".join(out)


# =====================
# MAIN
# =====================
def main():
    sources = read_sources()
    print(f"Using sources file: {SOURCES_FILE}")
    print(f"Loaded {len(sources)} sources")
    print(f"MAX_JOB_LINKS_PER_SOURCE={MAX_JOB_LINKS_PER_SOURCE} RETENTION_DAYS={RETENTION_DAYS} MAX_TOTAL_JOBS={MAX_TOTAL_JOBS}")

    new_jobs: List[Dict] = []
    seen_job_urls: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        for src in sources:
            if len(new_jobs) >= MAX_TOTAL_JOBS:
                print(f"Reached MAX_TOTAL_JOBS={MAX_TOTAL_JOBS}. Stopping early.")
                break

            employer = employer_for_source(src)
            print(f"\nSOURCE: {src}")
            print(f"Employer: {employer}")

            try:
                page.goto(src, timeout=BROWSER_TIMEOUT_MS, wait_until="domcontentloaded")
                sleep_a_bit()
                listing_html = page.content()
            except Exception as e:
                print(f"  Failed to load listing page: {e}")
                continue

            links = collect_job_links_from_page(src, listing_html)
            print(f"  Found {len(links)} candidate job links")

            if not links:
                links = [src]

            for job_url in links:
                if len(new_jobs) >= MAX_TOTAL_JOBS:
                    break

                if job_url in seen_job_urls:
                    continue
                seen_job_urls.add(job_url)

                if not is_http_url(job_url):
                    continue

                try:
                    if job_url.lower().endswith(PDF_EXT):
                        if is_bad_pdf(job_url):
                            continue
                        raw_text = fetch_pdf_text(job_url)
                        source_url = job_url
                    else:
                        page.goto(job_url, timeout=BROWSER_TIMEOUT_MS, wait_until="domcontentloaded")
                        sleep_a_bit()
                        raw_text = extract_text_from_html(page.content())
                        source_url = job_url

                    if len(raw_text) < 200:
                        continue

                    job = openai_extract_job(source_url, raw_text, employer)
                    if not job or not is_valid_job(job):
                        continue

                    if is_fixed_wing_job(job.get("title", ""), job.get("description", "")):
                        continue
                    if is_blocked_corporate(job.get("title", ""), job.get("description", "")):
                        continue

                    new_jobs.append(job)
                    print(f"   + {job.get('title','(no title)')[:90]}")
                except Exception as e:
                    print(f"   - Failed: {job_url} err: {str(e)[:160]}")
                    continue

        browser.close()

    store = load_store()
    store = upsert_jobs(store, new_jobs)
    store = scrub_store(store)
    store = prune_store(store)
    save_store(store)

    items = list(store.values())
    items.sort(key=lambda j: (parse_iso(j.get("first_seen", "")) or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)

    xml = build_feed(items)
    with open(OUT_XML, "w", encoding="utf-8") as f:
        f.write(xml)

    print(f"\nMerged {len(new_jobs)} new jobs. Store now has {len(items)} jobs.")
    print(f"Wrote {OUT_XML}")


if __name__ == "__main__":
    main()
