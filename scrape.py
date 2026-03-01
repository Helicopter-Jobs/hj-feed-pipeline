import os
import re
import time
import json
import html
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set
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
OUT_XML = "feed.xml"
STATE_JSON = "jobs.json"  # master merged job store

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

BROWSER_TIMEOUT_MS = 45_000

# Override per workflow if needed (recommended):
MAX_JOB_LINKS_PER_SOURCE = int(os.environ.get("MAX_JOB_LINKS_PER_SOURCE", "15"))
MAX_TEXT_CHARS_TO_LLM = int(os.environ.get("MAX_TEXT_CHARS_TO_LLM", "18000"))
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "30"))

CATEGORY_ENUM = ["Pilot", "Maintenance", "Medical", "Dispatch", "Operations", "Other"]
JOBTYPE_ENUM = ["Full-time", "Part-time", "Contract", "Temporary", "Internship", "Rotation", "Other"]

EMPLOYER_MAP = {
    "castleair.co.uk": "Castle Air Aviation",
    "jobs.heliservice.de": "HeliService",
    "sloanehelicopters.com": "Sloane Helicopters",
    "nhv-group.jobtoolz.com": "NHV Group",
    "bristow.wd1.myworkdayjobs.com": "Bristow Group",
    "allcareers-quanta.icims.com": "PJ Helicopters",
    "hphelicopters.com": "HIGH PERFORMANCE HELICOPTERS",
}

ATS_HINTS = [
    "myworkdayjobs.com",
    "icims.com",
    "jobtoolz.com",
    "smartrecruiters.com",
    "greenhouse.io",
    "lever.co",
    "workable.com",
]

PDF_EXT = ".pdf"


# =====================
# HELPERS
# =====================
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


def read_sources() -> List[str]:
    raw = open(SOURCES_FILE, "r", encoding="utf-8").read().strip()
    if not raw:
        raise SystemExit(f"{SOURCES_FILE} is empty.")

    lines = [ln.strip() for ln in raw.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    urls: List[str] = []
    for ln in lines:
        if "http" in ln and " " in ln:
            for p in ln.split():
                if p.startswith("http"):
                    urls.append(p.strip())
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


def rfc2822_now() -> str:
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


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


# =====================
# LIGHTWEIGHT NORMALIZERS (Automation polish)
# =====================
REMOTE_HINTS = [
    "remote", "work from home", "telecommute", "telecommuting", "home-based",
    "hybrid", "virtual", "fully remote"
]

ROTATION_HINTS = [
    "14x14", "28x28", "21x21", "7 on", "7 off", "on/off", "rotation", "rotational"
]

COUNTRY_HINTS = [
    "united states", "usa", "u.s.", "us", "canada", "united kingdom", "uk", "england",
    "scotland", "wales", "ireland", "france", "germany", "netherlands", "belgium",
    "denmark", "norway", "sweden", "poland", "brazil", "suriname", "australia",
    "new zealand", "uae", "united arab emirates"
]


def normalize_remote(remote_value: bool, text: str) -> bool:
    if remote_value is True:
        return True
    t = (text or "").lower()
    return any(h in t for h in REMOTE_HINTS)


def normalize_location(location: str, text: str) -> str:
    loc = (location or "").strip()
    if loc and loc.lower() != "not specified":
        return loc

    t = (text or "").lower()
    for c in COUNTRY_HINTS:
        if c in t:
            # title-case basic countries; keep US as "United States"
            if c in ("usa", "u.s.", "us"):
                return "United States"
            if c == "uk":
                return "United Kingdom"
            return c.title()
    return "Not specified"


def infer_job_type(title: str, desc: str) -> str:
    t = f"{title}\n{desc}".lower()

    if any(x in t for x in ["intern", "internship"]):
        return "Internship"
    if any(x in t for x in ["part-time", "part time"]):
        return "Part-time"
    if any(x in t for x in ["contract", "1099", "fixed term", "fixed-term"]):
        return "Contract"
    if any(x in t for x in ["temporary", "temp"]):
        return "Temporary"
    if any(x in t for x in ROTATION_HINTS):
        return "Rotation"
    if any(x in t for x in ["full-time", "full time"]):
        return "Full-time"
    # default
    return "Other"


def infer_category(title: str, desc: str) -> str:
    t = f"{title}\n{desc}".lower()

    # Pilot
    if any(k in t for k in ["pilot", "captain", "co-pilot", "copilot", "sic", "pic", "first officer"]):
        return "Pilot"

    # Maintenance
    if any(k in t for k in ["mechanic", "engineer", "avionics", "a&p", "a and p", "b1.3", "b2", "part-145", "maintenance"]):
        return "Maintenance"

    # Medical
    if any(k in t for k in ["paramedic", "nurse", "rn", "flight nurse", "emt", "clinician", "medic"]):
        return "Medical"

    # Dispatch / Ops
    if any(k in t for k in ["dispatcher", "dispatch"]):
        return "Dispatch"

    if any(k in t for k in ["operations", "ops", "ground operations", "flight operations", "coordinator", "coordination"]):
        return "Operations"

    return "Other"


def format_description_for_jboard(desc: str) -> str:
    """
    Keeps it readable in JBoard:
    - Converts newlines to <br/>
    - Adds extra break before common headings
    """
    d = (desc or "").strip()
    if not d:
        return ""

    # normalize bullet characters slightly
    d = d.replace("•", "-").replace("·", "-")

    # keep line breaks in JBoard
    d = d.replace("\n", "<br/>")

    # add breathing room before headings
    for h in ["DUTIES", "RESPONSIBILITIES", "REQUIREMENTS", "QUALIFICATIONS", "SKILLS", "EXPERIENCE", "WHAT YOU’LL", "WHAT YOU'LL"]:
        d = re.sub(rf"(<br/>)+\s*{h}", r"<br/><br/>" + h, d, flags=re.IGNORECASE)

    return d


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
    u = (url or "").strip().lower()

    if u.startswith(("mailto:", "tel:", "javascript:")):
        return False
    if u.endswith("#") or "#content" in u:
        return False

    if u.endswith(PDF_EXT):
        return True

    if any(w in u for w in BAD_WORDS):
        return False

    # iCIMS: ONLY job detail pages
    if "icims.com" in u:
        if "/jobs/search" in u or "searchkeyword=" in u or "#icims_content_iframe" in u:
            return False
        return bool(re.search(r"/jobs/\d+/.+/job", u))

    # Workday: only job detail pages
    if "myworkdayjobs.com" in u:
        return "/job/" in u

    # Jobtoolz: /en/<slug> jobs
    if "jobtoolz.com" in u:
        return "/en/" in u and "cookie" not in u and "privacy" not in u

    # HeliService: /de?id=...
    if "jobs.heliservice.de" in u:
        return "id=" in u

    # Known ATS fallback
    if any(h in u for h in ATS_HINTS):
        return True

    return False


def collect_job_links_from_page(base_url: str, html_content: str) -> List[str]:
    soup = BeautifulSoup(html_content, "lxml")
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(base_url, href)
        if is_likely_job_link(full):
            links.append(full)

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
job_type (one of {JOBTYPE_ENUM}),
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
- description MUST be the FULL job posting text from the page text.
  Do NOT summarize. Do NOT paraphrase. Keep original wording.
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

    # enforce base fields
    job["employer"] = employer
    job["source_url"] = source_url
    job["guid"] = source_url

    # sanitize types
    if job.get("category") not in CATEGORY_ENUM:
        job["category"] = "Other"
    if job.get("job_type") not in JOBTYPE_ENUM:
        job["job_type"] = "Other"
    if not isinstance(job.get("remote"), bool):
        job["remote"] = False

    for k in ["title", "location", "apply_url", "description", "salary_line"]:
        job[k] = str(job.get(k, "")).strip()
    if not job["apply_url"]:
        job["apply_url"] = source_url

    # Automation polish: normalize/override to be consistent
    job["remote"] = normalize_remote(job.get("remote", False), job.get("description", ""))
    job["location"] = normalize_location(job.get("location", ""), job.get("description", ""))
    job["category"] = infer_category(job.get("title", ""), job.get("description", ""))

    # if model didn't set job_type well, infer it
    if job.get("job_type") == "Other":
        job["job_type"] = infer_job_type(job.get("title", ""), job.get("description", ""))

    return job


# =====================
# JOB VALIDATION / STORE SCRUB
# =====================
def is_valid_job(job: Dict) -> bool:
    title = (job.get("title") or "").strip()
    desc = (job.get("description") or "").strip()
    link = (job.get("apply_url") or job.get("source_url") or "").lower()

    if not title:
        return False
    if len(desc) < 120:
        return False
    if "icims.com" in link and ("/jobs/search" in link or "searchkeyword=" in link or "#icims_content_iframe" in link):
        return False
    return True


# =====================
# MERGE STORE (jobs.json)
# =====================
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
    pubdate = rfc2822_now()
    out: List[str] = []
    out.append('<?xml version="1.0" encoding="UTF-8"?>')
    out.append('<rss version="2.0">')
    out.append("  <channel>")
    out.append("    <title>Helicopter-Jobs Aggregated Feed</title>")
    out.append("    <link>https://helicopter-jobs.com</link>")
    out.append("    <description>Direct-employer helicopter jobs</description>")

    # Sort by most recently seen first (polish)
    def sort_key(j: Dict):
        dt = parse_iso(j.get("last_seen", "")) or parse_iso(j.get("first_seen", "")) or datetime(1970, 1, 1, tzinfo=timezone.utc)
        return dt

    items_sorted = sorted(items, key=sort_key, reverse=True)

    for j in items_sorted:
        title = rss_escape(j.get("title", ""))
        employer = rss_escape(j.get("employer", ""))
        link = rss_escape(j.get("apply_url", j.get("source_url", "")))
        guid = rss_escape(j.get("guid", j.get("source_url", "")))
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
        out.append(f'      <guid isPermaLink="true">{guid}</guid>')
        out.append(f"      <pubDate>{pubdate}</pubDate>")
        out.append(f"      <category>{category}</category>")
        out.append(f"      <location>{location}</location>")
        out.append(f"      <remote>{remote}</remote>")
        # job_type is extra metadata; JBoard may ignore it (safe)
        out.append(f"      <job_type>{rss_escape(j.get('job_type','Other'))}</job_type>")
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
    print(f"MAX_JOB_LINKS_PER_SOURCE={MAX_JOB_LINKS_PER_SOURCE} RETENTION_DAYS={RETENTION_DAYS}")

    new_jobs: List[Dict] = []
    seen_job_urls: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        for src in sources:
            employer = employer_for_domain(domain_from_url(src))
            print(f"\nSOURCE: {src}")
            print(f"Employer: {employer}")

            try:
                page.goto(src, timeout=BROWSER_TIMEOUT_MS, wait_until="networkidle")
                time.sleep(1.0)
                listing_html = page.content()
            except Exception as e:
                print(f"  Failed to load listing page: {e}")
                continue

            links = collect_job_links_from_page(src, listing_html)
            print(f"  Found {len(links)} candidate job links")
            if not links:
                links = [src]

            for job_url in links:
                if job_url in seen_job_urls:
                    continue
                seen_job_urls.add(job_url)

                try:
                    if job_url.lower().endswith(PDF_EXT):
                        raw_text = fetch_pdf_text(job_url)
                        source_url = job_url
                    else:
                        page.goto(job_url, timeout=BROWSER_TIMEOUT_MS, wait_until="networkidle")
                        time.sleep(0.8)
                        raw_text = extract_text_from_html(page.content())
                        source_url = job_url

                    if len(raw_text) < 200:
                        continue

                    job = openai_extract_job(source_url, raw_text, employer)
                    if not job:
                        continue
                    if not is_valid_job(job):
                        print("   - Skipped invalid job:", source_url)
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
    xml = build_feed(items)
    with open(OUT_XML, "w", encoding="utf-8") as f:
        f.write(xml)

    print(f"\nMerged {len(new_jobs)} new jobs. Store now has {len(items)} jobs.")
    print(f"Wrote {OUT_XML}")


if __name__ == "__main__":
    main()
