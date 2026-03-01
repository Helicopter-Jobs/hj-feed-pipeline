# scrape.py
# Copy/paste this ENTIRE file into your repo as scrape.py (replace everything).
# Then Commit, then Actions → Run workflow.
#
# What this fixes:
# - OpenAI 429 rate limit: automatic retry/backoff
# - Filters junk links (privacy/cookie/tel/mailto/# anchors) so you don't waste OpenAI calls
# - Keeps per-source link count low (25) so it runs reliably
# - Writes proper RSS XML with <item> entries for JBoard

import os
import re
import time
import json
import html
from datetime import datetime, timezone
from typing import Dict, List, Set
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
    raise SystemExit(
        "Missing OPENAI_API_KEY (GitHub repo → Settings → Secrets and variables → Actions)."
    )

SOURCES_FILE = "sources.txt"
OUT_XML = "feed.xml"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

BROWSER_TIMEOUT_MS = 45_000

# IMPORTANT: keep this small so you don't hit OpenAI rate limits.
MAX_JOB_LINKS_PER_SOURCE = 25
MAX_TEXT_CHARS_TO_LLM = 35_000

CATEGORY_ENUM = ["Pilot", "Maintenance", "Medical", "Dispatch", "Operations", "Other"]

# Your exact employer names:
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

JOB_LINK_HINTS = [
    "/job",
    "/jobs",
    "/career",
    "/careers",
    "/position",
    "/positions",
    "/vacancy",
    "/vacancies",
    "jobid=",
    "requisition",
    "/stellen",
    "/vacatures",
    "/en-us/careers/job",
]

PDF_EXT = ".pdf"


# =====================
# BASIC HELPERS
# =====================
def normalize_space(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def domain_from_url(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower().replace("www.", "")
        return host
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
    """
    Reads sources.txt.
    Robust to either:
      - One URL per line (ideal)
      - Accidentally pasted as one long line (splits on whitespace)
    """
    raw = open(SOURCES_FILE, "r", encoding="utf-8").read().strip()
    if not raw:
        raise SystemExit("sources.txt is empty.")

    lines = [ln.strip() for ln in raw.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    if not lines:
        raise SystemExit("sources.txt has no usable URLs.")

    # If it's one line with multiple URLs, split on whitespace.
    if len(lines) == 1 and " " in lines[0] and "http" in lines[0]:
        parts = [p.strip() for p in lines[0].split() if p.strip().startswith("http")]
        return dedupe(parts)

    urls: List[str] = []
    for ln in lines:
        if " " in ln and "http" in ln:
            for p in ln.split():
                if p.strip().startswith("http"):
                    urls.append(p.strip())
        else:
            urls.append(ln)
    return dedupe(urls)


def dedupe(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def is_likely_job_link(url: str) -> bool:
    h = (url or "").strip().lower()

    # Skip non-web links
    if h.startswith("mailto:") or h.startswith("tel:") or h.startswith("javascript:"):
        return False

    # Skip anchors-only
    if h.endswith("#") or "#content" in h:
        return False

    # PDFs allowed (some employers post job postings as PDFs)
    if h.endswith(PDF_EXT):
        return True

    # Block obvious non-job pages
    bad_words = [
        "privacy", "cookie", "legal", "terms", "accessibility", "sustainability",
        "diversity", "community", "stories", "leadership", "culture", "history",
        "capabilities", "companies", "supplier", "veteran", "internship", "pay-benefits",
        "press", "news", "blog"
    ]
    if any(w in h for w in bad_words):
        return False

    # Tight ATS rules
    if "icims.com" in h:
        return "/jobs/" in h and "/job" in h
    if "myworkdayjobs.com" in h:
        return "/job/" in h
    if "jobtoolz.com" in h:
        return "/en/" in h and "cookie" not in h and "privacy" not in h

    # Generic patterns
    if any(x in h for x in JOB_LINK_HINTS):
        return True

    # If it is on a known ATS domain, keep it (some use different patterns)
    if any(x in h for x in ATS_HINTS):
        return True

    return False


def extract_text_from_html(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "lxml")
    for tag in soup(["script", "style", "noscript", "svg", "canvas"]):
        tag.decompose()
    text = soup.get_text("\n")
    return normalize_space(text)[:MAX_TEXT_CHARS_TO_LLM]


def fetch_pdf_text(url: str) -> str:
    r = requests.get(url, timeout=60, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    tmp = "/tmp/doc.pdf"
    with open(tmp, "wb") as f:
        f.write(r.content)
    text = pdf_extract_text(tmp) or ""
    return normalize_space(text)[:MAX_TEXT_CHARS_TO_LLM]


def rfc2822_now() -> str:
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def rss_escape(s: str) -> str:
    return html.escape(s or "", quote=True)


def stable_guid(url: str) -> str:
    return url


# =====================
# OPENAI (with 429 backoff)
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
            print(f"    OpenAI 429 rate limit. Sleeping {backoff}s (attempt {attempt}/7) ...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("OpenAI rate limited too long (429). Reduce links per run or try later.")


def openai_extract_job(source_url: str, raw_text: str, employer: str) -> Dict:
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
- location:
  - If an explicit location is stated, use it.
  - If location is not specified anywhere, use the country if clearly stated; otherwise "Not specified".
- remote: true only if explicitly stated Remote/Hybrid/Telecommute; otherwise false.
- apply_url: use a clear apply link if present; otherwise use source_url.
- salary_line:
  - Only if pay is explicitly stated. Format like:
    Pay: $X–$Y/hr (from posting)
    Pay: $X–$Y/yr (from posting)
  - Otherwise return "".
- description: clean plain text, keep bullets; no HTML.
"""

    payload = {
        "model": "gpt-4o-mini",
        "input": [
            {"role": "system", "content": "Extract structured job fields for a helicopter job board. Be precise; do not guess."},
            {"role": "user", "content": f"source_url: {source_url}\n\n{instructions}\n\nJOB PAGE TEXT:\n{raw_text}"},
        ],
        "temperature": 0.0,
    }

    data = openai_post_with_backoff(payload, timeout_s=90)

    def get_output_text(d: dict) -> str:
        out = ""
        for item in d.get("output", []):
            for c in item.get("content", []):
                if c.get("type") == "output_text":
                    out += c.get("text", "")
        return out.strip()

    text_out = get_output_text(data)

    try:
        job = json.loads(text_out)
    except Exception:
        payload["model"] = "gpt-4o"
        data2 = openai_post_with_backoff(payload, timeout_s=120)
        job = json.loads(get_output_text(data2))

    # enforce + sanitize
    job["employer"] = employer
    job["source_url"] = source_url
    job["guid"] = stable_guid(source_url)

    if job.get("category") not in CATEGORY_ENUM:
        job["category"] = "Other"

    if not isinstance(job.get("remote"), bool):
        job["remote"] = False

    for k in ["title", "location", "apply_url", "description", "salary_line"]:
        job[k] = str(job.get(k, "")).strip()

    if not job["apply_url"]:
        job["apply_url"] = source_url

    if job.get("salary_line"):
        job["salary_line"] = normalize_space(job["salary_line"]).replace("\n", " ")

    return job


# =====================
# CRAWL + LINK DISCOVERY
# =====================
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
    links = dedupe(links)
    return links[:MAX_JOB_LINKS_PER_SOURCE]


def dedupe_jobs_by_guid(jobs: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for j in jobs:
        g = (j.get("guid") or j.get("source_url") or "").strip().lower()
        if not g or g in seen:
            continue
        seen.add(g)
        out.append(j)
    return out


# =====================
# RSS FEED OUTPUT
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

    for j in items:
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

        out.append("    <item>")
        out.append(f"      <title>{title}</title>")
        out.append(f"      <employer>{employer}</employer>")
        out.append(f"      <link>{link}</link>")
        out.append(f'      <guid isPermaLink="true">{guid}</guid>')
        out.append(f"      <pubDate>{pubdate}</pubDate>")
        out.append(f"      <category>{category}</category>")
        out.append(f"      <location>{location}</location>")
        out.append(f"      <remote>{remote}</remote>")
        out.append("      <description><![CDATA[")
        out.append(desc)
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
    print(f"Loaded {len(sources)} sources")

    jobs: List[Dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        for src in sources:
            dom = domain_from_url(src)
            employer = employer_for_domain(dom)

            print(f"\nSOURCE: {src}")
            print(f"Employer: {employer}")

            # Load listing page
            try:
                page.goto(src, timeout=BROWSER_TIMEOUT_MS, wait_until="networkidle")
                time.sleep(1.0)
                listing_html = page.content()
            except Exception as e:
                print(f"  Failed to load listing page: {e}")
                continue

            links = collect_job_links_from_page(src, listing_html)
            print(f"  Found {len(links)} candidate links")

            if not links:
                # fall back: treat the source as a single page
                links = [src]

            for job_url in links:
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

                    job = openai_extract_job(source_url=source_url, raw_text=raw_text, employer=employer)
                    jobs.append(job)
                    print(f"   + {job.get('title','(no title)')[:90]}")
                except Exception as e:
                    print(f"   - Failed: {job_url} err: {str(e)[:160]}")
                    continue

        browser.close()

    jobs = dedupe_jobs_by_guid(jobs)
    xml = build_feed(jobs)

    with open(OUT_XML, "w", encoding="utf-8") as f:
        f.write(xml)

    print(f"\nWrote {OUT_XML} with {len(jobs)} items")


if __name__ == "__main__":
    main()
