import os
import re
import time
import json
import html
from datetime import datetime, timezone
from typing import Dict, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
if not OPENAI_API_KEY:
    raise SystemExit("Missing OPENAI_API_KEY (GitHub repo → Settings → Secrets and variables → Actions).")

SOURCES_FILE = "sources.txt"
OUT_XML = "feed.xml"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

BROWSER_TIMEOUT_MS = 45_000
MAX_JOB_LINKS_PER_SOURCE = 5
MAX_TEXT_CHARS_TO_LLM = 25000

CATEGORY_ENUM = ["Pilot", "Maintenance", "Medical", "Dispatch", "Operations", "Other"]

# TEST employer fixed
EMPLOYER = "HeliService"


def normalize_space(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def read_sources() -> List[str]:
    raw = open(SOURCES_FILE, "r", encoding="utf-8").read().strip()
    if not raw:
        raise SystemExit("sources.txt is empty.")
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    return lines


def extract_text_from_html(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "lxml")
    for tag in soup(["script", "style", "noscript", "svg", "canvas"]):
        tag.decompose()
    text = soup.get_text("\n")
    return normalize_space(text)[:MAX_TEXT_CHARS_TO_LLM]


def rss_escape(s: str) -> str:
    return html.escape(s or "", quote=True)


def rfc2822_now() -> str:
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


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
    raise RuntimeError("OpenAI rate limited too long (429). Try later.")


def openai_extract_job(source_url: str, raw_text: str) -> Dict:
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
- employer MUST be exactly: "{EMPLOYER}"
- location: if stated, use it; else "Not specified".
- remote: true only if explicitly stated; else false.
- apply_url: use a clear apply link if present; otherwise use source_url.
- salary_line:
  - Only if pay is explicitly stated.
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

    text_out = ""
    for item in data.get("output", []):
        for c in item.get("content", []):
            if c.get("type") == "output_text":
                text_out += c.get("text", "")
    text_out = text_out.strip()

    try:
        job = json.loads(text_out)
    except Exception:
        payload["model"] = "gpt-4o"
        data2 = openai_post_with_backoff(payload, timeout_s=120)
        text2 = ""
        for item in data2.get("output", []):
            for c in item.get("content", []):
                if c.get("type") == "output_text":
                    text2 += c.get("text", "")
        job = json.loads(text2.strip())

    # enforce
    job["employer"] = EMPLOYER
    job["source_url"] = source_url
    job["guid"] = source_url

    if job.get("category") not in CATEGORY_ENUM:
        job["category"] = "Other"
    if not isinstance(job.get("remote"), bool):
        job["remote"] = False

    for k in ["title", "location", "apply_url", "description", "salary_line"]:
        job[k] = str(job.get(k, "")).strip()
    if not job["apply_url"]:
        job["apply_url"] = source_url

    return job


def is_heliservice_job_link(url: str) -> bool:
    u = url.lower()
    # HeliService uses /de?id=xxxx style job pages; keep those
    if "jobs.heliservice.de" in u and "id=" in u:
        return True
    return False


def collect_job_links(listing_url: str, listing_html: str) -> List[str]:
    soup = BeautifulSoup(listing_html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(listing_url, href)
        if is_heliservice_job_link(full):
            links.append(full)

    # unique, preserve order
    seen = set()
    out = []
    for l in links:
        if l not in seen:
            out.append(l)
            seen.add(l)
    return out[:MAX_JOB_LINKS_PER_SOURCE]


def build_feed(items: List[Dict]) -> str:
    pubdate = rfc2822_now()
    out = []
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


def main():
    sources = read_sources()
    if len(sources) != 1:
        print("WARNING: This TEST script expects exactly 1 source in sources.txt (HeliService).")

    listing_url = sources[0]
    print(f"Listing: {listing_url}")

    jobs = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        page.goto(listing_url, timeout=BROWSER_TIMEOUT_MS, wait_until="networkidle")
        time.sleep(1.0)
        listing_html = page.content()

        job_links = collect_job_links(listing_url, listing_html)
        print(f"Found {len(job_links)} HeliService job links")

        for job_url in job_links:
            page.goto(job_url, timeout=BROWSER_TIMEOUT_MS, wait_until="networkidle")
            time.sleep(0.8)
            raw_text = extract_text_from_html(page.content())
            if len(raw_text) < 200:
                continue
            job = openai_extract_job(job_url, raw_text)
            jobs.append(job)
            print(f"+ {job.get('title','(no title)')[:90]}")

        browser.close()

    xml = build_feed(jobs)
    open(OUT_XML, "w", encoding="utf-8").write(xml)
    print(f"Wrote {OUT_XML} with {len(jobs)} items")


if __name__ == "__main__":
    main()
