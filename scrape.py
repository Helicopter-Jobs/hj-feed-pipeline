import os, re, time, html, json
from datetime import datetime, timezone
from typing import Dict, List
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from pdfminer.high_level import extract_text as pdf_extract_text

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
if not OPENAI_API_KEY:
    raise SystemExit("Missing OPENAI_API_KEY (add it in GitHub repo → Settings → Secrets and variables → Actions).")

SOURCES_FILE = "sources.txt"
OUT_XML = "feed.xml"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

BROWSER_TIMEOUT_MS = 45_000
MAX_JOB_LINKS_PER_SOURCE = 200

CATEGORY_ENUM = ["Pilot", "Maintenance", "Medical", "Dispatch", "Operations", "Other"]

EMPLOYER_MAP = {
    "castleair.co.uk": "Castle Air Aviation",
    "jobs.heliservice.de": "HeliService",
    "sloanehelicopters.com": "Sloane Helicopters",
    "nhv-group.jobtoolz.com": "NHV Group",
    "bristow.wd1.myworkdayjobs.com": "Bristow Group",
    "allcareers-quanta.icims.com": "PJ Helicopters",
    "hphelicopters.com": "HIGH PERFORMANCE HELICOPTERS",
}

JOB_LINK_HINTS = [
    "/job", "/jobs", "/career", "/careers", "/position", "/positions",
    "/vacancy", "/vacancies", "jobid=", "requisition", "/stellen", "/vacatures",
]
ATS_HINTS = [
    "myworkdayjobs.com", "icims.com", "jobtoolz.com", "smartrecruiters.com",
    "greenhouse.io", "lever.co", "workable.com",
]
PDF_HINT = ".pdf"


def read_sources() -> List[str]:
    urls = []
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        for line in f:
            u = line.strip()
            if not u or u.startswith("#"):
                continue
            urls.append(u)
    return urls


def normalize_space(s: str) -> str:
    return re.sub(r"[ \t]+", " ", re.sub(r"\s+\n", "\n", s)).strip()


def domain_from_url(url: str) -> str:
    m = re.match(r"^https?://([^/]+)/?", url.strip(), re.I)
    return (m.group(1).lower() if m else "").replace("www.", "")


def employer_for_domain(dom: str) -> str:
    if dom in EMPLOYER_MAP:
        return EMPLOYER_MAP[dom]
    for k, v in EMPLOYER_MAP.items():
        if dom.endswith(k):
            return v
    return dom


def is_likely_job_link(href: str) -> bool:
    h = href.lower()
    if h.startswith("mailto:") or h.startswith("javascript:"):
        return False
    if PDF_HINT in h:
        return True
    if any(x in h for x in ATS_HINTS):
        return True
    return any(x in h for x in JOB_LINK_HINTS)


def canonicalize_url(base: str, href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        d = re.match(r"^(https?://[^/]+)", base, re.I)
        return (d.group(1) + href) if d else base.rstrip("/") + href
    if base.endswith("/"):
        return base + href
    return base.rsplit("/", 1)[0] + "/" + href


def extract_text_from_html(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "lxml")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    text = soup.get_text("\n")
    text = normalize_space(text)
    return text[:40_000]


def fetch_pdf_text(url: str) -> str:
    r = requests.get(url, timeout=45, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    tmp = "/tmp/doc.pdf"
    with open(tmp, "wb") as f:
        f.write(r.content)
    text = pdf_extract_text(tmp) or ""
    return normalize_space(text)[:40_000]


def openai_extract_job(source_url: str, raw_text: str, employer: str) -> Dict:
    schema_instructions = f"""
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
  - If location is not specified anywhere, set "Not specified".
- remote: true only if explicitly stated; otherwise false.
- apply_url: use a clear apply link if present; otherwise use source_url.
- salary_line:
  - If salary/pay is explicitly stated, format like:
    Pay: $X–$Y/hr (from posting)
    Pay: $X–$Y/yr (from posting)
  - Otherwise return "" (empty string).
- description: clean plain text, keep bullets; no HTML.
"""

    payload = {
        "model": "gpt-4o-mini",
        "input": [
            {"role": "system", "content": "Extract structured job fields for a helicopter job board. Be precise; do not guess."},
            {"role": "user", "content": f"source_url: {source_url}\n\n{schema_instructions}\n\nJOB PAGE TEXT:\n{raw_text}"}
        ],
        "temperature": 0.0,
    }

    r = requests.post(
        "https://api.openai.com/v1/responses",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=90,
    )
    r.raise_for_status()
    data = r.json()

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
        r2 = requests.post(
            "https://api.openai.com/v1/responses",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=120,
        )
        r2.raise_for_status()
        data2 = r2.json()
        text_out2 = ""
        for item in data2.get("output", []):
            for c in item.get("content", []):
                if c.get("type") == "output_text":
                    text_out2 += c.get("text", "")
        job = json.loads(text_out2.strip())

    job["employer"] = employer
    job["source_url"] = source_url
    if job.get("category") not in CATEGORY_ENUM:
        job["category"] = "Other"
    if not isinstance(job.get("remote"), bool):
        job["remote"] = False

    for k in ["title", "location", "apply_url", "description", "salary_line"]:
        job[k] = str(job.get(k, "")).strip()

    if not job["apply_url"]:
        job["apply_url"] = source_url

    return job


def rss_escape(s: str) -> str:
    return html.escape(s or "", quote=True)


def rfc2822_now() -> str:
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def collect_job_links_from_page(base_url: str, html_content: str) -> List[str]:
    soup = BeautifulSoup(html_content, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href:
            continue
        full = canonicalize_url(base_url, href)
        if is_likely_job_link(full):
            links.append(full)

    uniq, seen = [], set()
    for l in links:
        if l not in seen:
            seen.add(l)
            uniq.append(l)
    return uniq


def dedupe_by_source_url(jobs: List[Dict]) -> List[Dict]:
    out, seen = [], set()
    for j in jobs:
        key = (j.get("source_url") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(j)
    return out


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
        title = rss_escape(j["title"])
        employer = rss_escape(j["employer"])
        link = rss_escape(j["apply_url"])
        guid = rss_escape(j["source_url"])  # stable external ID
        category = rss_escape(j["category"])
        location = rss_escape(j["location"] or "Not specified")
        remote = "true" if j.get("remote") else "false"

        desc = j["description"]
        if j.get("salary_line"):
            desc = f"{j['salary_line']}\n\n{desc}"

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
        out.append(desc.strip())
        out.append("]]></description>")
        out.append("    </item>")

    out.append("  </channel>")
    out.append("</rss>")
    return "\n".join(out)


def main():
    sources = read_sources()
    if not sources:
        raise SystemExit("sources.txt is empty.")

    jobs: List[Dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        for src in sources:
            dom = domain_from_url(src)
            employer = employer_for_domain(dom)

            print(f"\nSOURCE: {src}\nEmployer: {employer}")

            try:
                page.goto(src, timeout=BROWSER_TIMEOUT_MS, wait_until="networkidle")
                time.sleep(1.0)
                listing_html = page.content()
            except Exception as e:
                print("  Failed to load source page:", e)
                continue

            links = collect_job_links_from_page(src, listing_html)
            if len(links) > MAX_JOB_LINKS_PER_SOURCE:
                links = links[:MAX_JOB_LINKS_PER_SOURCE]
            print(f"  Found {len(links)} candidate links")

            for job_url in links:
                try:
                    if job_url.lower().endswith(".pdf"):
                        raw_text = fetch_pdf_text(job_url)
                        source_url = job_url
                    else:
                        page.goto(job_url, timeout=BROWSER_TIMEOUT_MS, wait_until="networkidle")
                        time.sleep(0.6)
                        raw_text = extract_text_from_html(page.content())
                        source_url = job_url

                    if len(raw_text) < 200:
                        continue

                    job = openai_extract_job(source_url=source_url, raw_text=raw_text, employer=employer)
                    jobs.append(job)
                    print(f"   + {job.get('title','(no title)')[:90]}")
                except Exception as e:
                    print("   - Failed:", job_url, "err:", str(e)[:160])
                    continue

        browser.close()

    jobs = dedupe_by_source_url(jobs)

    xml = build_feed(jobs)
    with open(OUT_XML, "w", encoding="utf-8") as f:
        f.write(xml)

    print(f"\nWrote {OUT_XML} with {len(jobs)} items")


if __name__ == "__main__":
    main()
