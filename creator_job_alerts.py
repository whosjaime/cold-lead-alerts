import asyncio
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

STATE_FILE = Path("seen_jobs.json")

YTJOBS_WEBHOOK_URL = os.getenv("YTJOBS_WEBHOOK_URL", "")
ROSTER_WEBHOOK_URL = os.getenv("ROSTER_WEBHOOK_URL", "")

YTJOBS_URL = "https://ytjobs.co/job/search"
ROSTER_URL = "https://app.joinroster.co/jobs"


def load_seen() -> set[str]:
    if not STATE_FILE.exists():
        return set()
    try:
        return set(json.loads(STATE_FILE.read_text()))
    except Exception:
        return set()


def save_seen(items: set[str]) -> None:
    STATE_FILE.write_text(json.dumps(sorted(items), indent=2))


def make_id(*parts: str) -> str:
    base = " | ".join(parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def clean_text(text: Optional[str]) -> str:
    return " ".join((text or "").split())


def get_webhook_url(source: str) -> str:
    if source == "YTJobs":
        return YTJOBS_WEBHOOK_URL
    if source == "Roster":
        return ROSTER_WEBHOOK_URL
    return ""


def clean_job_title(title: str) -> str:
    title = clean_text(title)

    # Remove obvious junk after separators
    for sep in ["+", "|", "•"]:
        if sep in title:
            title = title.split(sep)[0].strip()

    # Remove pay fragments from title if they bled in
    title = re.sub(r"\$\d[\d,]*(?:\s*-\s*\$\d[\d,]*)?.*", "", title).strip()

    # Keep only the actual role/title part
    return title[:100] or "New job"


def extract_pay(text: str) -> str:
    text = clean_text(text)

    patterns = [
        r"(\$\d[\d,]*(?:\s*-\s*\$\d[\d,]*)?\s*(?:per hour|/hour|hourly))",
        r"(\$\d[\d,]*(?:\s*-\s*\$\d[\d,]*)?\s*(?:per project|/project))",
        r"(\$\d[\d,]*(?:\s*-\s*\$\d[\d,]*)?)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return clean_text(match.group(1))

    return "Not listed"


def extract_job_type(text: str) -> str:
    text_lower = clean_text(text).lower()

    if "part-time" in text_lower or "part time" in text_lower:
        return "Part-time"
    if "full-time" in text_lower or "full time" in text_lower:
        return "Full-time"
    if "contract" in text_lower:
        return "Contract"
    if "freelance" in text_lower:
        return "Freelance"
    if "intern" in text_lower:
        return "Internship"
    if "project-based" in text_lower or "per project" in text_lower:
        return "Project-based"

    return "Not listed"


def extract_location(text: str) -> str:
    text_lower = clean_text(text).lower()

    if "remote" in text_lower:
        return "Remote"
    if "hybrid" in text_lower:
        return "Hybrid"
    if "on-site" in text_lower or "onsite" in text_lower:
        return "On-site"

    return "Not listed"


def extract_creator(text: str) -> str:
    text = clean_text(text)

    # Remove apply now prefixes if present
    text = re.sub(r"^apply now\s*\|\s*", "", text, flags=re.IGNORECASE).strip()

    # Try to split off after title/pay/type/location
    separators = ["Remote", "Hybrid", "On-site", "Onsite"]
    for sep in separators:
        if sep in text:
            parts = text.split(sep, 1)
            tail = clean_text(parts[1])
            if tail:
                tail = re.sub(r"\+\d+\s*more.*", "", tail, flags=re.IGNORECASE).strip()
                tail = re.sub(r"\d+(\.\d+)?[KMB]?\s*subs?", "", tail, flags=re.IGNORECASE).strip()
                return tail[:80] or "Not listed"

    return "Not listed"


def extract_description(title: str, text: str) -> str:
    description = clean_text(text)

    # Remove title from description if duplicated
    if title:
        description = description.replace(title, "").strip()

    # Remove creator line fragments and obvious junk
    description = re.sub(r"^apply now\s*\|\s*", "", description, flags=re.IGNORECASE).strip()
    description = re.sub(r"\+\d+\s*more.*", "", description, flags=re.IGNORECASE).strip()
    description = re.sub(r"\d+(\.\d+)?[KMB]?\s*subs?", "", description, flags=re.IGNORECASE).strip()

    # Remove repeated pay/location/type chunks from front if they dominate
    description = re.sub(
        r"^\$?\d[\d,]*(?:\s*-\s*\$?\d[\d,]*)?\s*(?:per hour|/hour|hourly|per project|/project)?",
        "",
        description,
        flags=re.IGNORECASE
    ).strip(" |,-")

    if not description:
        return "No description listed."

    return description[:280]


def send_to_discord(job: Dict[str, Any]) -> None:
    source = job.get("source", "Unknown")
    webhook_url = get_webhook_url(source)

    if not webhook_url:
        raise RuntimeError(f"Missing webhook URL for source: {source}")

    title = clean_text(job.get("title") or "New job")
    creator = clean_text(job.get("creator") or "Not listed")
    location = clean_text(job.get("location") or "Not listed")
    job_type = clean_text(job.get("job_type") or "Not listed")
    pay = clean_text(job.get("pay") or "Not listed")
    description = clean_text(job.get("summary") or "No description listed.")
    url = clean_text(job.get("url") or "")

    content = (
        f"🔥 **Cold lead spotted. Time to warm it up.**\n\n"
        f"**Job:** {title}\n"
        f"<{url}>\n\n"
        f"**Type:** {job_type}\n"
        f"**Location:** {location}\n"
        f"**Pay:** {pay}\n"
        f"**Creator / Poster:** {creator}\n"
        f"**Description:** {description}"
    )

    payload = {
        "username": "Manifest Media Leads",
        "content": content,
        "allowed_mentions": {"parse": []},
    }

    response = requests.post(webhook_url, json=payload, timeout=30)
    response.raise_for_status()


def dedupe_jobs(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen_ids = set()
    cleaned: List[Dict[str, Any]] = []
    for job in jobs:
        if job["id"] in seen_ids:
            continue
        seen_ids.add(job["id"])
        cleaned.append(job)
    return cleaned


async def scrape_ytjobs(page) -> List[Dict[str, Any]]:
    await page.goto(YTJOBS_URL, wait_until="networkidle")
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    jobs: List[Dict[str, Any]] = []

    for a in soup.select('a[href*="/job/"]'):
        href = a.get("href") or ""
        if "/job/search" in href:
            continue

        raw_title = clean_text(a.get_text(" ", strip=True))
        if not raw_title:
            continue

        card = a.parent
        context = clean_text(card.get_text(" ", strip=True) if card else raw_title)
        full_url = href if href.startswith("http") else f"https://ytjobs.co{href}"

        title = clean_job_title(raw_title)
        pay = extract_pay(context)
        job_type = extract_job_type(context)
        location = extract_location(context)
        creator = extract_creator(context)
        summary = extract_description(title, context)

        jobs.append(
            {
                "id": make_id("ytjobs", title, full_url),
                "title": title,
                "creator": creator,
                "summary": summary,
                "location": location,
                "job_type": job_type,
                "pay": pay,
                "url": full_url,
                "source": "YTJobs",
            }
        )

    print(f"YTJobs found: {len(jobs)}")
    return dedupe_jobs(jobs)


async def scrape_roster(page) -> List[Dict[str, Any]]:
    await page.goto(ROSTER_URL, wait_until="networkidle")
    await page.wait_for_timeout(8000)

    links = await page.eval_on_selector_all(
        "a",
        """elements => elements.map(a => ({
            href: a.href || "",
            text: (a.innerText || "").trim()
        }))"""
    )

    jobs: List[Dict[str, Any]] = []

    for item in links:
        href = item.get("href", "")
        text = clean_text(item.get("text", ""))

        if not href or not text:
            continue
        if "job" not in href.lower():
            continue

        title = clean_job_title(text)
        pay = extract_pay(text)
        job_type = extract_job_type(text)
        location = extract_location(text)
        creator = "Not listed"
        summary = extract_description(title, text)

        jobs.append(
            {
                "id": make_id("roster", title, href),
                "title": title,
                "creator": creator,
                "summary": summary,
                "location": location,
                "job_type": job_type,
                "pay": pay,
                "url": href,
                "source": "Roster",
            }
        )

    print(f"Roster jobs found: {len(jobs)}")
    return dedupe_jobs(jobs)


async def fetch_jobs() -> List[Dict[str, Any]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        jobs: List[Dict[str, Any]] = []

        try:
            jobs.extend(await scrape_ytjobs(page))
        except Exception as e:
            print(f"YTJobs scrape failed: {e}")

        try:
            jobs.extend(await scrape_roster(page))
        except Exception as e:
            print(f"Roster scrape failed: {e}")

        await browser.close()
        return jobs


async def main() -> None:
    seen = load_seen()
    jobs = await fetch_jobs()

    new_count = 0
    for job in jobs:
        if job["id"] in seen:
            continue

        try:
            send_to_discord(job)
            seen.add(job["id"])
            new_count += 1
            print(f"Posted: {job['title']} ({job['source']})")
        except Exception as e:
            print(f"Error sending job: {e}")

    save_seen(seen)
    print(f"Done. Sent {new_count} new jobs.")


if __name__ == "__main__":
    asyncio.run(main())
