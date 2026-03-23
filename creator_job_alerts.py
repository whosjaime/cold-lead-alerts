import asyncio
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

STATE_FILE = Path("seen_jobs.json")

YTJOBS_WEBHOOK_URL = os.getenv("YTJOBS_WEBHOOK_URL", "")
ROSTER_WEBHOOK_URL = os.getenv("ROSTER_WEBHOOK_URL", "")
WEBHOOK_AVATAR_URL = os.getenv("WEBHOOK_AVATAR_URL", "")

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


def send_to_discord(job: Dict[str, Any]) -> None:
    source = job.get("source", "Unknown")
    webhook_url = get_webhook_url(source)

    if not webhook_url:
        raise RuntimeError(f"Missing webhook URL for source: {source}")

    title = clean_text(job.get("title") or "New job")
    company = clean_text(job.get("company") or "Not listed")
    creator = clean_text(job.get("creator") or "Not listed")
    location = clean_text(job.get("location") or "Not listed")
    job_type = clean_text(job.get("job_type") or "Not listed")
    pay = clean_text(job.get("pay") or "Not listed")
    description = clean_text(job.get("summary") or "No description listed.")
    url = job.get("url", "")

    if len(description) > 350:
        description = description[:347] + "..."

    content = (
        f"**Job Title:** {title}\n"
        f"{url}\n\n"
        f"**Source:** {source}\n"
        f"**Type:** {job_type}\n"
        f"**Location:** {location}\n"
        f"**Pay:** {pay}\n"
        f"**Creator / Poster:** {creator}\n"
        f"**Company:** {company}\n"
        f"**Description:** {description}"
    )

    payload = {
        "username": "Manifest Media Leads",
        "avatar_url": WEBHOOK_AVATAR_URL,
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

        title = clean_text(a.get_text(" ", strip=True))
        card = a.parent
        context = clean_text(card.get_text(" ", strip=True) if card else title)
        full_url = href if href.startswith("http") else f"https://ytjobs.co{href}"

        if not title:
            continue

        jobs.append(
            {
                "id": make_id("ytjobs", title, full_url),
                "title": title[:120],
                "company": "",
                "creator": "",
                "summary": context[:350],
                "location": "Not listed",
                "job_type": "Not listed",
                "pay": "Not listed",
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

        if not href:
            continue
        if "job" not in href.lower():
            continue
        if not text:
            continue

        jobs.append(
            {
                "id": make_id("roster", text, href),
                "title": text[:120],
                "company": "",
                "creator": "",
                "summary": text[:350],
                "location": "Not listed",
                "job_type": "Not listed",
                "pay": "Not listed",
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
