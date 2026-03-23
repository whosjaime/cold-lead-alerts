import asyncio
import hashlib
import json
import os
from datetime import datetime, timezone
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
ALERT_HEADER = os.getenv("ALERT_HEADER", "Cold lead spotted. Time to warm it up.")


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

    title = job.get("title", "New job")
    company = job.get("company", "Unknown creator/company")
    url = job.get("url", "")
    location = job.get("location", "Not listed")
    job_type = job.get("job_type", "Not listed")
    summary = (job.get("summary", "") or "")[:3000]

    payload = {
        "username": "Cold Lead Alerts",
        "content": ALERT_HEADER,
        "embeds": [
            {
                "title": f"{company} is hiring: {title}",
                "url": url,
                "description": summary or "A new creator-economy job was found.",
                "fields": [
                    {"name": "Source", "value": source, "inline": True},
                    {"name": "Location", "value": location, "inline": True},
                    {"name": "Type", "value": job_type, "inline": True},
                ],
                "footer": {"text": "Cold lead tracker"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ],
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
                "title": title,
                "company": "",
                "summary": context[:1000],
                "location": "",
                "job_type": "",
                "url": full_url,
                "source": "YTJobs",
            }
        )

    return dedupe_jobs(jobs)


async def scrape_roster(page) -> List[Dict[str, Any]]:
    await page.goto(ROSTER_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(5000)
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    jobs: List[Dict[str, Any]] = []

    for a in soup.select("a"):
        href = a.get("href") or ""
        text = clean_text(a.get_text(" ", strip=True))

        if not href:
            continue
        if "job" not in href.lower() and "apply" not in text.lower():
            continue

        full_url = href if href.startswith("http") else f"https://app.joinroster.co{href}"
        title = text or "Roster Job"
        card = a.parent
        context = clean_text(card.get_text(" ", strip=True) if card else title)

        jobs.append(
            {
                "id": make_id("roster", title, full_url),
                "title": title,
                "company": "",
                "summary": context[:1000],
                "location": "",
                "job_type": "",
                "url": full_url,
                "source": "Roster",
            }
        )

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
            print(f"Discord send failed for {job.get('title')}: {e}")

    save_seen(seen)
    print(f"Done. Sent {new_count} new jobs.")


if __name__ == "__main__":
    asyncio.run(main())
