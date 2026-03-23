import asyncio
import hashlib
import json
import os
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


def clip(text: str, max_len: int) -> str:
    text = clean_text(text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "..."


def send_to_discord(job: Dict[str, Any]) -> None:
    source = job.get("source", "Unknown")
    webhook_url = get_webhook_url(source)

    if not webhook_url:
        raise RuntimeError(f"Missing webhook URL for source: {source}")

    title = clip(job.get("title", "New job"), 180)
    company = clip(job.get("company", "Not listed"), 120)
    creator = clip(job.get("creator", "Not listed"), 120)
    location = clip(job.get("location", "Not listed"), 120)
    job_type = clip(job.get("job_type", "Not listed"), 120)
    pay = clip(job.get("pay", "Not listed"), 120)
    description = clip(job.get("summary", "No description listed."), 700)
    url = (job.get("url") or "").strip()

    if url:
        title_line = f"**Job Title:** [{title}]({url})"
    else:
        title_line = f"**Job Title:** {title}"

    content = (
        f"{title_line}\n"
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
        "content": content,
        "allowed_mentions": {"parse": []},
    }

    if WEBHOOK_AVATAR_URL:
        payload["avatar_url"] = WEBHOOK_AVATAR_URL

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


def parse_ytjobs_card_text(text: str) -> Dict[str, str]:
    text = clean_text(text)

    result = {
        "company": "Not listed",
        "creator": "Not listed",
        "location": "Not listed",
        "job_type": "Not listed",
        "pay": "Not listed",
        "summary": text[:700] if text else "No description listed.",
    }

    lower = text.lower()

    if "remote" in lower:
        result["location"] = "Remote"
    elif "hybrid" in lower:
        result["location"] = "Hybrid"
    elif "on-site" in lower or "onsite" in lower:
        result["location"] = "On-site"

    if "part-time" in lower:
        result["job_type"] = "Part-time"
    elif "full-time" in lower:
        result["job_type"] = "Full-time"
    elif "contract" in lower:
        result["job_type"] = "Contract"
    elif "freelance" in lower:
        result["job_type"] = "Freelance"

    tokens = text.split()
    pay_tokens = []
    capture = False
    for token in tokens:
        if "$" in token:
            capture = True
        if capture:
            pay_tokens.append(token)
            if len(pay_tokens) >= 8:
                break
    if pay_tokens:
        result["pay"] = clean_text(" ".join(pay_tokens))

    return result


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
        if not title:
            continue

        full_url = href if href.startswith("http") else f"https://ytjobs.co{href}"

        card = a.parent
        context = clean_text(card.get_text(" ", strip=True) if card else title)
        parsed = parse_ytjobs_card_text(context)

        jobs.append(
            {
                "id": make_id("ytjobs", title, full_url),
                "title": title,
                "company": parsed["company"],
                "creator": parsed["creator"],
                "summary": parsed["summary"],
                "location": parsed["location"],
                "job_type": parsed["job_type"],
                "pay": parsed["pay"],
                "url": full_url,
                "source": "YTJobs",
            }
        )

    jobs = dedupe_jobs(jobs)
    print(f"YTJobs found: {len(jobs)}")
    return jobs


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
                "title": text,
                "company": "Not listed",
                "creator": "Not listed",
                "summary": text,
                "location": "Not listed",
                "job_type": "Not listed",
                "pay": "Not listed",
                "url": href,
                "source": "Roster",
            }
        )

    jobs = dedupe_jobs(jobs)
    print(f"Roster jobs found: {len(jobs)}")
    return jobs


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
