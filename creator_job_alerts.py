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
WEBHOOK_AVATAR_URL = os.getenv("WEBHOOK_AVATAR_URL", "")

YTJOBS_URL = "https://ytjobs.co/job/search"
ROSTER_URL = "https://www.joinroster.co/jobs"

HEADER_TEXT = "Cold leads, warm them up!"


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


def clip(text: str, max_len: int) -> str:
    text = clean_text(text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "..."


def get_webhook_url(source: str) -> str:
    if source == "YTJobs":
        return YTJOBS_WEBHOOK_URL
    if source == "Roster":
        return ROSTER_WEBHOOK_URL
    return ""


def extract_role_only(text: str) -> str:
    text = clean_text(text)

    split_markers = [
        r"\$",
        r"\bRemote\b",
        r"\bHybrid\b",
        r"\bOn[- ]?site\b",
        r"\bPart[- ]?time\b",
        r"\bFull[- ]?time\b",
        r"\bContract\b",
        r"\bFreelance\b",
        r"\bPer project\b",
        r"\bPer hour\b",
        r"\bHour\b",
        r"\bApply\b",
        r"\bsubs\b",
    ]

    for marker in split_markers:
        match = re.search(marker, text, flags=re.IGNORECASE)
        if match:
            text = text[: match.start()].strip()
            break

    for sep in [" | ", " - ", " — ", " +", " / "]:
        if sep in text:
            text = text.split(sep)[0].strip()

    words = text.split()
    if len(words) > 8:
        text = " ".join(words[:8])

    return clip(text, 80) if text else "New Job"


def extract_pay(text: str) -> str:
    text = clean_text(text)
    pay_match = re.search(
        r"(\$\d[\d,]*(?:\s*-\s*\$\d[\d,]*)?(?:\s*(?:/|per)\s*(?:hour|hr|project|month|year))?)",
        text,
        flags=re.IGNORECASE,
    )
    if pay_match:
        return clip(pay_match.group(1), 80)
    return "Not listed"


def extract_location(text: str) -> str:
    lower = text.lower()
    if "remote" in lower:
        return "Remote"
    if "hybrid" in lower:
        return "Hybrid"
    if "on-site" in lower or "onsite" in lower:
        return "On-site"
    return "Not listed"


def extract_job_type(text: str) -> str:
    lower = text.lower()
    if "part-time" in lower or "part time" in lower:
        return "Part-time"
    if "full-time" in lower or "full time" in lower:
        return "Full-time"
    if "contract" in lower:
        return "Contract"
    if "freelance" in lower:
        return "Freelance"
    if "per project" in lower:
        return "Per project"
    return "Not listed"


def build_description(text: str, role: str) -> str:
    text = clean_text(text)
    if text.lower().startswith(role.lower()):
        return clip(text, 220)
    return clip(f"{role} — {text}", 220)


def send_to_discord(job: Dict[str, Any]) -> None:
    source = job.get("source", "Unknown")
    webhook_url = get_webhook_url(source)

    if not webhook_url:
        raise RuntimeError(f"Missing webhook URL for source: {source}")

    role = clip(job.get("title", "New job"), 100)
    location = clip(job.get("location", "Not listed"), 60)
    job_type = clip(job.get("job_type", "Not listed"), 60)
    pay = clip(job.get("pay", "Not listed"), 80)
    description = clip(job.get("summary", "No description listed."), 220)
    url = (job.get("url") or "").strip()

    content = (
        f"{HEADER_TEXT}\n\n"
        f"**Job Title:** {role}\n"
        f"**Source:** {source}\n"
        f"**Type:** {job_type}\n"
        f"**Location:** {location}\n"
        f"**Pay:** {pay}\n"
        f"**Description:** {description}\n"
        f"**Link:** {url if url else 'Not listed'}"
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


async def scrape_ytjobs(page) -> List[Dict[str, Any]]:
    await page.goto(YTJOBS_URL, wait_until="networkidle")
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    jobs: List[Dict[str, Any]] = []

    for a in soup.select('a[href*="/job/"]'):
        href = a.get("href") or ""
        if "/job/search" in href:
            continue

        full_url = href if href.startswith("http") else f"https://ytjobs.co{href}"

        card = a.parent
        context = clean_text(card.get_text(" ", strip=True) if card else a.get_text(" ", strip=True))
        if not context:
            continue

        role = extract_role_only(context)
        pay = extract_pay(context)
        location = extract_location(context)
        job_type = extract_job_type(context)
        description = build_description(context, role)

        jobs.append(
            {
                "id": make_id("ytjobs", role, full_url),
                "title": role,
                "summary": description,
                "location": location,
                "job_type": job_type,
                "pay": pay,
                "url": full_url,
                "source": "YTJobs",
            }
        )

    jobs = dedupe_jobs(jobs)
    print(f"YTJobs found: {len(jobs)}")
    return jobs


async def scrape_roster(page) -> List[Dict[str, Any]]:
    await page.goto(ROSTER_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(12000)

    # Try to trigger lazy-loaded content
    for _ in range(4):
        await page.mouse.wheel(0, 3000)
        await page.wait_for_timeout(1500)

    html = await page.content()
    Path("roster_debug.html").write_text(html, encoding="utf-8")

    soup = BeautifulSoup(html, "html.parser")

    # Debug: print potentially useful links
    hrefs = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/jobs/" in href.lower() or "apply" in href.lower():
            hrefs.append(href)

    print(f"Roster debug href count: {len(hrefs)}")
    for href in hrefs[:20]:
        print(f"Roster href: {href}")

    jobs: List[Dict[str, Any]] = []

    # Pass 1: direct job links
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = clean_text(a.get_text(" ", strip=True))

        if not href:
            continue
        if "/jobs/" not in href.lower():
            continue

        full_url = href if href.startswith("http") else f"https://www.joinroster.co{href}"
        context = clean_text(a.parent.get_text(" ", strip=True) if a.parent else text) or text

        role = extract_role_only(context)
        if not role or role == "New Job":
            continue

        jobs.append(
            {
                "id": make_id("roster", role, full_url),
                "title": role,
                "summary": clip(context, 220),
                "location": extract_location(context),
                "job_type": extract_job_type(context),
                "pay": extract_pay(context),
                "url": full_url,
                "source": "Roster",
            }
        )

    # Pass 2: look for apply buttons/cards if direct links are hidden
    if not jobs:
        texts = await page.locator("body").inner_text()
        Path("roster_debug.txt").write_text(texts, encoding="utf-8")
        print("Roster page text snapshot saved to roster_debug.txt")
        print(f"Roster body text preview: {clip(texts, 800)}")

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
