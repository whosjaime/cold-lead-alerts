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

HEADER_TEXT = "Cold leads, warm them up! 🔥"


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
        r"\bApply\b",
        r"\bsubs\b",
        r"\bfollowers\b",
    ]

    for marker in split_markers:
        match = re.search(marker, text, flags=re.IGNORECASE)
        if match:
            text = text[: match.start()].strip()
            break

    for sep in [" | ", " - ", " — ", " +", " / ", ":"]:
        if sep in text:
            text = text.split(sep)[0].strip()

    words = text.split()
    if len(words) > 6:
        text = " ".join(words[:6])

    return clip(text, 60) if text else "New Job"


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
    async def load_roster_list() -> str:
        await page.goto(ROSTER_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)

        for _ in range(4):
            await page.mouse.wheel(0, 3000)
            await page.wait_for_timeout(1200)

        html = await page.content()
        Path("roster_debug.html").write_text(html, encoding="utf-8")
        body = await page.locator("body").inner_text()
        return body

    def parse_roster_block(block: str, idx: int) -> Dict[str, str]:
        block = clean_text(block)

        title = block
        for marker in ["📍", "💼", "👥", "$", "Apply"]:
            if marker in title:
                title = title.split(marker)[0].strip()

        title = clip(title, 60)
        if not title:
            title = f"Roster Job {idx + 1}"

        location = extract_location(block)
        job_type = extract_job_type(block)
        pay = extract_pay(block)

        description = block
        if description.lower().startswith(title.lower()):
            description = description[len(title):].strip(" -—:|")

        for marker in ["📍", "💼", "👥"]:
            description = description.replace(marker, " ")

        description = clean_text(description)
        description = clip(description, 180)

        if not description:
            description = "No description listed."

        return {
            "title": title,
            "summary": description,
            "location": location,
            "job_type": job_type,
            "pay": pay,
        }

    body_text = await load_roster_list()
    Path("roster_debug.txt").write_text(body_text, encoding="utf-8")

    print(f"Roster body preview: {clip(body_text, 1200)}")

    raw_blocks = [clean_text(x) for x in body_text.split("Apply")]
    raw_blocks = [x for x in raw_blocks if x and len(x) > 30]

    print(f"Roster raw blocks: {len(raw_blocks)}")

    apply_count = await page.locator("text=Apply").count()
    print(f"Roster apply buttons: {apply_count}")

    jobs: List[Dict[str, Any]] = []
    total = min(len(raw_blocks), apply_count)

    for i in range(total):
        await load_roster_list()

        apply_locator = page.locator("text=Apply").nth(i)
        detail_url = ROSTER_URL

        try:
            await apply_locator.scroll_into_view_if_needed()
            await page.wait_for_timeout(500)

            before_url = page.url

            try:
                async with page.expect_navigation(wait_until="networkidle", timeout=10000):
                    await apply_locator.click()
            except Exception:
                await apply_locator.click(force=True)
                await page.wait_for_timeout(3000)

            after_url = page.url
            if after_url and after_url != before_url:
                detail_url = after_url
        except Exception as e:
            print(f"Roster click failed for block {i}: {e}")

        block = raw_blocks[i]
        parsed = parse_roster_block(block, i)

        jobs.append(
            {
                "id": make_id("roster", parsed["title"], detail_url),
                "title": parsed["title"],
                "summary": parsed["summary"],
                "location": parsed["location"],
                "job_type": parsed["job_type"],
                "pay": parsed["pay"],
                "url": detail_url,
                "source": "Roster",
            }
        )

    jobs = dedupe_jobs(jobs)
    print(f"Roster jobs found: {len(jobs)}")
    for job in jobs[:10]:
        print(f"Roster parsed job: {job['title']} | {job['pay']} | {job['location']} | {job['url']}")

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
            await asyncio.sleep(60)
        except Exception as e:
            print(f"Discord send failed for {job.get('title')}: {e}")

    save_seen(seen)
    print(f"Done. Sent {new_count} new jobs.")


if __name__ == "__main__":
    asyncio.run(main())
