import asyncio
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

STATE_FILE = Path("seen_jobs.json")

YTJOBS_WEBHOOK_URL = os.getenv("YTJOBS_WEBHOOK_URL", "")
ROSTER_WEBHOOK_URL = os.getenv("ROSTER_WEBHOOK_URL", "")

YTJOBS_URL = "https://ytjobs.co/job/search"
ROSTER_URL = "https://www.joinroster.co/jobs"

POST_DELAY_SECONDS = int(os.getenv("POST_DELAY_SECONDS", "10"))


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

    for sep in ["+", "|", "•"]:
        if sep in title:
            title = title.split(sep)[0].strip()

    title = re.sub(r"\$\d[\d,]*(?:\s*-\s*\$\d[\d,]*)?.*", "", title).strip()
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


def extract_email(text: str) -> str:
    matches = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")
    if matches:
        return matches[0]
    return "Not listed"


def extract_creator(text: str) -> str:
    text = clean_text(text)
    text = re.sub(r"^apply now\s*\|\s*", "", text, flags=re.IGNORECASE).strip()

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

    if title:
        description = description.replace(title, "").strip()

    description = re.sub(r"^apply now\s*\|\s*", "", description, flags=re.IGNORECASE).strip()
    description = re.sub(r"\+\d+\s*more.*", "", description, flags=re.IGNORECASE).strip()
    description = re.sub(r"\d+(\.\d+)?[KMB]?\s*subs?", "", description, flags=re.IGNORECASE).strip()

    description = re.sub(
        r"^\$?\d[\d,]*(?:\s*-\s*\$?\d[\d,]*)?\s*(?:per hour|/hour|hourly|per project|/project)?",
        "",
        description,
        flags=re.IGNORECASE
    ).strip(" |,-")

    if not description:
        return "No description listed."

    return description[:400]


def choose_best_link(links: List[str], domains: List[str]) -> str:
    for link in links:
        lower = link.lower()
        if any(domain in lower for domain in domains):
            return link
    return "Not listed"


async def scrape_job_detail(page, url: str) -> Dict[str, str]:
    try:
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(2000)

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        page_text = clean_text(soup.get_text(" ", strip=True))

        all_links = []
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            full_href = urljoin(url, href)
            if full_href.startswith("http"):
                all_links.append(full_href)

        email = extract_email(page_text)

        youtube_link = choose_best_link(all_links, ["youtube.com", "youtu.be"])
        website_link = "Not listed"

        for link in all_links:
            lower = link.lower()
            if all(domain not in lower for domain in [
                "youtube.com", "youtu.be", "instagram.com",
                "x.com", "twitter.com", "linkedin.com",
                "discord.com", "ytjobs.co", "joinroster.co", "app.joinroster.co"
            ]):
                website_link = link
                break

        description = "No description listed."
        selectors = [
            "main",
            "article",
            "[class*='description']",
            "[class*='content']",
            "[class*='job']",
        ]

        for selector in selectors:
            node = soup.select_one(selector)
            if node:
                candidate = clean_text(node.get_text(" ", strip=True))
                if len(candidate) > 80:
                    description = candidate[:500]
                    break

        return {
            "email": email,
            "youtube_link": youtube_link,
            "website_link": website_link,
            "detail_description": description,
        }
    except Exception as e:
        print(f"Detail scrape failed for {url}: {e}")
        return {
            "email": "Not listed",
            "youtube_link": "Not listed",
            "website_link": "Not listed",
            "detail_description": "No description listed.",
        }


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
    email = clean_text(job.get("email") or "Not listed")
    youtube_link = clean_text(job.get("youtube_link") or "Not listed")
    website_link = clean_text(job.get("website_link") or "Not listed")

    extra_lines = []

    if email != "Not listed":
        extra_lines.append(f"**Email:** {email}")
    if youtube_link != "Not listed":
        extra_lines.append(f"**YouTube:** <{youtube_link}>")
    if website_link != "Not listed":
        extra_lines.append(f"**Website:** <{website_link}>")

    extras = "\n".join(extra_lines)
    if extras:
        extras = f"{extras}\n"

    content = (
        f"🔥 **Cold lead spotted. Time to warm it up.**\n\n"
        f"**Job:** {title}\n"
        f"<{url}>\n\n"
        f"**Type:** {job_type}\n"
        f"**Location:** {location}\n"
        f"**Pay:** {pay}\n"
        f"**Creator / Poster:** {creator}\n"
        f"{extras}"
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
    await page.goto(ROSTER_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(12000)

    jobs: List[Dict[str, Any]] = []

    # Try to grab rendered anchors first
    links = await page.eval_on_selector_all(
        "a",
        """elements => elements.map(a => ({
            href: a.href || "",
            text: (a.innerText || a.textContent || "").trim()
        }))"""
    )

    for item in links:
        href = clean_text(item.get("href", ""))
        text = clean_text(item.get("text", ""))

        if not href or "/jobs/" not in href:
            continue
        if href.rstrip("/") == ROSTER_URL.rstrip("/"):
            continue
        if "details" not in href:
            continue

        title = clean_job_title(text) if text else "Roster Job"
        if title.lower() in ["create free account →", "log in", "view full job description"]:
            continue

        jobs.append(
            {
                "id": make_id("roster", title, href),
                "title": title,
                "creator": "Not listed",
                "summary": "No description listed.",
                "location": "Not listed",
                "job_type": "Not listed",
                "pay": "Not listed",
                "url": href,
                "source": "Roster",
            }
        )

    # Fallback: scrape URLs directly from rendered HTML if anchors are sparse
    if not jobs:
        html = await page.content()
        urls = set(re.findall(r'https://www\.joinroster\.co/jobs/[a-f0-9\-]+/details', html))
        urls.update(re.findall(r'https://app\.joinroster\.co/jobs/[a-f0-9\-]+/details', html))

        for href in urls:
            jobs.append(
                {
                    "id": make_id("roster", href),
                    "title": "Roster Job",
                    "creator": "Not listed",
                    "summary": "No description listed.",
                    "location": "Not listed",
                    "job_type": "Not listed",
                    "pay": "Not listed",
                    "url": href,
                    "source": "Roster",
                }
            )

    print(f"Roster jobs found: {len(jobs)}")
    return dedupe_jobs(jobs)


async def enrich_jobs_with_detail(page, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched = []
    for job in jobs:
        detail = await scrape_job_detail(page, job["url"])

        if detail.get("email") and detail["email"] != "Not listed":
            job["email"] = detail["email"]

        if detail.get("youtube_link") and detail["youtube_link"] != "Not listed":
            job["youtube_link"] = detail["youtube_link"]

        if detail.get("website_link") and detail["website_link"] != "Not listed":
            job["website_link"] = detail["website_link"]

        detail_description = detail.get("detail_description", "").strip()
        if detail_description and detail_description != "No description listed.":
            job["summary"] = detail_description[:400]

        # Second-pass extraction from detail text
        detail_text = detail_description if detail_description != "No description listed." else job.get("summary", "")
        if job.get("pay", "Not listed") == "Not listed":
            job["pay"] = extract_pay(detail_text)
        if job.get("job_type", "Not listed") == "Not listed":
            job["job_type"] = extract_job_type(detail_text)
        if job.get("location", "Not listed") == "Not listed":
            job["location"] = extract_location(detail_text)
        if job.get("creator", "Not listed") == "Not listed":
            job["creator"] = extract_creator(detail_text)

        enriched.append(job)

    return enriched


async def fetch_jobs() -> List[Dict[str, Any]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        list_page = await browser.new_page()
        detail_page = await browser.new_page()

        jobs: List[Dict[str, Any]] = []

        try:
            jobs.extend(await scrape_ytjobs(list_page))
        except Exception as e:
            print(f"YTJobs scrape failed: {e}")

        try:
            jobs.extend(await scrape_roster(list_page))
        except Exception as e:
            print(f"Roster scrape failed: {e}")

        try:
            jobs = await enrich_jobs_with_detail(detail_page, jobs)
        except Exception as e:
            print(f"Detail enrichment failed: {e}")

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

            if POST_DELAY_SECONDS > 0:
                print(f"Waiting {POST_DELAY_SECONDS} seconds before next post...")
                await asyncio.sleep(POST_DELAY_SECONDS)

        except Exception as e:
            print(f"Error sending job: {e}")

    save_seen(seen)
    print(f"Done. Sent {new_count} new jobs.")


if __name__ == "__main__":
    asyncio.run(main())
