import asyncio
import hashlib
import json
import os
import re
from datetime import datetime, timezone
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

# 65 sec so posts do not feel instant/back-to-back
POST_DELAY_SECONDS = int(os.getenv("POST_DELAY_SECONDS", "65"))


def log(message: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    print(f"[{now}] {message}")


def load_seen() -> set[str]:
    if not STATE_FILE.exists():
        return set()
    try:
        return set(json.loads(STATE_FILE.read_text(encoding="utf-8")))
    except Exception:
        return set()


def save_seen(items: set[str]) -> None:
    STATE_FILE.write_text(json.dumps(sorted(items), indent=2), encoding="utf-8")


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
    title = re.sub(r"\s{2,}", " ", title).strip()

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


def strip_noise(text: str) -> str:
    text = clean_text(text)

    noise_patterns = [
        r"Post a Job",
        r"Join as Talent",
        r"Talent Jobs",
        r"Talent",
        r"How It Works",
        r"Login",
        r"Log in",
        r"Sign up",
        r"Posted on:",
        r"Starts ASAP",
    ]

    for pattern in noise_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    text = re.sub(r"\s{2,}", " ", text).strip()
    return text


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
        flags=re.IGNORECASE,
    ).strip(" |,-")

    description = strip_noise(description)

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
        log(f"[DETAIL] Visiting: {url}")
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)

        html = await page.content()
        log(f"[DETAIL] HTML length: {len(html)}")

        soup = BeautifulSoup(html, "html.parser")

        for tag in soup.select("script, style, noscript, svg, header, footer, nav, aside"):
            tag.decompose()

        page_text = clean_text(soup.get_text(" ", strip=True))
        page_text = strip_noise(page_text)

        all_links: List[str] = []
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
                "youtube.com",
                "youtu.be",
                "instagram.com",
                "x.com",
                "twitter.com",
                "linkedin.com",
                "discord.com",
                "ytjobs.co",
                "joinroster.co",
                "app.joinroster.co",
                "t.me/",
                "telegram.me/",
            ]):
                website_link = link
                break

        description = "No description listed."
        selectors = [
            "[class*='description']",
            "[class*='details']",
            "[class*='content']",
            "article",
            "main",
        ]

        for selector in selectors:
            node = soup.select_one(selector)
            if not node:
                continue

            candidate = clean_text(node.get_text(" ", strip=True))
            candidate = strip_noise(candidate)

            if len(candidate) > 80:
                description = candidate[:500]
                log(f"[DETAIL] Description found using selector: {selector}")
                break

        return {
            "email": email,
            "youtube_link": youtube_link,
            "website_link": website_link,
            "detail_description": description,
        }

    except Exception as e:
        log(f"Detail scrape failed for {url}: {e}")
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

    if not url.startswith("http"):
        log(f"[DISCORD] Invalid or missing URL for job: {title} | url={url!r}")
        url = "Not listed"

    extra_lines = []

    if email != "Not listed":
        extra_lines.append(f"**Email:** {email}")
    if youtube_link != "Not listed":
        extra_lines.append(f"**YouTube:** {youtube_link}")
    if website_link != "Not listed":
        extra_lines.append(f"**Website:** {website_link}")

    extras = "\n".join(extra_lines)
    if extras:
        extras += "\n"

    # raw URL on its own line = most reliable clickable format
    content = (
        f"🔥 Cold lead spotted. Time to warm it up.\n\n"
        f"**Job:** {title}\n"
        f"{url}\n\n"
        f"**Type:** {job_type}\n"
        f"**Location:** {location}\n"
        f"**Pay:** {pay}\n"
        f"**Creator / Poster:** {creator}\n"
        f"{extras}"
        f"**Description:** {description}"
    )

    payload = {
        "username": "Manifest Media Leads",
        "content": content[:1900],
        "allowed_mentions": {"parse": []},
    }

    log(f"[DISCORD] Sending {source} job: {title}")
    log(f"[DISCORD] URL: {url}")

    response = requests.post(webhook_url, json=payload, timeout=30)

    log(f"[DISCORD] Status: {response.status_code}")
    if response.status_code >= 400:
        log(f"[DISCORD] Error body: {response.text}")

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
    log(f"[YTJOBS] Visiting {YTJOBS_URL}")
    await page.goto(YTJOBS_URL, wait_until="networkidle")
    await page.wait_for_timeout(3000)

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

        # Try to get a better bounded container than plain parent
        card = a.find_parent(["article", "li"]) or a.find_parent("div")
        context = clean_text(card.get_text(" ", strip=True) if card else raw_title)

        full_url = href if href.startswith("http") else f"https://ytjobs.co{href}"

        title = clean_job_title(raw_title)
        pay = extract_pay(context)
        job_type = extract_job_type(context)
        location = extract_location(context)
        creator = extract_creator(context)
        summary = extract_description(title, context)

        if not full_url.startswith("http"):
            continue

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

    log(f"[YTJOBS] Found: {len(jobs)}")
    return dedupe_jobs(jobs)


async def scrape_roster(page) -> List[Dict[str, Any]]:
    log(f"[ROSTER] Visiting {ROSTER_URL}")
    await page.goto(ROSTER_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(8000)

    for _ in range(3):
        await page.mouse.wheel(0, 3000)
        await page.wait_for_timeout(1500)

    current_url = page.url
    html = await page.content()

    log(f"[ROSTER] Final URL: {current_url}")
    log(f"[ROSTER] HTML length: {len(html)}")
    log(f"[ROSTER] HTML preview: {html[:800]}")

    jobs: List[Dict[str, Any]] = []

    anchors = await page.eval_on_selector_all(
        "a[href]",
        """elements => elements.map(a => ({
            href: a.href || "",
            text: (a.innerText || a.textContent || "").trim()
        }))"""
    )

    log(f"[ROSTER] Total anchors found: {len(anchors)}")

    seen_urls = set()

    for item in anchors:
        href = clean_text(item.get("href", ""))
        text = clean_text(item.get("text", ""))

        if not href.startswith("http"):
            continue

        href_lower = href.lower()

        if "joinroster.co/jobs/" not in href_lower and "app.joinroster.co/jobs/" not in href_lower:
            continue

        if href.rstrip("/") in {
            "https://www.joinroster.co/jobs",
            "https://app.joinroster.co/jobs",
        }:
            continue

        junk_texts = {
            "",
            "log in",
            "login",
            "sign up",
            "create free account",
            "create free account →",
            "view full job description",
            "apply",
            "apply now",
        }
        if text.lower() in junk_texts:
            continue

        if href in seen_urls:
            continue
        seen_urls.add(href)

        title = clean_job_title(text) if text else "Roster Job"

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

        log(f"[ROSTER] Anchor candidate -> title={title!r} href={href!r}")

    if not jobs:
        log("[ROSTER] No anchor-based jobs found. Trying regex fallback...")

        patterns = [
            r'https://www\.joinroster\.co/jobs/[^"\s<>]+',
            r'https://app\.joinroster\.co/jobs/[^"\s<>]+',
            r'/jobs/[^"\s<>]+',
        ]

        urls = set()
        for pattern in patterns:
            matches = re.findall(pattern, html, flags=re.IGNORECASE)
            log(f"[ROSTER] Regex {pattern} -> {len(matches)} matches")
            urls.update(matches)

        normalized_urls = set()
        for href in urls:
            full_href = urljoin(current_url, href)
            if "/jobs/" not in full_href.lower():
                continue
            if full_href.rstrip("/") in {
                "https://www.joinroster.co/jobs",
                "https://app.joinroster.co/jobs",
            }:
                continue
            normalized_urls.add(full_href)

        for href in sorted(normalized_urls):
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
            log(f"[ROSTER] Regex candidate -> href={href!r}")

    log(f"[ROSTER] Jobs found: {len(jobs)}")
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

        detail_description = clean_text(detail.get("detail_description", "").strip())
        if detail_description and detail_description != "No description listed.":
            job["summary"] = detail_description[:400]

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
            log(f"YTJobs scrape failed: {e}")

        try:
            jobs.extend(await scrape_roster(list_page))
        except Exception as e:
            log(f"Roster scrape failed: {e}")

        try:
            jobs = await enrich_jobs_with_detail(detail_page, jobs)
        except Exception as e:
            log(f"Detail enrichment failed: {e}")

        await browser.close()
        return jobs


async def main() -> None:
    seen = load_seen()
    jobs = await fetch_jobs()

    log(f"Total fetched jobs before seen filter: {len(jobs)}")
    log(f"YTJobs count: {len([j for j in jobs if j['source'] == 'YTJobs'])}")
    log(f"Roster count: {len([j for j in jobs if j['source'] == 'Roster'])}")

    new_count = 0

    for job in jobs:
        if job["id"] in seen:
            continue

        try:
            send_to_discord(job)
            seen.add(job["id"])
            new_count += 1
            log(f"Posted: {job['title']} ({job['source']})")

            if POST_DELAY_SECONDS > 0:
                log(f"Waiting {POST_DELAY_SECONDS} seconds before next post...")
                await asyncio.sleep(POST_DELAY_SECONDS)

        except Exception as e:
            log(f"Error sending job: {e}")

    save_seen(seen)
    log(f"Done. Sent {new_count} new jobs.")


if __name__ == "__main__":
    asyncio.run(main())
