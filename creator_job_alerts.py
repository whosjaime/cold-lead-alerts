import asyncio
import hashlib
import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

PENDING_FILE = Path("pending_jobs.json")

YTJOBS_WEBHOOK_URL = os.getenv("YTJOBS_WEBHOOK_URL", "")
ROSTER_WEBHOOK_URL = os.getenv("ROSTER_WEBHOOK_URL", "")
WEBHOOK_AVATAR_URL = os.getenv("WEBHOOK_AVATAR_URL", "")

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN", "")
MONDAY_BOARD_ID = os.getenv("MONDAY_BOARD_ID", "")
MONDAY_GROUP_ID = os.getenv("MONDAY_GROUP_ID", "")

MONDAY_COL_PAY = os.getenv("MONDAY_COL_PAY", "")
MONDAY_COL_TYPE = os.getenv("MONDAY_COL_TYPE", "")
MONDAY_COL_PRIMARY_SKILL = os.getenv("MONDAY_COL_PRIMARY_SKILL", "")
MONDAY_COL_PLATFORM = os.getenv("MONDAY_COL_PLATFORM", "")
MONDAY_COL_SOURCED_FROM = os.getenv("MONDAY_COL_SOURCED_FROM", "")
MONDAY_COL_CATEGORY = os.getenv("MONDAY_COL_CATEGORY", "")
MONDAY_COL_COMPANY = os.getenv("MONDAY_COL_COMPANY", "")
MONDAY_COL_ROLE = os.getenv("MONDAY_COL_ROLE", "")
MONDAY_COL_LOCATION = os.getenv("MONDAY_COL_LOCATION", "")
MONDAY_COL_DESCRIPTION = os.getenv("MONDAY_COL_DESCRIPTION", "")
MONDAY_COL_LINK = os.getenv("MONDAY_COL_LINK", "")
MONDAY_COL_POST_DATE = os.getenv("MONDAY_COL_POST_DATE", "")

YTJOBS_URL = "https://ytjobs.co/job/search"
ROSTER_URL = "https://www.joinroster.co/jobs"

HEADER_TEXT = "Cold leads, warm them up! 🔥"
VALID_SOURCES = ("YTJobs", "Roster")

ROLE_IDS = {
    "channel_manager": "1482015129150427166",
    "creative_director": "1482015129762660637",
    "thumbnail_designer": "1482015130807046194",
    "scriptwriter": "1482015131482194094",
    "editor": "1482015132753330236",
    "production_manager": "1482015133889986753",
    "strategist": "1482015134452023296",
}

JUNK_TITLE_PATTERNS = [
    r"^company about us",
    r"^all you have to do is",
    r"^be a beutiful prod",
    r"^video editing services$",
]


def load_pending() -> Dict[str, List[Dict[str, Any]]]:
    default_pending: Dict[str, List[Dict[str, Any]]] = {source: [] for source in VALID_SOURCES}

    if not PENDING_FILE.exists():
        return default_pending

    try:
        data = json.loads(PENDING_FILE.read_text())
    except Exception:
        return default_pending

    if isinstance(data, dict):
        normalized = {source: [] for source in VALID_SOURCES}
        for source in VALID_SOURCES:
            items = data.get(source, [])
            normalized[source] = items if isinstance(items, list) else []
        return normalized

    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            source = item.get("source")
            if source in default_pending:
                default_pending[source].append(item)
        return default_pending

    return default_pending


def save_pending(items: Dict[str, List[Dict[str, Any]]]) -> None:
    normalized = {source: items.get(source, []) for source in VALID_SOURCES}
    PENDING_FILE.write_text(json.dumps(normalized, indent=2))


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
        r"\bIn[- ]?person\b",
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
    if "in-person" in lower or "in person" in lower:
        return "In-person"
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
    text = text.replace("About the Channel", " About the Channel")
    text = text.replace("About the Job", " About the Job")
    if text.lower().startswith(role.lower()):
        return clip(text, 220)
    return clip(f"{role} — {text}", 220)


def is_junk_job(job: Dict[str, Any]) -> bool:
    title = clean_text(job.get("title", "")).lower()
    summary = clean_text(job.get("summary", "")).lower()

    if not title or title == "new job":
        return True

    for pattern in JUNK_TITLE_PATTERNS:
        if re.search(pattern, title, flags=re.IGNORECASE):
            return True

    if "privacy terms of service" in summary:
        return True
    if title.count(" ") < 1 and len(title) < 4:
        return True

    return False


def detect_role_tag(title: str, summary: str) -> Optional[str]:
    text = f"{title} {summary}".lower()

    if "thumbnail" in text:
        return "thumbnail_designer"

    if "creative director" in text or "content director" in text:
        return "creative_director"

    if "channel manager" in text or "youtube channel manager" in text:
        return "channel_manager"

    if "strategist" in text or "strategy" in text:
        return "strategist"

    if (
        "script" in text
        or "scriptwriter" in text
        or "script writer" in text
        or "copywriter" in text
        or "video essay writer" in text
    ):
        return "scriptwriter"

    if "editor" in text:
        return "editor"

    if (
        "producer" in text
        or "production manager" in text
        or "production" in text
        or "content producer" in text
    ):
        return "production_manager"

    return None


def build_role_line_and_mentions(title: str, summary: str) -> tuple[str, Dict[str, Any]]:
    role_key = detect_role_tag(title, summary)
    role_line = f"**Role:** {title}"
    allowed_mentions: Dict[str, Any] = {"parse": []}

    if role_key and role_key in ROLE_IDS:
        role_line += f"\n<@&{ROLE_IDS[role_key]}>"
        allowed_mentions["roles"] = [ROLE_IDS[role_key]]

    return role_line, allowed_mentions


def monday_company_name(job: Dict[str, Any]) -> str:
    title = clean_text(job.get("title", ""))
    source = job.get("source", "")

    if source == "Roster":
        return clip(title, 255)

    return "Unknown"


def map_monday_type(job_type: str, pay: str) -> Optional[str]:
    jt = clean_text(job_type).lower()
    pay_text = clean_text(pay).lower()

    if "per project" in jt or "per project" in pay_text:
        return "Per Project"
    if "/hour" in pay_text or "per hour" in pay_text or "/hr" in pay_text:
        return "Per Hour"
    if "salary" in pay_text or "/year" in pay_text or "per year" in pay_text:
        return "Salary"

    return None


def map_monday_platform(source: str) -> Optional[str]:
    if source in {"YTJobs", "Roster"}:
        return "YouTube"
    return "Other"


def map_monday_sourced_from(source: str) -> Optional[str]:
    if source == "YTJobs":
        return "YTJobs"
    if source == "Roster":
        return "Roster"
    return None


def map_monday_category(job: Dict[str, Any]) -> Optional[str]:
    source = job.get("source", "")
    title = clean_text(job.get("title", ""))
    summary = clean_text(job.get("summary", ""))
    text = f"{title} {summary}".lower()

    if source == "YTJobs":
        return "YouTuber"

    if source == "Roster":
        if any(word in text for word in ["agency", "client", "clients"]):
            return "Agency"
        if any(word in text for word in ["startup", "saas", "founder"]):
            return "Startup"
        if any(word in text for word in ["company", "brand", "business"]):
            return "Company"
        return "Creator"

    return None


def map_monday_location(location: str) -> Optional[str]:
    loc = clean_text(location).lower()
    if loc == "remote":
        return "Remote"
    if loc == "hybrid":
        return "Hybrid"
    if loc in {"on-site", "onsite", "in-person", "in person"}:
        return "Onsite"
    return None


def map_monday_role_label(job: Dict[str, Any]) -> str:
    role_key = detect_role_tag(job.get("title", ""), job.get("summary", ""))

    mapping = {
        "editor": "Video Editor",
        "scriptwriter": "Scriptwriter",
        "thumbnail_designer": "Thumbnail Designer",
        "strategist": "Strategist",
        "channel_manager": "Channel Manager",
        "creative_director": "Creative Director",
        "production_manager": "Producer",
    }

    return mapping.get(role_key, "Other")


def extract_numeric_pay(pay: str) -> Optional[float]:
    text = clean_text(pay)
    match = re.search(r"\$?(\d[\d,]*)(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def send_to_discord(job: Dict[str, Any]) -> None:
    source = job.get("source", "Unknown")
    webhook_url = get_webhook_url(source)

    if not webhook_url:
        raise RuntimeError(f"Missing webhook URL for source: {source}")

    title = clip(job.get("title", "New job"), 100)
    location = clip(job.get("location", "Not listed"), 60)
    job_type = clip(job.get("job_type", "Not listed"), 60)
    pay = clip(job.get("pay", "Not listed"), 80)
    description = clip(job.get("summary", "No description listed."), 220)
    url = (job.get("url") or "").strip()

    role_line, allowed_mentions = build_role_line_and_mentions(title, description)

    content = (
        f"{HEADER_TEXT}\n\n"
        f"{role_line}\n"
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
        "allowed_mentions": allowed_mentions,
    }

    if WEBHOOK_AVATAR_URL:
        payload["avatar_url"] = WEBHOOK_AVATAR_URL

    response = requests.post(webhook_url, json=payload, timeout=30)
    print(f"Discord response for {source}: {response.status_code}")
    response.raise_for_status()


def send_to_monday(job: Dict[str, Any]) -> None:
    if not MONDAY_API_TOKEN or not MONDAY_BOARD_ID:
        print("Monday not configured, skipping.")
        return

    role_title = clip(job.get("title", "New lead"), 255)
    source = job.get("source", "Unknown")
    job_type = job.get("job_type", "Not listed")
    location = job.get("location", "Not listed")
    pay = job.get("pay", "Not listed")
    description = clip(job.get("summary", "No description listed."), 1000)
    url = (job.get("url") or "").strip()

    company = monday_company_name(job)
    primary_skill = map_monday_role_label(job)
    role_position = map_monday_role_label(job)
    monday_type = map_monday_type(job_type, pay)
    monday_platform = map_monday_platform(source)
    monday_sourced_from = map_monday_sourced_from(source)
    monday_category = map_monday_category(job)
    monday_location = map_monday_location(location)
    post_date = str(date.today())
    numeric_pay = extract_numeric_pay(pay)

    column_values: Dict[str, Any] = {}

    if MONDAY_COL_PAY and numeric_pay is not None:
        column_values[MONDAY_COL_PAY] = numeric_pay

    if MONDAY_COL_TYPE and monday_type:
        column_values[MONDAY_COL_TYPE] = {"labels": [monday_type]}

    if MONDAY_COL_PRIMARY_SKILL and primary_skill:
        column_values[MONDAY_COL_PRIMARY_SKILL] = {"labels": [primary_skill]}

    if MONDAY_COL_ROLE and role_position:
        column_values[MONDAY_COL_ROLE] = {"labels": [role_position]}

    if MONDAY_COL_LOCATION and monday_location:
        column_values[MONDAY_COL_LOCATION] = {"labels": [monday_location]}

    if MONDAY_COL_PLATFORM and monday_platform:
        column_values[MONDAY_COL_PLATFORM] = {"label": monday_platform}

    if MONDAY_COL_SOURCED_FROM and monday_sourced_from:
        column_values[MONDAY_COL_SOURCED_FROM] = {"label": monday_sourced_from}

    if MONDAY_COL_CATEGORY and monday_category:
        column_values[MONDAY_COL_CATEGORY] = {"label": monday_category}

    if MONDAY_COL_COMPANY and company and company != "Unknown":
        column_values[MONDAY_COL_COMPANY] = company

    if MONDAY_COL_DESCRIPTION:
        column_values[MONDAY_COL_DESCRIPTION] = description

    if MONDAY_COL_LINK and url:
        column_values[MONDAY_COL_LINK] = {"url": url, "text": "Job post"}

    if MONDAY_COL_POST_DATE:
        column_values[MONDAY_COL_POST_DATE] = {"date": post_date}

    query = """
    mutation CreateItem($board_id: ID!, $group_id: String, $item_name: String!, $column_values: JSON!) {
      create_item(
        board_id: $board_id,
        group_id: $group_id,
        item_name: $item_name,
        column_values: $column_values
      ) {
        id
      }
    }
    """

    variables = {
        "board_id": str(MONDAY_BOARD_ID),
        "group_id": MONDAY_GROUP_ID or None,
        "item_name": role_title,
        "column_values": json.dumps(column_values),
    }

    print("Monday variables:")
    print(json.dumps(variables, indent=2))

    response = requests.post(
        "https://api.monday.com/v2",
        headers={
            "Authorization": MONDAY_API_TOKEN,
            "Content-Type": "application/json",
        },
        json={"query": query, "variables": variables},
        timeout=30,
    )

    print(f"Monday status: {response.status_code}")
    print(f"Monday raw response: {response.text}")

    response.raise_for_status()

    payload = response.json()
    if "errors" in payload:
        raise RuntimeError(f"Monday API error: {json.dumps(payload['errors'], indent=2)}")

    print(f"Monday item created for: {role_title}")


def dedupe_jobs(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen_ids = set()
    cleaned: List[Dict[str, Any]] = []
    for job in jobs:
        if job["id"] in seen_ids:
            continue
        if is_junk_job(job):
            print(f"Skipped junk job: {job.get('title', 'Unknown')} ({job.get('source', 'Unknown')})")
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

        title = clip(title, 80)
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
        description = description.replace("About the Channel", " About the Channel")
        description = description.replace("About the Job", " About the Job")
        description = clip(description, 220)

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


def enqueue_new_jobs(all_jobs: List[Dict[str, Any]], pending: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    pending_ids = {
        source: {job.get("id") for job in pending.get(source, [])}
        for source in VALID_SOURCES
    }
    added = {source: 0 for source in VALID_SOURCES}

    for job in all_jobs:
        source = job.get("source")
        if source not in pending_ids:
            continue
        if job["id"] in pending_ids[source]:
            continue

        pending[source].append(job)
        pending_ids[source].add(job["id"])
        added[source] += 1
        print(f"Queued: {job['title']} ({source})")

    return added


def post_next_job_for_source(source: str, pending: Dict[str, List[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
    queue = pending.get(source, [])
    if not queue:
        return None

    job = queue[0]

    try:
        send_to_discord(job)
    except Exception as e:
        print(f"Discord post failed for {source}, will retry next run: {e}")
        return None

    queue.pop(0)

    try:
        send_to_monday(job)
    except Exception as e:
        print(f"Monday create failed for {source}: {e}")

    return job


async def main() -> None:
    pending = load_pending()

    print(
        f"Pending jobs loaded: YTJobs={len(pending['YTJobs'])}, Roster={len(pending['Roster'])}"
    )

    jobs = await fetch_jobs()
    queued_count = enqueue_new_jobs(jobs, pending)
    save_pending(pending)

    print(
        f"Queued new jobs: YTJobs={queued_count['YTJobs']}, Roster={queued_count['Roster']}"
    )
    print(
        f"Pending queue sizes before post: YTJobs={len(pending['YTJobs'])}, Roster={len(pending['Roster'])}"
    )

    posted_ytjobs = post_next_job_for_source("YTJobs", pending)
    posted_roster = post_next_job_for_source("Roster", pending)
    save_pending(pending)

    if posted_ytjobs:
        print(f"Posted YTJobs: {posted_ytjobs['title']}")
    else:
        print("No YTJobs post sent this run.")

    if posted_roster:
        print(f"Posted Roster: {posted_roster['title']}")
    else:
        print("No Roster post sent this run.")

    print(
        f"Pending queue sizes after post: YTJobs={len(pending['YTJobs'])}, Roster={len(pending['Roster'])}"
    )


if __name__ == "__main__":
    asyncio.run(main())
