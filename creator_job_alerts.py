import asyncio
import hashlib
import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

PENDING_FILE = Path("pending_jobs.json")

YTJOBS_WEBHOOK_URL = os.getenv("YTJOBS_WEBHOOK_URL", "")
ROSTER_WEBHOOK_URL = os.getenv("ROSTER_WEBHOOK_URL", "")
YT_CAREERS_WEBHOOK_URL = os.getenv("YT_CAREERS_WEBHOOK_URL", "")
BOC_WEBHOOK_URL = os.getenv("BOC_WEBHOOK_URL", "")
WEBHOOK_AVATAR_URL = os.getenv("WEBHOOK_AVATAR_URL", "")

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN", "")
MONDAY_BOARD_ID = os.getenv("MONDAY_BOARD_ID", "")
MONDAY_GROUP_ID = os.getenv("MONDAY_GROUP_ID", "")

MONDAY_COL_PAY = os.getenv("MONDAY_COL_PAY", "")
MONDAY_COL_TYPE = os.getenv("MONDAY_COL_TYPE", "")
MONDAY_COL_EMAIL = os.getenv("MONDAY_COL_EMAIL", "")
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
YT_CAREERS_URL = "https://yt.careers/youtube-jobs"
BOC_URL = "https://www.bucketofcrabs.net/jobs"

HEADER_TEXT = "Cold leads, warm them up! 🔥"

# IMPORTANT:
# These source names are used in pending_jobs.json and Monday mapping.
# Keep YTCareers and BucketofCrabs matching your Monday labels exactly.
VALID_SOURCES = ("YTJobs", "Roster", "YTCareers", "BucketofCrabs")

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
    r"^sign in$",
    r"^log in$",
    r"^privacy policy$",
    r"^terms of service$",
    r"^home$",
    r"^jobs$",
    r"^youtube jobs$",
]

EMAIL_RE = re.compile(r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b", re.IGNORECASE)
BAD_EMAIL_PARTS = {
    "example.com",
    "email.com",
    "yourname",
    "hello@yourcompany",
    "name@email.com",
}


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

        # Preserve old queues if they exist.
        for source in VALID_SOURCES:
            items = data.get(source, [])
            normalized[source] = items if isinstance(items, list) else []

        # In case old versions used names with punctuation/spaces.
        if "YT.Careers" in data and not normalized["YTCareers"]:
            normalized["YTCareers"] = data.get("YT.Careers", [])
        if "Bucket of Crabs" in data and not normalized["BucketofCrabs"]:
            normalized["BucketofCrabs"] = data.get("Bucket of Crabs", [])

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
    if source == "YTCareers":
        return YT_CAREERS_WEBHOOK_URL
    if source == "BucketofCrabs":
        return BOC_WEBHOOK_URL
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
        r"\bView\b",
        r"\bPosted\b",
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
    if len(words) > 10:
        text = " ".join(words[:10])

    return clip(text, 80) if text else "New Job"


def extract_pay(text: str) -> str:
    text = clean_text(text)

    pay_patterns = [
        r"(\$\d[\d,]*(?:\.\d+)?\s*(?:k|K)?\s*(?:-|to|–|—)\s*\$?\d[\d,]*(?:\.\d+)?\s*(?:k|K)?(?:\s*(?:/|per)\s*(?:hour|hr|project|month|year|video|short))?)",
        r"(\$\d[\d,]*(?:\.\d+)?\s*(?:k|K)?(?:\s*(?:/|per)\s*(?:hour|hr|project|month|year|video|short))?)",
        r"(\d[\d,]*(?:\.\d+)?\s*(?:k|K)\s*(?:-|to|–|—)\s*\d[\d,]*(?:\.\d+)?\s*(?:k|K))",
    ]

    for pattern in pay_patterns:
        pay_match = re.search(pattern, text, flags=re.IGNORECASE)
        if pay_match:
            return clip(pay_match.group(1), 80)

    if re.search(r"\bunpaid\b", text, flags=re.IGNORECASE):
        return "Unpaid"

    return "Not listed"


def extract_location(text: str) -> str:
    text = clean_text(text)
    lower = text.lower()

    if "remote" in lower:
        return "Remote"
    if "hybrid" in lower:
        return "Hybrid"
    if "on-site" in lower or "onsite" in lower:
        return "On-site"
    if "in-person" in lower or "in person" in lower:
        return "In-person"

    # Try to capture city/state style after common labels.
    loc_match = re.search(
        r"(?:Location|Based in|City)\s*[:\-]?\s*([A-Za-z ,.\-]+)",
        text,
        flags=re.IGNORECASE,
    )
    if loc_match:
        return clip(loc_match.group(1), 80)

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
    if "internship" in lower or "intern" in lower:
        return "Internship"
    if "per project" in lower:
        return "Per project"
    return "Not listed"


def build_description(text: str, role: str) -> str:
    text = clean_text(text)
    text = text.replace("About the Channel", " About the Channel")
    text = text.replace("About the Job", " About the Job")
    text = text.replace("Responsibilities", " Responsibilities")
    text = text.replace("Requirements", " Requirements")

    if text.lower().startswith(role.lower()):
        return clip(text, 220)

    return clip(f"{role} — {text}", 220)


def is_junk_job(job: Dict[str, Any]) -> bool:
    title = clean_text(job.get("title", "")).lower()
    summary = clean_text(job.get("summary", "")).lower()
    url = clean_text(job.get("url", "")).lower()

    if not title or title == "new job":
        return True

    for pattern in JUNK_TITLE_PATTERNS:
        if re.search(pattern, title, flags=re.IGNORECASE):
            return True

    if "privacy terms of service" in summary:
        return True

    if title.count(" ") < 1 and len(title) < 4:
        return True

    if not url:
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
        or "writer" in text
    ):
        return "scriptwriter"

    if (
        "editor" in text
        or "editing" in text
        or "post-production" in text
        or "post production" in text
    ):
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
    company = clean_text(job.get("company", ""))
    if company:
        return clip(company, 255)

    title = clean_text(job.get("title", ""))
    source = job.get("source", "")

    if source in {"Roster", "YTCareers", "BucketofCrabs"} and title:
        return clip(title, 255)

    return "Unknown"


def map_monday_type(job_type: str, pay: str) -> Optional[str]:
    jt = clean_text(job_type).lower()
    pay_text = clean_text(pay).lower()

    if "per project" in jt or "per project" in pay_text:
        return "Per Project"
    if "/hour" in pay_text or "per hour" in pay_text or "/hr" in pay_text:
        return "Per Hour"
    if "salary" in pay_text or "/year" in pay_text or "per year" in pay_text or "k" in pay_text:
        return "Salary"

    return None


def map_monday_platform(source: str) -> Optional[str]:
    if source in {"YTJobs", "Roster", "YTCareers", "BucketofCrabs"}:
        return "YouTube"
    return "Other"


def map_monday_sourced_from(source: str) -> Optional[str]:
    if source == "YTJobs":
        return "YTJobs"
    if source == "Roster":
        return "Roster"
    if source == "YTCareers":
        return "YTCareers"
    if source == "BucketofCrabs":
        return "BucketofCrabs"
    return None


def map_monday_category(job: Dict[str, Any]) -> Optional[str]:
    source = job.get("source", "")
    title = clean_text(job.get("title", ""))
    summary = clean_text(job.get("summary", ""))
    text = f"{title} {summary}".lower()

    if source == "YTJobs":
        return "YouTuber"

    if source == "YTCareers":
        return "Creator"

    if source == "BucketofCrabs":
        if any(word in text for word in ["agency", "client", "clients"]):
            return "Agency"
        if any(word in text for word in ["startup", "saas", "founder"]):
            return "Startup"
        if any(word in text for word in ["company", "brand", "business"]):
            return "Company"
        return "Creator"

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

    # Convert "$50k" to 50000
    k_match = re.search(r"\$?(\d+(?:\.\d+)?)\s*[kK]\b", text)
    if k_match:
        try:
            return float(k_match.group(1)) * 1000
        except ValueError:
            return None

    match = re.search(r"\$?(\d[\d,]*)(?:\.\d+)?", text)
    if not match:
        return None

    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def normalize_email(email: str) -> Optional[str]:
    email = clean_text(email).strip(".,;:()[]{}<>\"'")
    lower = email.lower()

    if "@" not in lower:
        return None
    if any(bad in lower for bad in BAD_EMAIL_PARTS):
        return None
    if lower.endswith((".png", ".jpg", ".jpeg", ".webp", ".svg")):
        return None
    return lower


def extract_emails_from_text(text: str) -> List[str]:
    found: List[str] = []
    for match in EMAIL_RE.findall(text or ""):
        email = normalize_email(match)
        if email and email not in found:
            found.append(email)
    return found


def find_first_public_email_in_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    mailtos = []
    for a in soup.select('a[href^="mailto:"]'):
        href = a.get("href") or ""
        candidate = href.replace("mailto:", "").split("?")[0].strip()
        email = normalize_email(candidate)
        if email:
            mailtos.append(email)

    if mailtos:
        return mailtos[0]

    text_emails = extract_emails_from_text(soup.get_text(" ", strip=True))
    return text_emails[0] if text_emails else None


def find_candidate_links(html: str, base_url: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    links: Dict[str, Optional[str]] = {
        "website": None,
        "youtube": None,
    }

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        full = urljoin(base_url, href)
        lower = full.lower()

        if not links["youtube"] and ("youtube.com" in lower or "youtu.be" in lower):
            links["youtube"] = full

        if not links["website"]:
            if (
                lower.startswith("http")
                and "joinroster.co" not in lower
                and "ytjobs.co" not in lower
                and "yt.careers" not in lower
                and "bucketofcrabs.net" not in lower
                and "youtube.com" not in lower
                and "youtu.be" not in lower
            ):
                links["website"] = full

    return links


def discover_contact_pages(base_url: str, html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    found: List[str] = []

    keywords = ("contact", "about", "business", "inquiries", "team")
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        text = clean_text(a.get_text(" ", strip=True)).lower()
        full = urljoin(base_url, href)
        lower = full.lower()

        if any(k in text for k in keywords) or any(k in lower for k in keywords):
            if full not in found:
                found.append(full)

    common_paths = ["/contact", "/contact-us", "/about", "/about-us"]
    for path in common_paths:
        candidate = urljoin(base_url, path)
        if candidate not in found:
            found.append(candidate)

    return found[:8]


def safe_get(url: str, timeout: int = 20) -> Optional[requests.Response]:
    try:
        response = requests.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; ManifestMediaLeadBot/1.0)"
            },
            allow_redirects=True,
        )
        if response.status_code >= 400:
            return None
        content_type = (response.headers.get("Content-Type") or "").lower()
        if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
            return None
        return response
    except Exception:
        return None


def enrich_public_email(job: Dict[str, Any]) -> None:
    checked_urls: Set[str] = set()
    sources_checked: List[str] = []

    def try_url(url: Optional[str], label: str) -> Optional[str]:
        if not url:
            return None
        if url in checked_urls:
            return None
        checked_urls.add(url)
        sources_checked.append(f"{label}:{url}")

        response = safe_get(url)
        if not response:
            return None

        email = find_first_public_email_in_html(response.text)
        if email:
            return email

        return None

    job["email"] = None
    job["email_source"] = None

    direct_url = (job.get("url") or "").strip()
    if direct_url:
        response = safe_get(direct_url)
        if response:
            email = find_first_public_email_in_html(response.text)
            if email:
                job["email"] = email
                job["email_source"] = "job_post"
                print(f"Found public email on job post for {job.get('title')}: {email}")
                return

            links = find_candidate_links(response.text, direct_url)

            website_url = links.get("website")
            if website_url:
                email = try_url(website_url, "website")
                if email:
                    job["email"] = email
                    job["email_source"] = "website"
                    print(f"Found public email on website for {job.get('title')}: {email}")
                    return

                website_response = safe_get(website_url)
                if website_response:
                    for page_url in discover_contact_pages(website_url, website_response.text):
                        email = try_url(page_url, "website_contact")
                        if email:
                            job["email"] = email
                            job["email_source"] = "website_contact"
                            print(
                                f"Found public email on website contact page for {job.get('title')}: {email}"
                            )
                            return

            youtube_url = links.get("youtube")
            if youtube_url:
                email = try_url(youtube_url, "youtube")
                if email:
                    job["email"] = email
                    job["email_source"] = "youtube_public"
                    print(f"Found public email on YouTube page for {job.get('title')}: {email}")
                    return

    print(f"No public email found for {job.get('title')}. Checked: {sources_checked}")


def send_to_discord(job: Dict[str, Any]) -> None:
    source = job.get("source", "Unknown")
    webhook_url = get_webhook_url(source)

    if not webhook_url:
        raise RuntimeError(f"Missing webhook URL for source: {source}")

    title = clip(job.get("title", "New job"), 100)
    company = clip(job.get("company", "") or "Not listed", 80)
    location = clip(job.get("location", "Not listed"), 60)
    job_type = clip(job.get("job_type", "Not listed"), 60)
    pay = clip(job.get("pay", "Not listed"), 80)
    description = clip(job.get("summary", "No description listed."), 220)
    url = (job.get("url") or "").strip()

    role_line, allowed_mentions = build_role_line_and_mentions(title, description)

    content = (
        f"{HEADER_TEXT}\n\n"
        f"{role_line}\n"
        f"**Company:** {company}\n"
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
    email = clean_text(job.get("email"))

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

    if MONDAY_COL_EMAIL and email:
        column_values[MONDAY_COL_EMAIL] = {
            "email": email,
            "text": email,
        }

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


def extract_ytjobs_stable_id(full_url: str) -> str:
    match = re.search(r"/job/(\d+)", full_url)
    if match:
        return f"ytjobs_{match.group(1)}"
    return f"ytjobs_{hashlib.sha256(full_url.encode('utf-8')).hexdigest()}"


def extract_roster_stable_id(detail_url: str) -> str:
    normalized = clean_text(detail_url).rstrip("/")
    return f"roster_{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}"


def extract_ytcareers_stable_id(detail_url: str) -> str:
    normalized = clean_text(detail_url).rstrip("/")
    return f"ytcareers_{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}"


def extract_boc_stable_id(detail_url: str) -> str:
    normalized = clean_text(detail_url).rstrip("/")
    return f"boc_{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}"


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
                "id": extract_ytjobs_stable_id(full_url),
                "title": role,
                "summary": description,
                "location": location,
                "job_type": job_type,
                "pay": pay,
                "url": full_url,
                "source": "YTJobs",
                "email": None,
                "email_source": None,
                "company": None,
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

            # Return to job list for next Apply button.
            if page.url != ROSTER_URL:
                await page.goto(ROSTER_URL, wait_until="domcontentloaded")
                await page.wait_for_timeout(2000)
        except Exception as e:
            print(f"Roster click failed for block {i}: {e}")

        block = raw_blocks[i]
        parsed = parse_roster_block(block, i)

        jobs.append(
            {
                "id": extract_roster_stable_id(detail_url),
                "title": parsed["title"],
                "summary": parsed["summary"],
                "location": parsed["location"],
                "job_type": parsed["job_type"],
                "pay": parsed["pay"],
                "url": detail_url,
                "source": "Roster",
                "email": None,
                "email_source": None,
                "company": None,
            }
        )

    jobs = dedupe_jobs(jobs)
    print(f"Roster jobs found: {len(jobs)}")
    for job in jobs[:10]:
        print(f"Roster parsed job: {job['title']} | {job['pay']} | {job['location']} | {job['url']}")

    return jobs


async def scrape_ytcareers(page) -> List[Dict[str, Any]]:
    await page.goto(YT_CAREERS_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(5000)

    for _ in range(5):
        await page.mouse.wheel(0, 2500)
        await page.wait_for_timeout(800)

    html = await page.content()
    Path("ytcareers_debug.html").write_text(html, encoding="utf-8")

    body_text = await page.locator("body").inner_text()
    Path("ytcareers_debug.txt").write_text(body_text, encoding="utf-8")

    print(f"YTCareers body preview: {clip(body_text, 1200)}")

    soup = BeautifulSoup(html, "html.parser")
    jobs: List[Dict[str, Any]] = []

    # Main strategy: collect links likely pointing to job detail pages.
    candidate_links: List[tuple[str, str]] = []

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        text = clean_text(a.get_text(" ", strip=True))

        if not href:
            continue

        full_url = urljoin(YT_CAREERS_URL, href)
        lower = full_url.lower()

        if any(skip in lower for skip in ["#mailto", "mailto:", "javascript:"]):
            continue

        # Keep likely job detail links from yt.careers.
        if "yt.careers" in lower and (
            "/job" in lower
            or "/jobs" in lower
            or "/youtube-jobs/" in lower
            or "/opportunities" in lower
        ):
            if full_url.rstrip("/") == YT_CAREERS_URL.rstrip("/"):
                continue
            candidate_links.append((full_url, text))

    # Fallback: use sections/cards containing common job words.
    seen_urls = set()

    for full_url, link_text in candidate_links:
        normalized_url = full_url.rstrip("/")
        if normalized_url in seen_urls:
            continue
        seen_urls.add(normalized_url)

        # Try to use nearby card text.
        card_text = link_text
        try:
            locator = page.locator(f'a[href="{full_url}"]').first
            count = await locator.count()
            if count:
                card_text = await locator.locator("xpath=ancestor::*[self::div or self::article or self::li][1]").inner_text(timeout=2000)
        except Exception:
            pass

        context = clean_text(card_text or link_text)
        if len(context) < 10:
            context = link_text

        role = extract_role_only(context)
        pay = extract_pay(context)
        location = extract_location(context)
        job_type = extract_job_type(context)
        description = build_description(context, role)

        company = None
        # Try simple company extraction.
        company_match = re.search(r"(?:Company|Creator|Channel)\s*[:\-]?\s*([A-Za-z0-9 &.,'’\-]+)", context, flags=re.IGNORECASE)
        if company_match:
            company = clip(company_match.group(1), 120)

        jobs.append(
            {
                "id": extract_ytcareers_stable_id(normalized_url),
                "title": role,
                "summary": description,
                "location": location,
                "job_type": job_type,
                "pay": pay,
                "url": normalized_url,
                "source": "YTCareers",
                "email": None,
                "email_source": None,
                "company": company,
            }
        )

    # Strong fallback if no links parse correctly:
    if not jobs:
        lines = [clean_text(line) for line in body_text.splitlines()]
        lines = [line for line in lines if len(line) > 4]

        blocks: List[str] = []
        current: List[str] = []

        for line in lines:
            current.append(line)
            joined = " ".join(current)

            if re.search(r"\b(Apply|View Job|Posted|Remote|Full-time|Part-time|Contract|Freelance)\b", joined, flags=re.IGNORECASE):
                if len(joined) > 35:
                    blocks.append(clean_text(joined))
                    current = []

        for idx, block in enumerate(blocks[:50]):
            role = extract_role_only(block)
            pay = extract_pay(block)
            location = extract_location(block)
            job_type = extract_job_type(block)
            description = build_description(block, role)

            jobs.append(
                {
                    "id": extract_ytcareers_stable_id(make_id("ytcareers", role, block[:120])),
                    "title": role,
                    "summary": description,
                    "location": location,
                    "job_type": job_type,
                    "pay": pay,
                    "url": YT_CAREERS_URL,
                    "source": "YTCareers",
                    "email": None,
                    "email_source": None,
                    "company": None,
                }
            )

    jobs = dedupe_jobs(jobs)
    print(f"YTCareers jobs found: {len(jobs)}")
    for job in jobs[:10]:
        print(f"YTCareers parsed job: {job['title']} | {job['pay']} | {job['location']} | {job['url']}")

    return jobs


async def scrape_bucketofcrabs(page) -> List[Dict[str, Any]]:
    await page.goto(BOC_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(6000)

    for _ in range(5):
        await page.mouse.wheel(0, 2500)
        await page.wait_for_timeout(900)

    html = await page.content()
    Path("boc_debug.html").write_text(html, encoding="utf-8")

    body_text = await page.locator("body").inner_text()
    Path("boc_debug.txt").write_text(body_text, encoding="utf-8")

    print(f"BucketofCrabs body preview: {clip(body_text, 1200)}")

    soup = BeautifulSoup(html, "html.parser")
    jobs: List[Dict[str, Any]] = []

    candidate_links: List[tuple[str, str]] = []

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        text = clean_text(a.get_text(" ", strip=True))

        if not href:
            continue

        full_url = urljoin(BOC_URL, href)
        lower = full_url.lower()

        if any(skip in lower for skip in ["mailto:", "javascript:", "#"]):
            continue

        if "bucketofcrabs.net" in lower and (
            "/jobs/" in lower
            or "/job/" in lower
            or "jobs" in lower
        ):
            if full_url.rstrip("/") == BOC_URL.rstrip("/"):
                continue
            candidate_links.append((full_url, text))

    seen_urls = set()

    for full_url, link_text in candidate_links:
        normalized_url = full_url.rstrip("/")
        if normalized_url in seen_urls:
            continue
        seen_urls.add(normalized_url)

        card_text = link_text

        # Try to find surrounding card text using BeautifulSoup.
        matching_a = None
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if urljoin(BOC_URL, href).rstrip("/") == normalized_url:
                matching_a = a
                break

        if matching_a:
            parent = matching_a
            for _ in range(4):
                if parent and parent.parent:
                    parent = parent.parent
                    text = clean_text(parent.get_text(" ", strip=True))
                    if len(text) > len(card_text):
                        card_text = text
                else:
                    break

        context = clean_text(card_text or link_text)
        if len(context) < 10:
            context = link_text

        role = extract_role_only(context)
        pay = extract_pay(context)
        location = extract_location(context)
        job_type = extract_job_type(context)
        description = build_description(context, role)

        company = None
        company_match = re.search(r"(?:Company|Studio|Creator|Channel|Team)\s*[:\-]?\s*([A-Za-z0-9 &.,'’\-]+)", context, flags=re.IGNORECASE)
        if company_match:
            company = clip(company_match.group(1), 120)

        jobs.append(
            {
                "id": extract_boc_stable_id(normalized_url),
                "title": role,
                "summary": description,
                "location": location,
                "job_type": job_type,
                "pay": pay,
                "url": normalized_url,
                "source": "BucketofCrabs",
                "email": None,
                "email_source": None,
                "company": company,
            }
        )

    # Fallback if links do not expose jobs clearly.
    if not jobs:
        lines = [clean_text(line) for line in body_text.splitlines()]
        lines = [line for line in lines if len(line) > 4]

        blocks: List[str] = []
        current: List[str] = []

        for line in lines:
            current.append(line)
            joined = " ".join(current)

            if re.search(r"\b(Apply|View Job|Posted|Remote|Full-time|Part-time|Contract|Freelance)\b", joined, flags=re.IGNORECASE):
                if len(joined) > 35:
                    blocks.append(clean_text(joined))
                    current = []

        for idx, block in enumerate(blocks[:50]):
            role = extract_role_only(block)
            pay = extract_pay(block)
            location = extract_location(block)
            job_type = extract_job_type(block)
            description = build_description(block, role)

            jobs.append(
                {
                    "id": extract_boc_stable_id(make_id("boc", role, block[:120])),
                    "title": role,
                    "summary": description,
                    "location": location,
                    "job_type": job_type,
                    "pay": pay,
                    "url": BOC_URL,
                    "source": "BucketofCrabs",
                    "email": None,
                    "email_source": None,
                    "company": None,
                }
            )

    jobs = dedupe_jobs(jobs)
    print(f"BucketofCrabs jobs found: {len(jobs)}")
    for job in jobs[:10]:
        print(f"BucketofCrabs parsed job: {job['title']} | {job['pay']} | {job['location']} | {job['url']}")

    return jobs


async def fetch_jobs() -> List[Dict[str, Any]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )

        jobs: List[Dict[str, Any]] = []

        try:
            jobs.extend(await scrape_ytjobs(page))
        except Exception as e:
            print(f"YTJobs scrape failed: {e}")

        try:
            jobs.extend(await scrape_roster(page))
        except Exception as e:
            print(f"Roster scrape failed: {e}")

        try:
            jobs.extend(await scrape_ytcareers(page))
        except Exception as e:
            print(f"YTCareers scrape failed: {e}")

        try:
            jobs.extend(await scrape_bucketofcrabs(page))
        except Exception as e:
            print(f"BucketofCrabs scrape failed: {e}")

        await browser.close()
        return jobs


def enqueue_new_jobs(all_jobs: List[Dict[str, Any]], pending: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    pending_ids = {
        source: {job.get("id") for job in pending.get(source, [])}
        for source in VALID_SOURCES
    }
    pending_urls = {
        source: {clean_text(job.get("url", "")).rstrip("/") for job in pending.get(source, [])}
        for source in VALID_SOURCES
    }
    added = {source: 0 for source in VALID_SOURCES}

    for job in all_jobs:
        source = job.get("source")
        if source not in pending_ids:
            continue

        normalized_url = clean_text(job.get("url", "")).rstrip("/")

        if job["id"] in pending_ids[source]:
            continue

        if normalized_url and normalized_url in pending_urls[source]:
            print(f"Skipped duplicate URL already queued: {job['title']} ({source}) | {normalized_url}")
            continue

        pending[source].append(job)
        pending_ids[source].add(job["id"])
        if normalized_url:
            pending_urls[source].add(normalized_url)
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
        enrich_public_email(job)
    except Exception as e:
        print(f"Email enrichment failed for {source}: {e}")

    try:
        send_to_monday(job)
    except Exception as e:
        print(f"Monday create failed for {source}: {e}")

    return job


async def main() -> None:
    pending = load_pending()

    print(
        "Pending jobs loaded: "
        f"YTJobs={len(pending['YTJobs'])}, "
        f"Roster={len(pending['Roster'])}, "
        f"YTCareers={len(pending['YTCareers'])}, "
        f"BucketofCrabs={len(pending['BucketofCrabs'])}"
    )

    jobs = await fetch_jobs()
    queued_count = enqueue_new_jobs(jobs, pending)
    save_pending(pending)

    print(
        "Queued new jobs: "
        f"YTJobs={queued_count['YTJobs']}, "
        f"Roster={queued_count['Roster']}, "
        f"YTCareers={queued_count['YTCareers']}, "
        f"BucketofCrabs={queued_count['BucketofCrabs']}"
    )

    print(
        "Pending queue sizes before post: "
        f"YTJobs={len(pending['YTJobs'])}, "
        f"Roster={len(pending['Roster'])}, "
        f"YTCareers={len(pending['YTCareers'])}, "
        f"BucketofCrabs={len(pending['BucketofCrabs'])}"
    )

    posted_ytjobs = post_next_job_for_source("YTJobs", pending)
    posted_roster = post_next_job_for_source("Roster", pending)
    posted_ytcareers = post_next_job_for_source("YTCareers", pending)
    posted_boc = post_next_job_for_source("BucketofCrabs", pending)

    save_pending(pending)

    if posted_ytjobs:
        print(f"Posted YTJobs: {posted_ytjobs['title']}")
    else:
        print("No YTJobs post sent this run.")

    if posted_roster:
        print(f"Posted Roster: {posted_roster['title']}")
    else:
        print("No Roster post sent this run.")

    if posted_ytcareers:
        print(f"Posted YTCareers: {posted_ytcareers['title']}")
    else:
        print("No YTCareers post sent this run.")

    if posted_boc:
        print(f"Posted BucketofCrabs: {posted_boc['title']}")
    else:
        print("No BucketofCrabs post sent this run.")

    print(
        "Pending queue sizes after post: "
        f"YTJobs={len(pending['YTJobs'])}, "
        f"Roster={len(pending['Roster'])}, "
        f"YTCareers={len(pending['YTCareers'])}, "
        f"BucketofCrabs={len(pending['BucketofCrabs'])}"
    )


if __name__ == "__main__":
    asyncio.run(main())
