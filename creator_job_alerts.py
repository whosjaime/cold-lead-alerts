import asyncio
import hashlib
import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import quote, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

PENDING_FILE = Path("pending_jobs.json")

YTJOBS_WEBHOOK_URL = os.getenv("YTJOBS_WEBHOOK_URL", "")
ROSTER_WEBHOOK_URL = os.getenv("ROSTER_WEBHOOK_URL", "")
YT_CAREERS_WEBHOOK_URL = os.getenv("YT_CAREERS_WEBHOOK_URL", "")
BOC_WEBHOOK_URL = os.getenv("BOC_WEBHOOK_URL", "")
X_WEBHOOK_URL = os.getenv("X_WEBHOOK_URL", "")
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
ROSTER_URL = "https://app.joinroster.co/jobs"
YT_CAREERS_URL = "https://yt.careers/youtube-jobs"
BOC_URL = "https://www.bucketofcrabs.net/jobs"

HEADER_TEXT = "Cold leads, warm them up! 🔥"

VALID_SOURCES = ("YTJobs", "Roster", "YTCareers", "BucketofCrabs", "X")

ROLE_IDS = {
    "channel_manager": "1482015129150427166",
    "creative_director": "1482015129762660637",
    "thumbnail_designer": "1482015130807046194",
    "scriptwriter": "1482015131482194094",
    "editor": "1482015132753330236",
    "production_manager": "1482015133889986753",
    "strategist": "1482015134452023296",
}

X_SEARCH_QUERIES = [
    '"hiring editor"',
    '"looking for editor"',
    '"need an editor"',
    '"video editor needed"',
    '"youtube editor needed"',
    '"shorts editor"',
    '"short-form editor"',
    '"short form editor"',
    '"editor recommendations"',
    '"need editing help"',
    '"looking for a youtube editor"',
    '"need shorts editor"',
    '"looking for shorts editor"',
    '"hiring thumbnail designer"',
    '"looking for thumbnail designer"',
    '"thumbnail designer needed"',
    '"thumbnail artist needed"',
    '"need thumbnail help"',
    '"hiring scriptwriter"',
    '"looking for scriptwriter"',
    '"youtube writer needed"',
    '"hiring writer"',
]

X_BAD_TERMS = [
    "nft",
    "crypto",
    "forex",
    "casino",
    "gambling",
    "onlyfans",
    "hire me",
    "available for work",
    "my portfolio",
    "commission me",
    "i am an editor",
    "i'm an editor",
    "dm me for editing",
    "looking for work",
    "open for work",
]

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
    r"^create job offer$",
    r"^post a job$",
    r"^submit job$",
    r"^job title",
    r"^job title & info",
    r"^for creators",
    r"^for creators for talent",
    r"^find your next opportunity",
    r"^use our interactive ai tool",
    r"^for talent",
    r"^features",
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

        for source in VALID_SOURCES:
            items = data.get(source, [])
            normalized[source] = items if isinstance(items, list) else []

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


def count_unposted(pending: Dict[str, List[Dict[str, Any]]], source: str) -> int:
    return sum(1 for job in pending.get(source, []) if not job.get("posted"))


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


def normalize_url_for_dedupe(url: str) -> str:
    url = clean_text(url).strip()

    if not url:
        return ""

    parsed = urlparse(url)
    clean_parsed = parsed._replace(query="", fragment="")
    normalized = urlunparse(clean_parsed).rstrip("/")

    return normalized


def normalize_signature_value(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean_text(text).lower()).strip()


def job_signature(job: Dict[str, Any]) -> str:
    source = normalize_signature_value(job.get("source", ""))
    title = normalize_signature_value(job.get("title", ""))
    company = normalize_signature_value(job.get("company", ""))
    pay = normalize_signature_value(job.get("pay", ""))
    location = normalize_signature_value(job.get("location", ""))

    return " | ".join([source, title, company, pay, location])


def get_webhook_url(source: str) -> str:
    if source == "YTJobs":
        return YTJOBS_WEBHOOK_URL

    if source == "Roster":
        return ROSTER_WEBHOOK_URL

    if source == "YTCareers":
        return YT_CAREERS_WEBHOOK_URL

    if source == "BucketofCrabs":
        return BOC_WEBHOOK_URL

    if source == "X":
        return X_WEBHOOK_URL

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
        r"\bInternship\b",
        r"\bPer project\b",
        r"\bPer hour\b",
        r"\bOne[- ]?off project\b",
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
    if len(words) > 12:
        text = " ".join(words[:12])

    return clip(text, 80) if text else "New Job"


def clean_source_specific_title(source: str, text: str) -> str:
    text = clean_text(text)

    if source == "BucketofCrabs":
        text = re.sub(r"^Job Title\s*&\s*Info\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"^Job Title\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*Game\s*&\s*Date\s*", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*Posted\s+\w+\s+\d{1,2}(st|nd|rd|th)?\s+\d{4}.*$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*Remote Only.*$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*Long Term.*$", " Long Term", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*\$\d.*$", "", text)

    if source == "YTCareers":
        text = re.sub(r"^Create job offer$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*Apply Now.*$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*View Job.*$", "", text, flags=re.IGNORECASE)

    if source == "Roster":
        text = re.sub(r"^For creators For talent Features Login Get started Jobs\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"^Find your next opportunity in the creator economy\s*", "", text, flags=re.IGNORECASE)

    text = clean_text(text)

    return clip(text, 80) if text else "New Job"


def extract_pay(text: str) -> str:
    text = clean_text(text)

    pay_patterns = [
        r"(\$\d[\d,]*(?:\.\d+)?\s*(?:k|K)?\s*(?:-|to|–|—)\s*\$?\d[\d,]*(?:\.\d+)?\s*(?:k|K)?(?:\s*(?:/|per)\s*(?:hour|hr|project|month|year|video|vid|short|episode))?)",
        r"(\$\d[\d,]*(?:\.\d+)?\s*(?:k|K)?(?:\s*(?:/|per)\s*(?:hour|hr|project|month|year|video|vid|short|episode))?)",
        r"(\d[\d,]*(?:\.\d+)?\s*(?:k|K)\s*(?:-|to|–|—)\s*\d[\d,]*(?:\.\d+)?\s*(?:k|K))",
        r"(\d+\s*-\s*\d+\s*(?:usd|USD|Euro|euro|INR|inr)?(?:\s*per\s*(?:vid|video|episode|month|project))?)",
    ]

    for pattern in pay_patterns:
        pay_match = re.search(pattern, text, flags=re.IGNORECASE)
        if pay_match:
            return clip(pay_match.group(1), 80)

    if re.search(r"\bnegotiable\b", text, flags=re.IGNORECASE):
        return "Negotiable"

    if re.search(r"\bvoluntary\b", text, flags=re.IGNORECASE):
        return "Voluntary"

    if re.search(r"\bsee job description\b", text, flags=re.IGNORECASE):
        return "See job description"

    return "Not listed"


def extract_location(text: str) -> str:
    text = clean_text(text)
    lower = text.lower()

    if "worldwide remote" in lower or "remote only" in lower or "synchronous remote" in lower:
        return "Remote"

    if "remote" in lower:
        return "Remote"

    if "hybrid" in lower:
        return "Hybrid"

    if "on-site" in lower or "onsite" in lower:
        return "On-site"

    if "in-person" in lower or "in person" in lower:
        return "In-person"

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

    if "one-off project" in lower or "one off project" in lower:
        return "One-off project"

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

    if "voluntary" in lower:
        return "Voluntary"

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


def is_known_bad_listing_url(source: str, url: str) -> bool:
    normalized = normalize_url_for_dedupe(url).lower()

    bad_urls = {
        "https://www.joinroster.co/jobs",
        "https://joinroster.co/jobs",
        "https://app.joinroster.co/jobs",
        "https://yt.careers/youtube-jobs",
        "https://www.yt.careers/youtube-jobs",
        "https://www.bucketofcrabs.net/jobs",
        "https://bucketofcrabs.net/jobs",
    }

    if normalized in bad_urls:
        return True

    if source == "Roster" and re.fullmatch(r"https?://(www\.|app\.)?joinroster\.co/jobs/?", normalized):
        return True

    if source == "YTCareers" and re.fullmatch(r"https?://(www\.)?yt\.careers/youtube-jobs/?", normalized):
        return True

    return False


def is_junk_job(job: Dict[str, Any]) -> bool:
    source = clean_text(job.get("source", ""))
    title = clean_text(job.get("title", "")).lower()
    summary = clean_text(job.get("summary", "")).lower()
    url = clean_text(job.get("url", "")).lower()

    if source == "X":
        if not url or "x.com/" not in url or "/status/" not in url:
            return True

        if not summary:
            return True

        return False

    if not title or title == "new job":
        return True

    for pattern in JUNK_TITLE_PATTERNS:
        if re.search(pattern, title, flags=re.IGNORECASE):
            return True

    if is_known_bad_listing_url(source, url):
        return True

    bad_url_parts = [
        "/new",
        "/create",
        "/post",
        "/submit",
        "/login",
        "/sign-in",
        "/signin",
        "/register",
        "/pricing",
        "/privacy",
        "/terms",
    ]

    if any(part in url for part in bad_url_parts):
        return True

    if "privacy terms of service" in summary:
        return True

    if "for creators for talent features login" in summary:
        return True

    if "use our interactive ai tool" in summary:
        return True

    if title.count(" ") < 1 and len(title) < 4:
        return True

    if not url:
        return True

    return False


def detect_role_tag(title: str, summary: str) -> Optional[str]:
    text = f"{title} {summary}".lower()

    if any(word in text for word in [
        "thumbnail",
        "thumbnail designer",
        "thumbnail artist",
        "youtube thumbnail",
    ]):
        return "thumbnail_designer"

    if any(word in text for word in [
        "creative director",
        "content director",
        "creative lead",
        "head of creative",
        "creative strategist",
        "scene director",
    ]):
        return "creative_director"

    if any(word in text for word in [
        "channel manager",
        "youtube channel manager",
        "channel operator",
        "content operator",
        "youtube manager",
        "youtube specialist",
        "creator manager",
        "account manager",
    ]):
        return "channel_manager"

    if any(word in text for word in [
        "strategist",
        "strategy",
        "youtube strategist",
        "content strategist",
        "growth strategist",
        "audience development",
        "growth manager",
    ]):
        return "strategist"

    if any(word in text for word in [
        "scriptwriter",
        "script writer",
        "script",
        "writer",
        "copywriter",
        "video essay writer",
        "story writer",
        "narrative designer",
        "content writer",
    ]):
        return "scriptwriter"

    if any(word in text for word in [
        "editor",
        "video editor",
        "short-form editor",
        "short form editor",
        "long-form editor",
        "long form editor",
        "editing",
        "post-production",
        "post production",
        "motion graphics",
        "vfx",
        "after effects",
        "premiere pro",
        "davinci",
        "trailer maker",
        "clipper",
        "promo video",
    ]):
        return "editor"

    if any(word in text for word in [
        "producer",
        "production manager",
        "production coordinator",
        "production assistant",
        "content producer",
        "video producer",
        "line producer",
        "showrunner",
    ]):
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

    if company and company.lower() not in {"not listed", "unknown"}:
        return clip(company, 255)

    return "Unknown"


def map_monday_type(job_type: str, pay: str) -> Optional[str]:
    jt = clean_text(job_type).lower()
    pay_text = clean_text(pay).lower()

    if any(x in jt for x in ["one-off project", "one off project", "per project", "project"]):
        return "Per Project"

    if any(x in pay_text for x in [
        "fixed amount",
        "project based",
        "per video",
        "per vid",
        "per youtube episode",
        "per episode",
        "per project",
        "rate",
    ]):
        return "Per Project"

    if any(x in pay_text for x in ["/hour", "per hour", "/hr", "hourly"]):
        return "Per Hour"

    if any(x in jt for x in ["full-time", "full time"]):
        return "Salary"

    if any(x in pay_text for x in ["salary", "/year", "per year", "yearly", "k per year"]):
        return "Salary"

    if any(x in jt for x in ["part-time", "part time", "contract", "freelance"]):
        return "Per Project"

    if "monthly" in pay_text or "per month" in pay_text:
        return "Salary"

    return None


def map_monday_platform(source: str) -> Optional[str]:
    if source in {"YTJobs", "Roster", "YTCareers", "BucketofCrabs"}:
        return "YouTube"

    if source == "X":
        return "X"

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

    if source == "X":
        return "X"

    return None


def map_monday_category(job: Dict[str, Any]) -> Optional[str]:
    source = job.get("source", "")
    title = clean_text(job.get("title", ""))
    summary = clean_text(job.get("summary", ""))
    text = f"{title} {summary}".lower()

    if source == "X":
        return "Creator"

    if source == "YTJobs":
        return "YouTuber"

    if source == "YTCareers":
        return "Creator"

    if source in {"Roster", "BucketofCrabs"}:
        if any(word in text for word in ["agency", "client", "clients"]):
            return "Agency"

        if any(word in text for word in ["startup", "saas", "founder"]):
            return "Startup"

        if any(word in text for word in ["company", "brand", "business", "studio", "games"]):
            return "Company"

        return "Creator"

    return None


def map_monday_location(location: str) -> Optional[str]:
    loc = clean_text(location).lower()

    if loc in {"remote", "remote only", "worldwide remote", "synchronous remote"}:
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
    text = clean_text(pay).lower()

    if not text or text in {"not listed", "negotiable", "see job description", "voluntary"}:
        return None

    k_match = re.search(r"\$?(\d+(?:\.\d+)?)\s*k\b", text)
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
                and "x.com" not in lower
                and "twitter.com" not in lower
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
    if job.get("source") == "X":
        print("Skipping public email enrichment for X source.")
        job["email"] = None
        job["email_source"] = None
        return

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

    if source == "X":
        matched_query = job.get("matched_query", "Not listed")
        content = (
            f"{HEADER_TEXT}\n\n"
            f"{role_line}\n"
            f"**Creator/Profile:** {company}\n"
            f"**Source:** X\n"
            f"**Type:** {job_type}\n"
            f"**Location:** {location}\n"
            f"**Pay:** {pay}\n"
            f"**Matched Keyword:** {matched_query}\n"
            f"**Post:** {description}\n"
            f"**Link:** {url if url else 'Not listed'}"
        )
    else:
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
    seen_urls = set()
    seen_signatures = set()
    cleaned: List[Dict[str, Any]] = []

    for job in jobs:
        source = job.get("source", "")
        normalized_url = normalize_url_for_dedupe(job.get("url", ""))
        signature = job_signature(job)

        if job["id"] in seen_ids:
            continue

        if normalized_url and normalized_url in seen_urls:
            continue

        if source not in {"YTCareers", "X"}:
            if signature and signature in seen_signatures:
                continue

        if is_junk_job(job):
            print(f"Skipped junk job: {job.get('title', 'Unknown')} ({source})")
            continue

        seen_ids.add(job["id"])

        if normalized_url:
            seen_urls.add(normalized_url)

        if signature:
            seen_signatures.add(signature)

        cleaned.append(job)

    return cleaned


def clean_pending_queues(pending: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    removed = {source: 0 for source in VALID_SOURCES}

    for source in VALID_SOURCES:
        cleaned_queue = []
        seen_ids = set()
        seen_urls = set()
        seen_signatures = set()

        for job in pending.get(source, []):
            if not isinstance(job, dict):
                removed[source] += 1
                continue

            normalized_url = normalize_url_for_dedupe(job.get("url", ""))
            signature = job_signature(job)
            job_id = job.get("id")

            if is_junk_job(job):
                print(f"Removed junk pending job: {job.get('title', 'Unknown')} ({source}) | {job.get('url', '')}")
                removed[source] += 1
                continue

            if job_id and job_id in seen_ids:
                removed[source] += 1
                continue

            if normalized_url and normalized_url in seen_urls:
                removed[source] += 1
                continue

            if source not in {"YTCareers", "X"}:
                if signature and signature in seen_signatures:
                    removed[source] += 1
                    continue

            if job_id:
                seen_ids.add(job_id)

            if normalized_url:
                seen_urls.add(normalized_url)

            if signature:
                seen_signatures.add(signature)

            cleaned_queue.append(job)

        pending[source] = cleaned_queue

    return removed


def extract_ytjobs_stable_id(full_url: str) -> str:
    match = re.search(r"/job/(\d+)", full_url)

    if match:
        return f"ytjobs_{match.group(1)}"

    normalized = normalize_url_for_dedupe(full_url)
    return f"ytjobs_{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}"


def extract_roster_stable_id(detail_url: str, title: str = "", company: str = "") -> str:
    normalized_url = normalize_url_for_dedupe(detail_url).lower()
    normalized_title = normalize_signature_value(title)
    normalized_company = normalize_signature_value(company)

    base = " | ".join([normalized_url, normalized_title, normalized_company])
    return f"roster_{hashlib.sha256(base.encode('utf-8')).hexdigest()}"


def extract_ytcareers_stable_id(detail_url: str, title: str = "", company: str = "") -> str:
    normalized_url = normalize_url_for_dedupe(detail_url).lower()
    normalized_title = normalize_signature_value(title)
    normalized_company = normalize_signature_value(company)

    base = " | ".join([normalized_url, normalized_title, normalized_company])
    return f"ytcareers_{hashlib.sha256(base.encode('utf-8')).hexdigest()}"


def extract_boc_stable_id(detail_url: str) -> str:
    normalized = normalize_url_for_dedupe(detail_url)
    return f"boc_{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}"


def extract_x_stable_id(tweet_url: str) -> str:
    match = re.search(r"/status/(\d+)", tweet_url)

    if match:
        return f"x_{match.group(1)}"

    normalized = normalize_url_for_dedupe(tweet_url)
    return f"x_{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}"


def get_card_context(a) -> str:
    candidates = []

    for parent_name in ["article", "li", "div", "section"]:
        parent = a.find_parent(parent_name)
        if parent:
            text = clean_text(parent.get_text(" ", strip=True))
            if text:
                candidates.append(text)

    link_text = clean_text(a.get_text(" ", strip=True))
    if link_text:
        candidates.append(link_text)

    if not candidates:
        return ""

    candidates = sorted(candidates, key=len)

    for candidate in candidates:
        lower = candidate.lower()

        if len(candidate) <= 600 and "privacy terms" not in lower:
            return candidate

    return candidates[0]


def build_x_search_url(query: str) -> str:
    full_query = f"{query} lang:en -filter:replies"
    return f"https://x.com/search?q={quote(full_query)}&src=typed_query&f=live"


def is_bad_x_post(text: str) -> bool:
    lower = clean_text(text).lower()

    if any(term in lower for term in X_BAD_TERMS):
        return True

    role_terms = [
        "editor",
        "thumbnail",
        "scriptwriter",
        "script writer",
        "writer",
        "video editor",
        "youtube editor",
        "shorts editor",
        "short-form editor",
        "short form editor",
    ]

    return not any(term in lower for term in role_terms)


def x_title_from_text(text: str) -> str:
    lower = text.lower()

    if "thumbnail" in lower:
        return "Thumbnail Designer"

    if "scriptwriter" in lower or "script writer" in lower:
        return "Scriptwriter"

    if "writer" in lower:
        return "Writer"

    if "shorts editor" in lower or "short-form editor" in lower or "short form editor" in lower:
        return "Short-Form Video Editor"

    if "youtube editor" in lower:
        return "YouTube Video Editor"

    if "video editor" in lower or "editor" in lower:
        return "Video Editor"

    return "Creator Hiring Lead"


async def save_page_debug(page, html_path: str, png_path: str) -> None:
    try:
        await page.screenshot(path=png_path, full_page=True)
        html = await page.content()
        Path(html_path).write_text(html, encoding="utf-8")
    except Exception as e:
        print(f"Could not save debug files {html_path}/{png_path}: {e}")


async def scrape_ytjobs(page) -> List[Dict[str, Any]]:
   await page.goto(YTJOBS_URL, wait_until="domcontentloaded", timeout=60000)
await page.wait_for_timeout(3000)

    jobs: List[Dict[str, Any]] = []

    for a in soup.select('a[href*="/job/"]'):
        href = a.get("href") or ""

        if "/job/search" in href:
            continue

        full_url = normalize_url_for_dedupe(href if href.startswith("http") else f"https://ytjobs.co{href}")

        context = get_card_context(a)

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
                "posted": False,
            }
        )

    jobs = dedupe_jobs(jobs)
    print(f"YTJobs found: {len(jobs)}")

    return jobs


async def scrape_roster(page) -> List[Dict[str, Any]]:
    print("Starting Roster scrape...")

    # Roster no longer exposes a clean public no-login job feed at /jobs.
    # The public /jobs page is a marketing/post-a-job page, not actual job listings.
    # Returning [] prevents fake/junk posts and keeps the rest of the scraper running.
    print("Roster public jobs page does not expose no-login job listings. Skipping Roster for now.")

    return []


async def scrape_ytcareers(page) -> List[Dict[str, Any]]:
    print("Starting YTCareers scrape...")

    jobs: List[Dict[str, Any]] = []

    response = safe_get(YT_CAREERS_URL)

    if not response:
        print("YTCareers safe_get failed. Trying Playwright fallback...")

        await page.goto(
            YT_CAREERS_URL,
            wait_until="domcontentloaded",
            timeout=60000,
        )

        await page.wait_for_timeout(5000)

        html = await page.content()
    else:
        html = response.text

    Path("ytcareers_debug.html").write_text(html, encoding="utf-8")

    soup = BeautifulSoup(html, "html.parser")

    detail_urls: List[str] = []

    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        full_url = normalize_url_for_dedupe(urljoin(YT_CAREERS_URL, href))

        if re.search(r"/youtube-jobs/\d+$", full_url):
            if full_url not in detail_urls:
                detail_urls.append(full_url)

    print(f"YTCareers detail URLs found: {len(detail_urls)}")

    for detail_url in detail_urls[:30]:
        try:
            detail_response = safe_get(detail_url)

            if not detail_response:
                print(f"Could not fetch YTCareers detail page: {detail_url}")
                continue

            detail_soup = BeautifulSoup(detail_response.text, "html.parser")
            detail_text = clean_text(detail_soup.get_text(" ", strip=True))

            page_title = clean_text(detail_soup.title.get_text(" ", strip=True)) if detail_soup.title else ""

            if " - " in page_title:
                role = page_title.split(" - ")[0].strip()
            elif "·" in page_title:
                role = page_title.split("·")[0].strip()
            else:
                role = extract_role_only(detail_text)

            role = clean_source_specific_title("YTCareers", role)

            if not role or role.lower() in {"yt.careers", "job posts", "youtube jobs"}:
                role = "New YouTube Job"

            pay = extract_pay(detail_text)
            location = extract_location(detail_text)
            job_type = extract_job_type(detail_text)
            description = build_description(detail_text, role)

            jobs.append(
                {
                    "id": extract_ytcareers_stable_id(detail_url, role, ""),
                    "title": role,
                    "summary": description,
                    "location": location,
                    "job_type": job_type,
                    "pay": pay,
                    "url": detail_url,
                    "source": "YTCareers",
                    "email": None,
                    "email_source": None,
                    "company": None,
                    "posted": False,
                }
            )

            print(f"YTCareers parsed job: {role} | {pay} | {location} | {detail_url}")

        except Exception as e:
            print(f"YTCareers detail parse failed for {detail_url}: {e}")

    jobs = dedupe_jobs(jobs)

    print(f"YTCareers jobs found after dedupe: {len(jobs)}")

    return jobs


async def scrape_bucketofcrabs(page) -> List[Dict[str, Any]]:
    print("Starting BucketofCrabs scrape...")

    await page.goto(
        BOC_URL,
        wait_until="domcontentloaded",
        timeout=60000,
    )

    await page.wait_for_timeout(8000)

    for _ in range(6):
        await page.mouse.wheel(0, 2500)
        await page.wait_for_timeout(1500)

    await save_page_debug(page, "boc_debug.html", "boc_debug.png")

    jobs: List[Dict[str, Any]] = []

    anchors = await page.locator("a[href]").all()

    print(f"BucketofCrabs anchors found: {len(anchors)}")

    for anchor in anchors:
        try:
            href = await anchor.get_attribute("href")

            if not href:
                continue

            full_url = normalize_url_for_dedupe(urljoin(BOC_URL, href))
            lower = full_url.lower()

            if "bucketofcrabs.net" not in lower:
                continue

            if is_known_bad_listing_url("BucketofCrabs", full_url):
                continue

            if any(bad in lower for bad in [
                "/login",
                "/sign",
                "/register",
                "/pricing",
                "/privacy",
                "/terms",
                "/contact",
                "/about",
            ]):
                continue

            try:
                context = await anchor.evaluate(
                    """
                    el => {
                        const parent = el.closest('article, li, div, section');
                        return parent ? parent.innerText : el.innerText;
                    }
                    """
                )
            except Exception:
                context = await anchor.inner_text()

            context = clean_text(context)

            if not context or len(context) < 20:
                continue

            role = extract_role_only(context)
            role = clean_source_specific_title("BucketofCrabs", role)

            if not role or role.lower() == "new job":
                continue

            pay = extract_pay(context)
            location = extract_location(context)
            job_type = extract_job_type(context)
            description = build_description(context, role)

            jobs.append(
                {
                    "id": extract_boc_stable_id(full_url),
                    "title": role,
                    "summary": description,
                    "location": location,
                    "job_type": job_type,
                    "pay": pay,
                    "url": full_url,
                    "source": "BucketofCrabs",
                    "email": None,
                    "email_source": None,
                    "company": None,
                    "posted": False,
                }
            )

            print(f"BucketofCrabs parsed job: {role} | {pay} | {location} | {full_url}")

        except Exception as e:
            print(f"BucketofCrabs anchor parse failed: {e}")

    jobs = dedupe_jobs(jobs)

    print(f"BucketofCrabs jobs found after dedupe: {len(jobs)}")

    return jobs


async def scrape_x(page) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []

    for index, query in enumerate(X_SEARCH_QUERIES):
        search_url = build_x_search_url(query)
        print(f"Searching X: {query}")

        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(7000)

            if index == 0:
                await save_page_debug(page, "x_debug.html", "x_debug.png")

        except Exception as e:
            print(f"X search failed for {query}: {e}")
            continue

        for _ in range(2):
            try:
                await page.mouse.wheel(0, 1800)
                await page.wait_for_timeout(2000)
            except Exception:
                pass

        try:
            articles = await page.locator('article[data-testid="tweet"]').all()
        except Exception as e:
            print(f"Could not find X tweet articles for {query}: {e}")
            continue

        print(f"X articles found for {query}: {len(articles)}")

        for article in articles[:40]:
            try:
                text = clean_text(await article.inner_text())
            except Exception:
                continue

            if not text:
                continue

            if is_bad_x_post(text):
                continue

            try:
                links = await article.locator('a[href*="/status/"]').evaluate_all(
                    """els => els.map(a => a.href)"""
                )
            except Exception:
                links = []

            if not links:
                continue

            tweet_url = normalize_url_for_dedupe(links[0])
            if not tweet_url:
                continue

            username_match = re.search(r"x\.com/([^/]+)/status", tweet_url)
            username = username_match.group(1) if username_match else "unknown"

            title = x_title_from_text(text)
            pay = extract_pay(text)
            location = extract_location(text)
            job_type = extract_job_type(text)

            jobs.append(
                {
                    "id": extract_x_stable_id(tweet_url),
                    "title": title,
                    "summary": clip(text, 260),
                    "location": location,
                    "job_type": job_type,
                    "pay": pay,
                    "url": tweet_url,
                    "source": "X",
                    "email": None,
                    "email_source": None,
                    "company": f"@{username}",
                    "matched_query": query,
                    "posted": False,
                }
            )

        await page.wait_for_timeout(2500)

    jobs = dedupe_jobs(jobs)
    print(f"X jobs found: {len(jobs)}")

    for job in jobs[:10]:
        print(f"X parsed job: {job['title']} | {job['company']} | {job['url']}")

    return jobs


async def fetch_jobs() -> List[Dict[str, Any]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        page = await browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
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

        try:
            jobs.extend(await scrape_x(page))
        except Exception as e:
            print(f"X scrape failed: {e}")

        await browser.close()

        return jobs


def enqueue_new_jobs(all_jobs: List[Dict[str, Any]], pending: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    pending_ids = {
        source: {job.get("id") for job in pending.get(source, [])}
        for source in VALID_SOURCES
    }

    pending_urls = {
        source: {normalize_url_for_dedupe(job.get("url", "")) for job in pending.get(source, [])}
        for source in VALID_SOURCES
    }

    pending_signatures = {
        source: {job_signature(job) for job in pending.get(source, [])}
        for source in VALID_SOURCES
    }

    added = {source: 0 for source in VALID_SOURCES}

    for job in all_jobs:
        source = job.get("source")

        if source not in pending_ids:
            continue

        if is_junk_job(job):
            print(f"Skipped junk job before queue: {job.get('title', 'Unknown')} ({source})")
            continue

        normalized_url = normalize_url_for_dedupe(job.get("url", ""))
        signature = job_signature(job)

        if job["id"] in pending_ids[source]:
            continue

        if normalized_url and normalized_url in pending_urls[source]:
            print(f"Skipped duplicate URL already queued: {job['title']} ({source}) | {normalized_url}")
            continue

        if source not in {"YTCareers", "X"}:
            if signature and signature in pending_signatures[source]:
                print(f"Skipped duplicate signature already queued: {job['title']} ({source}) | {signature}")
                continue

        job["posted"] = False

        pending[source].append(job)
        pending_ids[source].add(job["id"])

        if normalized_url:
            pending_urls[source].add(normalized_url)

        if signature:
            pending_signatures[source].add(signature)

        added[source] += 1
        print(f"Queued: {job['title']} ({source})")

    return added


def post_next_job_for_source(source: str, pending: Dict[str, List[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
    queue = pending.get(source, [])

    if not queue:
        return None

    job = None

    for candidate in queue:
        if candidate.get("posted"):
            continue

        if is_junk_job(candidate):
            candidate["posted"] = True
            candidate["skipped_reason"] = "junk_job"
            print(f"Skipped old junk queued job: {candidate.get('title')} ({source})")
            continue

        job = candidate
        break

    if not job:
        print(f"No unposted {source} jobs available.")
        return None

    try:
        send_to_discord(job)
    except Exception as e:
        print(f"Discord post failed for {source}, will retry next run: {e}")
        return None

    try:
        enrich_public_email(job)
    except Exception as e:
        print(f"Email enrichment failed for {source}: {e}")

    try:
        send_to_monday(job)
    except Exception as e:
        print(f"Monday create failed for {source}: {e}")

    job["posted"] = True
    job["posted_date"] = str(date.today())

    print(f"Marked as posted: {job.get('title')} ({source})")

    return job


async def main() -> None:
    pending = load_pending()

    print(
        "Pending jobs loaded: "
        f"YTJobs={len(pending['YTJobs'])}, "
        f"Roster={len(pending['Roster'])}, "
        f"YTCareers={len(pending['YTCareers'])}, "
        f"BucketofCrabs={len(pending['BucketofCrabs'])}, "
        f"X={len(pending['X'])}"
    )

    removed = clean_pending_queues(pending)

    print(
        "Cleaned pending jobs: "
        f"YTJobs removed={removed['YTJobs']}, "
        f"Roster removed={removed['Roster']}, "
        f"YTCareers removed={removed['YTCareers']}, "
        f"BucketofCrabs removed={removed['BucketofCrabs']}, "
        f"X removed={removed['X']}"
    )

    jobs = await fetch_jobs()
    queued_count = enqueue_new_jobs(jobs, pending)
    save_pending(pending)

    print(
        "Queued new jobs: "
        f"YTJobs={queued_count['YTJobs']}, "
        f"Roster={queued_count['Roster']}, "
        f"YTCareers={queued_count['YTCareers']}, "
        f"BucketofCrabs={queued_count['BucketofCrabs']}, "
        f"X={queued_count['X']}"
    )

    print(
        "Unposted queue sizes before post: "
        f"YTJobs={count_unposted(pending, 'YTJobs')}, "
        f"Roster={count_unposted(pending, 'Roster')}, "
        f"YTCareers={count_unposted(pending, 'YTCareers')}, "
        f"BucketofCrabs={count_unposted(pending, 'BucketofCrabs')}, "
        f"X={count_unposted(pending, 'X')}"
    )

    posted_ytjobs = post_next_job_for_source("YTJobs", pending)
    posted_roster = post_next_job_for_source("Roster", pending)
    posted_ytcareers = post_next_job_for_source("YTCareers", pending)
    posted_boc = post_next_job_for_source("BucketofCrabs", pending)
    posted_x = post_next_job_for_source("X", pending)

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

    if posted_x:
        print(f"Posted X: {posted_x['title']}")
    else:
        print("No X post sent this run.")

    print(
        "Unposted queue sizes after post: "
        f"YTJobs={count_unposted(pending, 'YTJobs')}, "
        f"Roster={count_unposted(pending, 'Roster')}, "
        f"YTCareers={count_unposted(pending, 'YTCareers')}, "
        f"BucketofCrabs={count_unposted(pending, 'BucketofCrabs')}, "
        f"X={count_unposted(pending, 'X')}"
    )


if __name__ == "__main__":
    asyncio.run(main())
