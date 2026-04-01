import asyncio
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

YTJOBS_TALENT_URL = os.getenv("YTJOBS_TALENT_URL", "https://ytjobs.co/talent/search/all_categories?page=1")
STATE_FILE = Path(os.getenv("STATE_FILE", "pending_talent.json"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "30"))
HIGH_VIEWS_THRESHOLD = int(os.getenv("HIGH_VIEWS_THRESHOLD", "100000"))

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN", "")
MONDAY_BOARD_ID = os.getenv("MONDAY_BOARD_ID", "")
MONDAY_WORKSPACE_ID = os.getenv("MONDAY_WORKSPACE_ID", "")
MONDAY_BOARD_NAME = os.getenv("MONDAY_BOARD_NAME", "YTJobs Talent Pipeline")
MONDAY_GROUP_ID = os.getenv("MONDAY_GROUP_ID", "")


def clean_text(value: str) -> str:
    return " ".join((value or "").split())


def make_id(*parts: str) -> str:
    base = " | ".join(parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def load_seen() -> Dict[str, Dict[str, Any]]:
    if not STATE_FILE.exists():
        return {}
    try:
        data = json.loads(STATE_FILE.read_text())
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def save_seen(items: Dict[str, Dict[str, Any]]) -> None:
    STATE_FILE.write_text(json.dumps(items, indent=2))


def with_page(url: str, page_num: int) -> str:
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    q["page"] = [str(page_num)]
    new_query = urlencode(q, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def parse_role_position(summary: str) -> Dict[str, str]:
    chunks = [c.strip() for c in summary.split("·") if c.strip()]
    role = chunks[0] if chunks else "Not listed"
    position = chunks[1] if len(chunks) > 1 else "Not listed"
    return {"role": role[:120], "position": position[:120]}


def detect_open_for_work(text: str) -> bool:
    lower = text.lower()
    signals = ["open to work", "open for work", "available for work", "available now"]
    return any(s in lower for s in signals)


def detect_big_creators(text: str) -> bool:
    lower = text.lower()
    patterns = [
        r"worked with\s+\d{3,}\s*(k|m)?\s*subscriber",
        r"worked with big creators",
        r"top creator",
        r"million subscriber",
        r"mrbeast",
        r"sidemen",
    ]
    return any(re.search(p, lower) for p in patterns)


def parse_views_value(text: str) -> int:
    lower = text.lower().replace(",", "")
    matches = re.findall(r"(\d+(?:\.\d+)?)\s*([km]?)\s*(?:views|view|subs|subscribers)", lower)
    best = 0
    for num_str, suffix in matches:
        try:
            value = float(num_str)
        except ValueError:
            continue
        if suffix == "k":
            value *= 1_000
        elif suffix == "m":
            value *= 1_000_000
        best = max(best, int(value))
    return best


def is_high_views(text: str) -> bool:
    return parse_views_value(text) >= HIGH_VIEWS_THRESHOLD


def parse_cards(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict[str, str]] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/talent/" not in href and "/profile/" not in href:
            continue

        profile_url = urljoin("https://ytjobs.co", href)
        card_text = clean_text(a.get_text(" ", strip=True))
        if not card_text:
            continue

        parts = [p.strip() for p in card_text.split("·") if p.strip()]
        name = parts[0] if parts else card_text[:100]
        summary = " · ".join(parts[1:]) if len(parts) > 1 else card_text
        rp = parse_role_position(summary)

        rows.append(
            {
                "name": name[:100],
                "role": rp["role"],
                "position": rp["position"],
                "summary": summary[:500],
                "profile_url": profile_url,
                "open_for_work": "yes" if detect_open_for_work(card_text) else "no",
                "works_with_big_creators": "yes" if detect_big_creators(card_text) or is_high_views(card_text) else "no",
                "views_estimate": str(parse_views_value(card_text)),
            }
        )

    unique: List[Dict[str, str]] = []
    seen_urls = set()
    for row in rows:
        if row["profile_url"] in seen_urls:
            continue
        seen_urls.add(row["profile_url"])
        unique.append(row)
    return unique


async def fetch_html(page, url: str) -> str:
    await page.goto(url, wait_until="networkidle", timeout=60000)
    for _ in range(4):
        await page.mouse.wheel(0, 2200)
        await page.wait_for_timeout(700)
    return await page.content()


async def fetch_profile_signals(page, profile_url: str) -> Dict[str, str]:
    try:
        html = await fetch_html(page, profile_url)
    except Exception:
        return {"open_for_work": "unknown", "works_with_big_creators": "unknown"}

    text = clean_text(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))
    return {
        "open_for_work": "yes" if detect_open_for_work(text) else "no",
        "works_with_big_creators": "yes" if detect_big_creators(text) or is_high_views(text) else "no",
        "views_estimate": str(parse_views_value(text)),
    }


def fetch_html_requests(url: str) -> str:
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            return ""
        return resp.text
    except Exception:
        return ""


def fetch_profile_signals_requests(profile_url: str) -> Dict[str, str]:
    html = fetch_html_requests(profile_url)
    if not html:
        return {"open_for_work": "unknown", "works_with_big_creators": "unknown"}
    text = clean_text(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))
    return {
        "open_for_work": "yes" if detect_open_for_work(text) else "no",
        "works_with_big_creators": "yes" if detect_big_creators(text) or is_high_views(text) else "no",
        "views_estimate": str(parse_views_value(text)),
    }


def crawl_all_pages_requests() -> List[Dict[str, str]]:
    all_rows: List[Dict[str, str]] = []
    seen_profile_urls = set()
    empty_streak = 0

    for page_num in range(1, MAX_PAGES + 1):
        url = with_page(YTJOBS_TALENT_URL, page_num)
        html = fetch_html_requests(url)
        if not html:
            continue

        rows = parse_cards(html)
        new_in_page = 0

        for row in rows:
            if row["profile_url"] in seen_profile_urls:
                continue
            seen_profile_urls.add(row["profile_url"])
            new_in_page += 1

            signals = fetch_profile_signals_requests(row["profile_url"])
            if signals["open_for_work"] != "unknown":
                row["open_for_work"] = signals["open_for_work"]
            if signals["works_with_big_creators"] != "unknown":
                row["works_with_big_creators"] = signals["works_with_big_creators"]
            if signals.get("views_estimate"):
                row["views_estimate"] = signals["views_estimate"]

            all_rows.append(row)

        if new_in_page == 0:
            empty_streak += 1
        else:
            empty_streak = 0

        if empty_streak >= 2:
            break

    return all_rows


async def crawl_all_pages() -> List[Dict[str, str]]:
    all_rows: List[Dict[str, str]] = []
    seen_profile_urls = set()

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            list_page = await browser.new_page()
            profile_page = await browser.new_page()

            empty_streak = 0
            for page_num in range(1, MAX_PAGES + 1):
                url = with_page(YTJOBS_TALENT_URL, page_num)
                try:
                    html = await fetch_html(list_page, url)
                except Exception:
                    continue

                rows = parse_cards(html)
                new_in_page = 0

                for row in rows:
                    if row["profile_url"] in seen_profile_urls:
                        continue
                    seen_profile_urls.add(row["profile_url"])
                    new_in_page += 1

                    signals = await fetch_profile_signals(profile_page, row["profile_url"])
                    if signals["open_for_work"] != "unknown":
                        row["open_for_work"] = signals["open_for_work"]
                    if signals["works_with_big_creators"] != "unknown":
                        row["works_with_big_creators"] = signals["works_with_big_creators"]
                    if signals.get("views_estimate"):
                        row["views_estimate"] = signals["views_estimate"]

                    all_rows.append(row)

                if new_in_page == 0:
                    empty_streak += 1
                else:
                    empty_streak = 0

                if empty_streak >= 2:
                    break

            await browser.close()
            return all_rows
    except Exception:
        return crawl_all_pages_requests()

    return all_rows


def monday_enabled() -> bool:
    return bool(MONDAY_API_TOKEN)


def monday_api(query: str, variables: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    headers = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}
    try:
        resp = requests.post(
            "https://api.monday.com/v2",
            json={"query": query, "variables": variables},
            headers=headers,
            timeout=25,
        )
        if resp.status_code != 200:
            return None
        body = resp.json()
        if "errors" in body:
            return None
        return body.get("data", {})
    except Exception:
        return None


def ensure_board() -> Optional[str]:
    if MONDAY_BOARD_ID:
        return str(MONDAY_BOARD_ID)
    if not MONDAY_WORKSPACE_ID or not monday_enabled():
        return None

    mutation = """
    mutation ($name: String!, $workspace_id: ID!) {
      create_board(board_name: $name, board_kind: public, workspace_id: $workspace_id) {
        id
      }
    }
    """
    data = monday_api(mutation, {"name": MONDAY_BOARD_NAME, "workspace_id": str(MONDAY_WORKSPACE_ID)})
    if not data or "create_board" not in data:
        return None
    return str(data["create_board"]["id"])


def push_to_monday(row: Dict[str, str], board_id: str) -> bool:
    priority_tag = "🔥" if row["open_for_work"] == "yes" or row["works_with_big_creators"] == "yes" else ""
    item_name = f"{priority_tag} {row['name']} — {row['role']}".strip()

    create_item_mutation = """
    mutation ($board_id: ID!, $group_id: String, $item_name: String!) {
      create_item(board_id: $board_id, group_id: $group_id, item_name: $item_name) { id }
    }
    """
    item_data = monday_api(
        create_item_mutation,
        {
            "board_id": str(board_id),
            "group_id": MONDAY_GROUP_ID or None,
            "item_name": item_name,
        },
    )
    if not item_data or "create_item" not in item_data:
        return False

    item_id = str(item_data["create_item"]["id"])
    note = (
        f"Role: {row['role']}\n"
        f"Position: {row['position']}\n"
        f"Open for work: {row['open_for_work']}\n"
        f"Works with big creators: {row['works_with_big_creators']}\n"
        f"Views estimate: {row.get('views_estimate', '0')}\n"
        f"Profile: {row['profile_url']}\n\n"
        f"Summary: {row['summary']}"
    )

    update_mutation = """
    mutation ($item_id: ID!, $body: String!) {
      create_update(item_id: $item_id, body: $body) { id }
    }
    """
    update_data = monday_api(update_mutation, {"item_id": item_id, "body": note})
    return bool(update_data and "create_update" in update_data)


async def main() -> None:
    rows = await crawl_all_pages()
    seen = load_seen()

    # Prioritize interesting talent first in Monday insert order.
    rows.sort(
        key=lambda r: (
            r["open_for_work"] != "yes",
            r["works_with_big_creators"] != "yes",
            -int(r.get("views_estimate", "0") or "0"),
        )
    )

    new_rows: List[Dict[str, str]] = []
    for row in rows:
        row_id = make_id(row["profile_url"], row["name"])
        if row_id in seen:
            continue
        new_rows.append(row)

    board_id = ensure_board()
    monday_added = 0
    for row in new_rows:
        row_id = make_id(row["profile_url"], row["name"])
        monday_ok = False
        if board_id and monday_enabled():
            monday_ok = push_to_monday(row, board_id)
            if monday_ok:
                monday_added += 1
        seen[row_id] = {**row, "monday_synced": monday_ok, "board_id": board_id or ""}

    save_seen(seen)

    open_count = sum(1 for r in new_rows if r["open_for_work"] == "yes")
    big_count = sum(1 for r in new_rows if r["works_with_big_creators"] == "yes")
    high_views_count = sum(1 for r in new_rows if int(r.get("views_estimate", "0") or "0") >= HIGH_VIEWS_THRESHOLD)

    print(
        f"Scanned total profiles: {len(rows)} | New: {len(new_rows)} | "
        f"Open for work: {open_count} | Big creators/high views: {big_count} | "
        f"High views (>= {HIGH_VIEWS_THRESHOLD}): {high_views_count} | "
        f"Monday added: {monday_added}"
    )


if __name__ == "__main__":
    asyncio.run(main())
