import asyncio
import json
from datetime import date
from typing import Any, Dict, List, Optional

import creator_job_alerts as alerts


def referral_bonus_number(value: Any) -> float:
    """Return a clean numeric referral bonus. Missing/non-numeric bonuses become 0."""
    if value is None:
        return 0

    if isinstance(value, (int, float)):
        return float(value)

    text = alerts.clean_text(str(value)).replace("$", "").replace(",", "")

    try:
        return float(text)
    except ValueError:
        return 0


def referral_bonus_text(value: Any) -> str:
    """Return the referral bonus as text for Monday text columns."""
    return f"{referral_bonus_number(value):.0f}"


def format_referral_bonus(value: Any) -> str:
    return f"${referral_bonus_number(value):,.0f}"


_original_send_to_discord = alerts.send_to_discord
_original_send_to_monday = alerts.send_to_monday


def send_to_discord_with_referral_fee(job: Dict[str, Any]) -> None:
    """Always show a YTJobs referral fee line in Discord, using $0 when none exists."""
    if job.get("source") != "YTJobs":
        _original_send_to_discord(job)
        return

    source = job.get("source", "Unknown")
    webhook_url = alerts.get_webhook_url(source)

    if not webhook_url:
        raise RuntimeError(f"Missing webhook URL for source: {source}")

    title = alerts.clip(job.get("title", "New job"), 100)
    company = alerts.clip(job.get("company", "") or "Not listed", 80)
    location = alerts.clip(job.get("location", "Not listed"), 60)
    job_type = alerts.clip(job.get("job_type", "Not listed"), 60)
    pay = alerts.clip(job.get("pay", "Not listed"), 80)
    description = alerts.clip(job.get("summary", "No description listed."), 220)
    url = (job.get("url") or "").strip()
    referral_bonus = format_referral_bonus(job.get("referral_bonus"))

    role_line, allowed_mentions = alerts.build_role_line_and_mentions(title, description)

    content = (
        f"{alerts.HEADER_TEXT}\n\n"
        f"{role_line}\n"
        f"**Company:** {company}\n"
        f"**Source:** {source}\n"
        f"**Type:** {job_type}\n"
        f"**Location:** {location}\n"
        f"**Pay:** {pay}\n"
        f"**Referral Fee:** {referral_bonus}\n"
        f"**Description:** {description}\n"
        f"**Link:** {url if url else 'Not listed'}"
    )

    payload = {
        "username": "Manifest Media Leads",
        "content": content,
        "allowed_mentions": allowed_mentions,
    }

    if alerts.WEBHOOK_AVATAR_URL:
        payload["avatar_url"] = alerts.WEBHOOK_AVATAR_URL

    response = alerts.requests.post(webhook_url, json=payload, timeout=30)
    print(f"Discord response for {source}: {response.status_code}")
    response.raise_for_status()


def send_to_monday_with_referral_bonus(job: Dict[str, Any]) -> None:
    """Send YTJobs to Monday with referral bonus text, defaulting to 0."""
    if job.get("source") != "YTJobs":
        _original_send_to_monday(job)
        return

    if not alerts.MONDAY_API_TOKEN or not alerts.MONDAY_BOARD_ID:
        print("Monday not configured, skipping.")
        return

    job["referral_bonus"] = referral_bonus_number(job.get("referral_bonus"))

    role_title = alerts.clip(job.get("title", "New lead"), 255)
    source = job.get("source", "Unknown")
    job_type = job.get("job_type", "Not listed")
    location = job.get("location", "Not listed")
    pay = job.get("pay", "Not listed")
    description = alerts.clip(job.get("summary", "No description listed."), 1000)
    url = (job.get("url") or "").strip()
    email = alerts.clean_text(job.get("email"))

    company = alerts.monday_company_name(job)
    primary_skill = alerts.map_monday_role_label(job)
    role_position = alerts.map_monday_role_label(job)
    monday_type = alerts.map_monday_type(job_type, pay)
    monday_platform = alerts.map_monday_platform(source)
    monday_sourced_from = alerts.map_monday_sourced_from(source)
    monday_category = alerts.map_monday_category(job)
    monday_location = alerts.map_monday_location(location)
    post_date = str(date.today())
    numeric_pay = alerts.extract_numeric_pay(pay)
    referral_bonus = referral_bonus_text(job.get("referral_bonus"))
    subscribers = job.get("subscribers")

    column_values: Dict[str, Any] = {}

    if alerts.MONDAY_COL_PAY and numeric_pay is not None:
        column_values[alerts.MONDAY_COL_PAY] = numeric_pay

    if alerts.MONDAY_COL_TYPE and monday_type:
        column_values[alerts.MONDAY_COL_TYPE] = {"labels": [monday_type]}

    if alerts.MONDAY_COL_PRIMARY_SKILL and primary_skill:
        column_values[alerts.MONDAY_COL_PRIMARY_SKILL] = {"labels": [primary_skill]}

    if alerts.MONDAY_COL_ROLE and role_position:
        column_values[alerts.MONDAY_COL_ROLE] = {"labels": [role_position]}

    if alerts.MONDAY_COL_LOCATION and monday_location:
        column_values[alerts.MONDAY_COL_LOCATION] = {"labels": [monday_location]}

    if alerts.MONDAY_COL_PLATFORM and monday_platform:
        column_values[alerts.MONDAY_COL_PLATFORM] = {"label": monday_platform}

    if alerts.MONDAY_COL_SOURCED_FROM and monday_sourced_from:
        column_values[alerts.MONDAY_COL_SOURCED_FROM] = {"label": monday_sourced_from}

    if alerts.MONDAY_COL_CATEGORY and monday_category:
        column_values[alerts.MONDAY_COL_CATEGORY] = {"label": monday_category}

    if alerts.MONDAY_COL_COMPANY and company and company != "Unknown":
        column_values[alerts.MONDAY_COL_COMPANY] = company

    if alerts.MONDAY_COL_DESCRIPTION:
        column_values[alerts.MONDAY_COL_DESCRIPTION] = description

    if alerts.MONDAY_COL_LINK and url:
        column_values[alerts.MONDAY_COL_LINK] = {"url": url, "text": "Job post"}

    if alerts.MONDAY_COL_POST_DATE:
        column_values[alerts.MONDAY_COL_POST_DATE] = {"date": post_date}

    if alerts.MONDAY_COL_EMAIL and email:
        column_values[alerts.MONDAY_COL_EMAIL] = {
            "email": email,
            "text": email,
        }

    if alerts.MONDAY_COL_SUBSCRIBERS and subscribers is not None:
        column_values[alerts.MONDAY_COL_SUBSCRIBERS] = subscribers

    if alerts.MONDAY_COL_REFERRAL_BONUS:
        column_values[alerts.MONDAY_COL_REFERRAL_BONUS] = referral_bonus

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
        "board_id": str(alerts.MONDAY_BOARD_ID),
        "group_id": alerts.MONDAY_GROUP_ID or None,
        "item_name": role_title,
        "column_values": json.dumps(column_values),
    }

    print("Monday variables:")
    print(json.dumps(variables, indent=2))

    response = alerts.requests.post(
        "https://api.monday.com/v2",
        headers={
            "Authorization": alerts.MONDAY_API_TOKEN,
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


async def post_next_job_for_source_with_ytjobs_referral(
    source: str,
    pending: Dict[str, List[Dict[str, Any]]],
    page,
) -> Optional[Dict[str, Any]]:
    queue = pending.get(source, [])

    if not queue:
        return None

    job = None

    for candidate in queue:
        if candidate.get("posted"):
            continue

        if alerts.is_junk_job(candidate):
            candidate["posted"] = True
            candidate["skipped_reason"] = "junk_job"
            print(f"Skipped old junk queued job: {candidate.get('title')} ({source})")
            continue

        job = candidate
        break

    if not job:
        print(f"No unposted {source} jobs available.")
        return None

    # Enrich YTJobs before Discord/Monday so referral bonus and subscribers are available.
    if source == "YTJobs":
        try:
            await alerts.enrich_ytjobs_with_page_data(page, job)
        except Exception as e:
            print(f"YTJobs enrichment failed for {source}: {e}")

        # Keep a clean number in Discord and Monday, even when no bonus exists.
        job["referral_bonus"] = referral_bonus_number(job.get("referral_bonus"))

    try:
        alerts.send_to_discord(job)
    except Exception as e:
        print(f"Discord post failed for {source}, will retry next run: {e}")
        return None

    try:
        alerts.enrich_public_email(job)
    except Exception as e:
        print(f"Email enrichment failed for {source}: {e}")

    try:
        alerts.send_to_monday(job)
    except Exception as e:
        print(f"Monday create failed for {source}: {e}")

    job["posted"] = True
    job["posted_date"] = str(date.today())

    print(f"Marked as posted: {job.get('title')} ({source})")

    return job


alerts.send_to_discord = send_to_discord_with_referral_fee
alerts.send_to_monday = send_to_monday_with_referral_bonus
alerts.post_next_job_for_source = post_next_job_for_source_with_ytjobs_referral


if __name__ == "__main__":
    asyncio.run(alerts.main())
