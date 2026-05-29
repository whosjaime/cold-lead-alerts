import asyncio
import json
import re
from datetime import date
from typing import Any, Dict, List, Optional

import creator_job_alerts as alerts
import creator_job_alerts_referral as referral

MONDAY_COL_LOCATION_TYPE = alerts.os.getenv("MONDAY_COL_LOCATION_TYPE", "")

_original_send_to_monday = alerts.send_to_monday


def detect_location_type(job: Dict[str, Any]) -> str:
    """Remote if the listing says remote. Otherwise Onsite."""
    text = " ".join(
        str(job.get(key, ""))
        for key in ["location", "summary", "title", "job_type", "company", "url"]
    ).lower()

    if re.search(r"\bremote\b", text):
        return "Remote"

    return "Onsite"


def add_location_type_to_job(job: Dict[str, Any]) -> None:
    source = job.get("source")

    if source in {"YTCareers", "BucketofCrabs", "YTJobs"}:
        job["location_type"] = detect_location_type(job)


def monday_create_item_with_location_type(job: Dict[str, Any]) -> None:
    """Create Monday item with Location Type support for YTCareers/BucketofCrabs/YTJobs."""
    source = job.get("source", "Unknown")

    if source not in {"YTCareers", "BucketofCrabs", "YTJobs"}:
        _original_send_to_monday(job)
        return

    if not alerts.MONDAY_API_TOKEN or not alerts.MONDAY_BOARD_ID:
        print("Monday not configured, skipping.")
        return

    add_location_type_to_job(job)

    if source == "YTJobs":
        job["referral_bonus"] = referral.referral_bonus_number(job.get("referral_bonus"))

    role_title = alerts.clip(job.get("title", "New lead"), 255)
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
    location_type = job.get("location_type") or detect_location_type(job)
    post_date = str(date.today())
    numeric_pay = alerts.extract_numeric_pay(pay)
    subscribers = job.get("subscribers")

    column_values: Dict[str, Any] = {}

    if alerts.MONDAY_COL_PAY and numeric_pay is not None:
        column_values[alerts.MONDAY_COL_PAY] = numeric_pay

    if alerts.MONDAY_COL_TYPE and monday_type:
        column_values[alerts.MONDAY_COL_TYPE] = {"labels": [monday_type]}

    if MONDAY_COL_LOCATION_TYPE:
        column_values[MONDAY_COL_LOCATION_TYPE] = {"labels": [location_type]}

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
        column_values[alerts.MONDAY_COL_EMAIL] = {"email": email, "text": email}

    if alerts.MONDAY_COL_SUBSCRIBERS and subscribers is not None:
        column_values[alerts.MONDAY_COL_SUBSCRIBERS] = subscribers

    if source == "YTJobs" and alerts.MONDAY_COL_REFERRAL_BONUS:
        column_values[alerts.MONDAY_COL_REFERRAL_BONUS] = referral.referral_bonus_text(job.get("referral_bonus"))

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


async def post_next_job_for_source_with_location_type(
    source: str,
    pending: Dict[str, List[Dict[str, Any]]],
    page,
) -> Optional[Dict[str, Any]]:
    job = await referral.post_next_job_for_source_with_ytjobs_referral(source, pending, page)

    if job and source in {"YTCareers", "BucketofCrabs", "YTJobs"}:
        add_location_type_to_job(job)

    return job


alerts.send_to_monday = monday_create_item_with_location_type
alerts.post_next_job_for_source = post_next_job_for_source_with_location_type


if __name__ == "__main__":
    asyncio.run(alerts.main())
