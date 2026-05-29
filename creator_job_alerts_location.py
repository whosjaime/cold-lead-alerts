import asyncio
import json
import re
from datetime import date
from typing import Any, Dict, List, Optional

import creator_job_alerts as alerts
import creator_job_alerts_referral as referral

_original_send_to_discord = alerts.send_to_discord
_original_send_to_monday = alerts.send_to_monday


def detect_location_label(job: Dict[str, Any]) -> str:
    """Use the existing Monday Location field: Remote if the post says remote, otherwise On-site."""
    text = " ".join(
        str(job.get(key, ""))
        for key in ["location", "summary", "title", "job_type", "company", "url", "detail_text"]
    ).lower()

    if re.search(r"\bremote\b", text):
        return "Remote"

    return "On-site"


def normalize_location_for_monday(job: Dict[str, Any]) -> None:
    source = job.get("source")

    if source in {"YTCareers", "BucketofCrabs", "YTJobs"}:
        job["location"] = detect_location_label(job)


def enrich_bucketofcrabs_from_detail_page(job: Dict[str, Any]) -> None:
    """Fetch the BucketofCrabs detail page so Remote/On-site is based on the full post, not just feed text."""
    if job.get("source") != "BucketofCrabs":
        return

    url = (job.get("url") or "").strip()
    if not url:
        return

    try:
        response = alerts.requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()

        soup = alerts.BeautifulSoup(response.text, "html.parser")
        page_text = alerts.clean_text(soup.get_text(" "))

        if page_text:
            job["detail_text"] = page_text

            if job.get("summary") in {None, "", "No description listed."}:
                job["summary"] = alerts.build_description(page_text, job.get("title", "BucketofCrabs lead"))

            extracted_location = alerts.extract_location(page_text)
            if extracted_location and extracted_location != "Not listed":
                job["location"] = extracted_location

            extracted_type = alerts.extract_job_type(page_text)
            if extracted_type and extracted_type != "Not listed":
                job["job_type"] = extracted_type

            extracted_pay = alerts.extract_pay(page_text)
            if extracted_pay and extracted_pay != "Not listed":
                job["pay"] = extracted_pay

        normalize_location_for_monday(job)

        print(
            "Enriched BucketofCrabs: "
            f"{job.get('title')} | {job.get('location')} | "
            f"{alerts.clip(job.get('summary', ''), 120)}"
        )

    except Exception as e:
        print(f"BucketofCrabs detail enrichment failed for {job.get('title')}: {e}")
        normalize_location_for_monday(job)


def send_to_discord_with_bucketofcrabs_enrichment(job: Dict[str, Any]) -> None:
    if job.get("source") == "BucketofCrabs":
        enrich_bucketofcrabs_from_detail_page(job)

    _original_send_to_discord(job)


def monday_create_item_with_existing_location(job: Dict[str, Any]) -> None:
    """Create Monday item using the existing MONDAY_COL_LOCATION mapping."""
    source = job.get("source", "Unknown")

    if source not in {"YTCareers", "BucketofCrabs", "YTJobs"}:
        _original_send_to_monday(job)
        return

    if not alerts.MONDAY_API_TOKEN or not alerts.MONDAY_BOARD_ID:
        print("Monday not configured, skipping.")
        return

    if source == "BucketofCrabs" and not job.get("detail_text"):
        enrich_bucketofcrabs_from_detail_page(job)

    normalize_location_for_monday(job)

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
    post_date = str(date.today())
    numeric_pay = alerts.extract_numeric_pay(pay)
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


async def post_next_job_for_source_with_existing_location(
    source: str,
    pending: Dict[str, List[Dict[str, Any]]],
    page,
) -> Optional[Dict[str, Any]]:
    job = await referral.post_next_job_for_source_with_ytjobs_referral(source, pending, page)

    if job and source in {"YTCareers", "BucketofCrabs", "YTJobs"}:
        normalize_location_for_monday(job)

    return job


alerts.send_to_discord = send_to_discord_with_bucketofcrabs_enrichment
alerts.send_to_monday = monday_create_item_with_existing_location
alerts.post_next_job_for_source = post_next_job_for_source_with_existing_location


if __name__ == "__main__":
    asyncio.run(alerts.main())
