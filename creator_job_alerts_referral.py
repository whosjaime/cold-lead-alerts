import asyncio
from datetime import date
from typing import Any, Dict, List, Optional

import creator_job_alerts as alerts


def format_referral_bonus(value: Any) -> str:
    try:
        return f"${float(value):,.0f}"
    except (TypeError, ValueError):
        return str(value)


_original_send_to_discord = alerts.send_to_discord


def send_to_discord_with_referral_fee(job: Dict[str, Any]) -> None:
    """Add YTJobs referral fee to the Discord alert without changing Monday values."""
    if job.get("source") != "YTJobs" or job.get("referral_bonus") is None:
        _original_send_to_discord(job)
        return

    discord_job = dict(job)
    referral_bonus = format_referral_bonus(job.get("referral_bonus"))
    pay = alerts.clean_text(discord_job.get("pay", "Not listed")) or "Not listed"
    discord_job["pay"] = f"{pay}\n**Referral Fee:** {referral_bonus}"

    _original_send_to_discord(discord_job)


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
alerts.post_next_job_for_source = post_next_job_for_source_with_ytjobs_referral


if __name__ == "__main__":
    asyncio.run(alerts.main())
