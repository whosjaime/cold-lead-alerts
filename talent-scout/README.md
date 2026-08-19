# YTJobs Talent → Monday.com Board Sync (standalone project)

This project crawls YTJobs talent listings, enriches profile data, and syncs new talent into a Monday.com board.

## What it does
- Crawls multiple YTJobs listing pages (not just a single page).
- Extracts talent profiles and profile metadata.
- Flags:
  - `open_for_work`
  - `works_with_big_creators`
- Prioritizes these flags in output and Monday item titles.
- De-duplicates using a local JSON state file.
- Creates/uses a Monday board and adds only new talent entries.

## Setup
```bash
cd talent-scout
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## Required from you
Set these before running:

```bash
export MONDAY_API_TOKEN='your_api_token'
export MONDAY_WORKSPACE_ID='1234567890'  # needed if MONDAY_BOARD_ID is not already set
```

Optional:
```bash
export MONDAY_BOARD_ID='1234567890'       # use existing board
export MONDAY_BOARD_NAME='YTJobs Talent Pipeline'
export MONDAY_GROUP_ID='topics'            # existing group id
export MAX_PAGES='40'                      # crawl depth
export HIGH_VIEWS_THRESHOLD='100000'       # used to flag big creators/high-view talent
export STATE_FILE='pending_talent.json'
export YTJOBS_TALENT_URL='https://ytjobs.co/talent/search/all_categories?page=1'
```

## Run
```bash
python ytjobs_talent_monitor.py
```

## Notes
- If `MONDAY_BOARD_ID` is missing and `MONDAY_WORKSPACE_ID` is present, the script auto-creates a board.
- New talent is appended to Monday only once (tracked in `STATE_FILE`).
- If Playwright Chromium is unavailable in the runtime, the crawler automatically falls back to plain HTTP requests (lower coverage, but still usable).
- High-view talent is prioritized via `HIGH_VIEWS_THRESHOLD` and included in Monday item details.
