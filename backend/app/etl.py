"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    - Uses httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Passes HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Returns the parsed list of dicts
    - Raises an exception if the response status is not 200
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    - Uses httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Passes HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handles pagination: keeps fetching while has_more is True
      - Uses the submitted_at of the last log as the new "since" value
    - Returns the combined list of all log dicts from all pages
    """
    all_logs: list[dict] = []
    current_since = since

    async with httpx.AsyncClient() as client:
        while True:
            params = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                params=params,
                auth=(settings.autochecker_email, settings.autochecker_password),
            )
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            # Update since to the last log's submitted_at for next page
            if logs:
                last_log = logs[-1]
                current_since = datetime.fromisoformat(
                    last_log["submitted_at"].replace("Z", "+00:00")
                )

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    - Imports ItemRecord from app.models.item
    - Processes labs first (items where type="lab"):
      - For each lab, checks if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERTs a new ItemRecord(type="lab", title=lab_title)
      - Builds a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then processes tasks (items where type="task"):
      - Finds the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict built above
      - Checks if a task with this title and parent_id already exists
      - If not, INSERTs a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commits after all inserts
    - Returns the number of newly created items
    """
    from sqlmodel import select
    from app.models.item import ItemRecord

    new_count = 0
    lab_id_map: dict[str, ItemRecord] = {}

    # Process labs first
    for item in items:
        if item.get("type") != "lab":
            continue

        title = item.get("title", "")
        lab_short_id = item.get("lab", "")

        # Check if lab already exists
        existing = (await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab", ItemRecord.title == title
            )
        )).first()

        if existing is None:
            lab_record = ItemRecord(type="lab", title=title)
            session.add(lab_record)
            await session.flush()  # Get the ID
            new_count += 1
        else:
            lab_record = existing

        # Map short ID to the record for task lookup
        lab_id_map[lab_short_id] = lab_record

    # Process tasks
    for item in items:
        if item.get("type") != "task":
            continue

        title = item.get("title", "")
        lab_short_id = item.get("lab", "")

        # Find parent lab
        parent_lab = lab_id_map.get(lab_short_id)
        if parent_lab is None:
            continue  # Skip if parent lab not found

        # Check if task already exists
        existing = (await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == title,
                ItemRecord.parent_id == parent_lab.id,
            )
        )).first()

        if existing is None:
            task_record = ItemRecord(
                type="task", title=title, parent_id=parent_lab.id
            )
            session.add(task_record)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    - Imports Learner from app.models.learner
    - Imports InteractionLog from app.models.interaction
    - Imports ItemRecord from app.models.item
    - Builds a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Finds or creates a Learner by external_id (log["student_id"])
         - If creating, sets student_group from log["group"]
      2. Finds the matching item in the database:
         - Uses the lookup to get the title for (log["lab"], log["task"])
         - Queries the DB for an ItemRecord with that title
         - Skips this log if no matching item is found
      3. Checks if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Creates InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commits after all inserts
    - Returns the number of newly created interactions
    """
    from sqlmodel import select
    from app.models.learner import Learner
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord

    new_count = 0

    # Build lookup: (lab_short_id, task_short_id) -> item title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_short_id = item.get("lab", "")
        task_short_id = item.get("task")  # Can be None for labs
        title = item.get("title", "")
        item_title_lookup[(lab_short_id, task_short_id)] = title

    for log in logs:
        # 1. Find or create learner
        student_id = log.get("student_id", "")
        group = log.get("group", "")

        learner = (await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        )).first()

        if learner is None:
            learner = Learner(external_id=student_id, student_group=group)
            session.add(learner)
            await session.flush()

        # 2. Find matching item
        lab_short_id = log.get("lab", "")
        task_short_id = log.get("task")  # Can be None
        item_title = item_title_lookup.get((lab_short_id, task_short_id))

        if item_title is None:
            continue  # Skip if no matching item found

        item_record = (await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )).first()

        if item_record is None:
            continue  # Skip if item not in DB

        # 3. Check for existing interaction (idempotency)
        log_external_id = log.get("id")
        existing_interaction = (await session.exec(
            select(InteractionLog).where(
                InteractionLog.external_id == log_external_id
            )
        )).first()

        if existing_interaction is not None:
            continue  # Skip if already exists

        # 4. Create new interaction log
        submitted_at_str = log.get("submitted_at", "")
        created_at = datetime.fromisoformat(
            submitted_at_str.replace("Z", "+00:00")
        )

        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item_record.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at,
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    - Step 1: Fetches items from the API (keeps the raw list) and loads them
      into the database
    - Step 2: Determines the last synced timestamp
      - Queries the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetches logs since that timestamp and loads them
      - Passes the raw items list to load_logs so it can map short IDs
        to titles
    - Returns a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    from sqlmodel import select, func
    from app.models.interaction import InteractionLog

    # Step 1: Fetch and load items
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Get last synced timestamp
    result = (await session.exec(
        select(InteractionLog.created_at)
        .order_by(InteractionLog.created_at.desc())
        .limit(1)
    )).first()
    since = result  # Most recent created_at, or None if no records

    # Step 3: Fetch and load logs
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items, session)

    # Get total count
    total_result = (await session.exec(
        select(func.count(InteractionLog.id))
    )).first()
    total_records = total_result if total_result else 0

    return {"new_records": new_records, "total_records": total_records}
