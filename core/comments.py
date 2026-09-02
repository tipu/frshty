from datetime import datetime, timezone

import core.db as db


def fetch_and_detect_comments(
    instance_key: str,
    platform,
    resource_type: str,
    resource_id: str,
    platform_comments: list | None = None,
) -> dict:
    """
    Fetch comments from platform and detect new, edited, and deleted comments.

    Args:
        instance_key: Instance identifier
        platform: Platform adapter with get_*_comments() method
        resource_type: 'pr' or 'ticket'
        resource_id: PR key (e.g., 'repo/123') or ticket key

    Returns:
        {
            "new": [{"id": "...", "body": "...", "author_id": "...", "created_at": "...", ...}],
            "edited": [{"id": "...", "body": "...", "edited_at": "...", "previously_at": "..."}],
            "deleted": ["comment_id_1", "comment_id_2"],
            "unchanged_count": 10
        }
    """
    # Fetch all comments from platform (unless caller already fetched them)
    if platform_comments is None:
        if resource_type == "pr":
            platform_comments = platform.get_pr_comments(resource_id.split("/")[0], int(resource_id.split("/")[1]))
        elif resource_type == "ticket":
            platform_comments = platform.get_ticket_comments(resource_id)
        else:
            raise ValueError(f"Unknown resource_type: {resource_type}")

    # Get current state from database
    existing = db.query_all(
        "SELECT comment_id, comment_edited_at, state FROM comment_state "
        "WHERE instance_key = ? AND resource_type = ? AND resource_id = ?",
        (instance_key, resource_type, resource_id),
    )
    existing_map = {row["comment_id"]: row for row in existing}

    # Categorize comments
    new_comments = []
    edited_comments = []
    deleted_ids = set(existing_map.keys())
    unchanged_count = 0

    for comment in platform_comments:
        comment_id = str(comment["id"])
        # Use updated_at if available (comment was edited), otherwise created_at
        current_timestamp = comment.get("updated_at") or comment.get("created_at")

        deleted_ids.discard(comment_id)

        if comment_id not in existing_map:
            # New comment
            new_comments.append(comment)
        else:
            # Check if edited (different timestamp than what we have stored)
            existing_timestamp = existing_map[comment_id].get("comment_edited_at")
            existing_state = existing_map[comment_id].get("state")

            # If timestamps match and comment was previously processed, it's unchanged
            if current_timestamp == existing_timestamp and existing_state == "processed":
                unchanged_count += 1
            # If timestamp is different, it's been edited
            elif current_timestamp != existing_timestamp:
                edited_comments.append({
                    **comment,
                    "previously_at": existing_timestamp,
                })
            else:
                # No change (new state or in-progress state but same timestamp)
                unchanged_count += 1

    # Remaining in deleted_ids are deleted comments
    deleted_list = list(deleted_ids)

    return {
        "new": new_comments,
        "edited": edited_comments,
        "deleted": deleted_list,
        "unchanged_count": unchanged_count,
    }


def mark_comment_processing(
    instance_key: str,
    resource_type: str,
    resource_id: str,
    comment_id: str,
    edited_at: str | None = None,
) -> None:
    """Mark a comment as being processed (transactional).

    Args:
        edited_at: Timestamp when comment was last modified (updated_at or created_at from platform)
    """
    now = datetime.now(timezone.utc).isoformat()

    with db.tx() as conn:
        # Insert or update to processing state
        conn.execute(
            """
            INSERT INTO comment_state (
                instance_key, resource_type, resource_id, comment_id,
                comment_edited_at, last_checked_at, state
            )
            VALUES (?, ?, ?, ?, ?, ?, 'processing')
            ON CONFLICT(instance_key, resource_type, resource_id, comment_id)
            DO UPDATE SET state = 'processing', last_checked_at = ?, comment_edited_at = ?
            """,
            (instance_key, resource_type, resource_id, comment_id, edited_at, now, now, edited_at),
        )


def mark_comment_deferred(
    instance_key: str,
    resource_type: str,
    resource_id: str,
    comment_id: str,
    edited_at: str | None = None,
) -> None:
    """Park a comment in the debounce pool awaiting a batched fix."""
    now = datetime.now(timezone.utc).isoformat()

    with db.tx() as conn:
        conn.execute(
            """
            INSERT INTO comment_state (
                instance_key, resource_type, resource_id, comment_id,
                comment_edited_at, last_checked_at, state
            )
            VALUES (?, ?, ?, ?, ?, ?, 'deferred')
            ON CONFLICT(instance_key, resource_type, resource_id, comment_id)
            DO UPDATE SET state = 'deferred', last_checked_at = ?, comment_edited_at = ?
            """,
            (instance_key, resource_type, resource_id, comment_id, edited_at, now, now, edited_at),
        )


def get_deferred_comments(
    instance_key: str,
    resource_type: str,
    resource_id: str,
) -> list[dict]:
    """Get all comments parked in the debounce pool for a resource."""
    return db.query_all(
        """
        SELECT comment_id, comment_edited_at, state, error_count, last_checked_at
        FROM comment_state
        WHERE instance_key = ? AND resource_type = ? AND resource_id = ?
        AND state = 'deferred'
        ORDER BY comment_edited_at ASC
        """,
        (instance_key, resource_type, resource_id),
    )


def mark_comment_processed(
    instance_key: str,
    resource_type: str,
    resource_id: str,
    comment_id: str,
) -> None:
    """Mark a comment as successfully processed."""
    now = datetime.now(timezone.utc).isoformat()

    db.execute(
        """
        UPDATE comment_state
        SET state = 'processed', processed_at = ?, error_count = 0, last_error = NULL
        WHERE instance_key = ? AND resource_type = ? AND resource_id = ? AND comment_id = ?
        """,
        (now, instance_key, resource_type, resource_id, comment_id),
    )


def mark_comment_error(
    instance_key: str,
    resource_type: str,
    resource_id: str,
    comment_id: str,
    error: str,
) -> None:
    """Track an error processing a comment (for retry logic)."""
    db.execute(
        """
        UPDATE comment_state
        SET state = 'new', error_count = error_count + 1, last_error = ?
        WHERE instance_key = ? AND resource_type = ? AND resource_id = ? AND comment_id = ?
        """,
        (error, instance_key, resource_type, resource_id, comment_id),
    )


def mark_comment_retryable(
    instance_key: str,
    resource_type: str,
    resource_id: str,
    comment_id: str,
    error: str,
) -> None:
    """Send a comment back for another pass without spending its retry budget.

    error_count decides when frshty stops trying a comment. Only a failure the
    comment itself caused belongs there. An LLM that never answered — a guard
    block, a timeout, a provider error — says nothing about the comment, and
    charging it exhausts the budget during one outage and abandons the comment
    for good."""
    db.execute(
        """
        UPDATE comment_state
        SET state = 'new', last_error = ?
        WHERE instance_key = ? AND resource_type = ? AND resource_id = ? AND comment_id = ?
        """,
        (error, instance_key, resource_type, resource_id, comment_id),
    )


def mark_comment_deleted(
    instance_key: str,
    resource_type: str,
    resource_id: str,
    comment_id: str,
) -> None:
    """Mark a comment as deleted in the platform."""
    now = datetime.now(timezone.utc).isoformat()

    db.execute(
        """
        UPDATE comment_state
        SET state = 'deleted', processed_at = ?
        WHERE instance_key = ? AND resource_type = ? AND resource_id = ? AND comment_id = ?
        """,
        (now, instance_key, resource_type, resource_id, comment_id),
    )


def has_comment_state(
    instance_key: str,
    resource_type: str,
    resource_id: str,
) -> bool:
    """True if any comment for this resource has been seen before."""
    rows = db.query_all(
        "SELECT 1 FROM comment_state "
        "WHERE instance_key = ? AND resource_type = ? AND resource_id = ? LIMIT 1",
        (instance_key, resource_type, resource_id),
    )
    return bool(rows)


def mark_comment_seen(
    instance_key: str,
    resource_type: str,
    resource_id: str,
    comment_id: str,
    edited_at: str | None = None,
) -> None:
    """Baseline a pre-existing comment as processed without acting on it."""
    now = datetime.now(timezone.utc).isoformat()

    with db.tx() as conn:
        conn.execute(
            """
            INSERT INTO comment_state (
                instance_key, resource_type, resource_id, comment_id,
                comment_edited_at, last_checked_at, state, processed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'processed', ?)
            ON CONFLICT(instance_key, resource_type, resource_id, comment_id)
            DO UPDATE SET state = 'processed', last_checked_at = ?, comment_edited_at = ?, processed_at = ?
            """,
            (instance_key, resource_type, resource_id, comment_id, edited_at, now, now, now, edited_at, now),
        )


def settled_comment_ids(
    instance_key: str,
    resource_type: str,
    resource_id: str,
) -> set[str]:
    """Ids frshty is finished with: processed, or gone from the platform.

    Everything else — never recorded, new, deferred, processing — is still
    owed an answer."""
    rows = db.query_all(
        """
        SELECT comment_id
        FROM comment_state
        WHERE instance_key = ? AND resource_type = ? AND resource_id = ?
        AND state IN ('processed', 'deleted')
        """,
        (instance_key, resource_type, resource_id),
    )
    return {str(row["comment_id"]) for row in rows}


def get_unprocessed_comments(
    instance_key: str,
    resource_type: str,
    resource_id: str,
) -> list[dict]:
    """Get all unprocessed comments for a resource."""
    return db.query_all(
        """
        SELECT comment_id, comment_edited_at, state, error_count, last_checked_at
        FROM comment_state
        WHERE instance_key = ? AND resource_type = ? AND resource_id = ?
        AND state IN ('new', 'processing')
        ORDER BY error_count DESC, comment_edited_at ASC
        """,
        (instance_key, resource_type, resource_id),
    )


def trigger_resource_recheck(
    instance_key: str,
    resource_type: str,
    resource_id: str,
) -> None:
    """
    Queue a job to recheck comments for a resource.
    Used when manually triggering a resource or retrying after errors.
    """
    import core.queue as q

    task_name = f"recheck_comments_{resource_type}"
    q.enqueue_job(
        instance_key,
        task_name,
        payload={
            "resource_type": resource_type,
            "resource_id": resource_id,
        },
    )
