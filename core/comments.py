from datetime import datetime, timezone

import core.db as db


def fetch_and_detect_comments(
    instance_key: str,
    platform,
    resource_type: str,
    resource_id: str,
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
    # Fetch all comments from platform
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
        edited_at = comment.get("updated_at") or comment.get("created_at")

        deleted_ids.discard(comment_id)

        if comment_id not in existing_map:
            # New comment
            new_comments.append(comment)
        else:
            # Check if edited
            existing_edited_at = existing_map[comment_id].get("comment_edited_at")
            if edited_at and existing_edited_at != edited_at:
                # Comment has been edited
                edited_comments.append({
                    **comment,
                    "previously_at": existing_edited_at,
                })
            else:
                # No change
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
    """Mark a comment as being processed (transactional)."""
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
            DO UPDATE SET state = 'processing', last_checked_at = ?
            """,
            (instance_key, resource_type, resource_id, comment_id, edited_at, now, now),
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


def get_unprocessed_comments(
    instance_key: str,
    resource_type: str,
    resource_id: str,
) -> list[dict]:
    """Get all unprocessed comments for a resource."""
    return db.query_all(
        """
        SELECT comment_id, comment_edited_at, state, error_count
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
