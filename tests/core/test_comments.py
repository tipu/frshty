"""Tests for stateful comment tracking system (core/comments.py)."""

import pytest
from datetime import datetime, timezone
from pathlib import Path

import core.db as db
import core.comments as comments


class MockPlatform:
    """Mock platform adapter for testing."""

    def __init__(self, pr_comments=None, ticket_comments=None):
        self.pr_comments = pr_comments or []
        self.ticket_comments = ticket_comments or []

    def get_pr_comments(self, repo, pr_id):
        return self.pr_comments

    def get_ticket_comments(self, ticket_key):
        return self.ticket_comments


@pytest.fixture
def instance_key():
    return "test_instance"


@pytest.fixture
def resource_type():
    return "pr"


@pytest.fixture
def resource_id():
    return "backend/123"


class TestDetectNewComments:
    """Test detection of new comments."""

    def test_empty_db_all_comments_are_new(self, instance_key, resource_type, resource_id):
        """First check should find all comments as new."""
        comment_list = [
            {
                "id": "c1",
                "body": "First comment",
                "author_id": "user1",
                "created_at": "2026-04-28T10:00:00Z",
                "updated_at": None,
            },
            {
                "id": "c2",
                "body": "Second comment",
                "author_id": "user2",
                "created_at": "2026-04-28T11:00:00Z",
                "updated_at": None,
            },
        ]
        platform = MockPlatform(pr_comments=comment_list)

        detection = comments.fetch_and_detect_comments(
            instance_key, platform, resource_type, resource_id
        )

        assert len(detection["new"]) == 2
        assert len(detection["edited"]) == 0
        assert len(detection["deleted"]) == 0
        assert detection["unchanged_count"] == 0

    def test_single_new_comment(self, instance_key, resource_type, resource_id):
        """Detect single new comment among processed ones."""
        # First, process two comments
        platform1 = MockPlatform(
            pr_comments=[
                {"id": "c1", "body": "First", "author_id": "u1", "created_at": "2026-04-28T10:00:00Z", "updated_at": None},
                {"id": "c2", "body": "Second", "author_id": "u2", "created_at": "2026-04-28T11:00:00Z", "updated_at": None},
            ]
        )
        comments.fetch_and_detect_comments(instance_key, platform1, resource_type, resource_id)
        for c in ["c1", "c2"]:
            comments.mark_comment_processing(instance_key, resource_type, resource_id, c, "2026-04-28T10:00:00Z")
            comments.mark_comment_processed(instance_key, resource_type, resource_id, c)

        # Now add a third comment
        platform2 = MockPlatform(
            pr_comments=[
                {"id": "c1", "body": "First", "author_id": "u1", "created_at": "2026-04-28T10:00:00Z", "updated_at": None},
                {"id": "c2", "body": "Second", "author_id": "u2", "created_at": "2026-04-28T11:00:00Z", "updated_at": None},
                {"id": "c3", "body": "Third", "author_id": "u3", "created_at": "2026-04-28T12:00:00Z", "updated_at": None},
            ]
        )
        detection = comments.fetch_and_detect_comments(
            instance_key, platform2, resource_type, resource_id
        )

        assert len(detection["new"]) == 1
        assert detection["new"][0]["id"] == "c3"
        assert len(detection["edited"]) == 0
        assert detection["unchanged_count"] == 2


class TestDetectEditedComments:
    """Test detection of edited comments."""

    def test_edited_comment_detection(self, instance_key, resource_type, resource_id):
        """Detect when a comment is edited (timestamp changes)."""
        # Process original comment
        platform1 = MockPlatform(
            pr_comments=[
                {
                    "id": "c1",
                    "body": "Original text",
                    "author_id": "u1",
                    "created_at": "2026-04-28T10:00:00Z",
                    "updated_at": None,
                }
            ]
        )
        comments.fetch_and_detect_comments(instance_key, platform1, resource_type, resource_id)
        comments.mark_comment_processing(instance_key, resource_type, resource_id, "c1", "2026-04-28T10:00:00Z")
        comments.mark_comment_processed(instance_key, resource_type, resource_id, "c1")

        # Re-check with edited comment
        platform2 = MockPlatform(
            pr_comments=[
                {
                    "id": "c1",
                    "body": "Edited text",
                    "author_id": "u1",
                    "created_at": "2026-04-28T10:00:00Z",
                    "updated_at": "2026-04-28T15:00:00Z",
                }
            ]
        )
        detection = comments.fetch_and_detect_comments(
            instance_key, platform2, resource_type, resource_id
        )

        assert len(detection["new"]) == 0
        assert len(detection["edited"]) == 1
        assert detection["edited"][0]["id"] == "c1"
        assert detection["edited"][0]["previously_at"] == "2026-04-28T10:00:00Z"
        assert detection["unchanged_count"] == 0

    def test_no_edit_when_timestamp_unchanged(self, instance_key, resource_type, resource_id):
        """Comment with same timestamp is not detected as edited."""
        # Process original
        platform1 = MockPlatform(
            pr_comments=[
                {
                    "id": "c1",
                    "body": "Text",
                    "author_id": "u1",
                    "created_at": "2026-04-28T10:00:00Z",
                    "updated_at": None,
                }
            ]
        )
        comments.fetch_and_detect_comments(instance_key, platform1, resource_type, resource_id)
        comments.mark_comment_processing(instance_key, resource_type, resource_id, "c1", "2026-04-28T10:00:00Z")
        comments.mark_comment_processed(instance_key, resource_type, resource_id, "c1")

        # Re-check with same timestamp (body may have changed but timestamp is same)
        platform2 = MockPlatform(
            pr_comments=[
                {
                    "id": "c1",
                    "body": "Different body",  # Changed but timestamp same
                    "author_id": "u1",
                    "created_at": "2026-04-28T10:00:00Z",
                    "updated_at": None,
                }
            ]
        )
        detection = comments.fetch_and_detect_comments(
            instance_key, platform2, resource_type, resource_id
        )

        assert len(detection["new"]) == 0
        assert len(detection["edited"]) == 0
        assert detection["unchanged_count"] == 1


class TestIdempotency:
    """Test idempotent re-processing of comments."""

    def test_processing_twice_is_noop(self, instance_key, resource_type, resource_id):
        """Processing same (id, timestamp) twice should detect second as unchanged."""
        platform = MockPlatform(
            pr_comments=[
                {
                    "id": "c1",
                    "body": "Text",
                    "author_id": "u1",
                    "created_at": "2026-04-28T10:00:00Z",
                    "updated_at": None,
                }
            ]
        )

        # First check: should be new
        detection1 = comments.fetch_and_detect_comments(
            instance_key, platform, resource_type, resource_id
        )
        assert len(detection1["new"]) == 1

        # Mark as processed
        comments.mark_comment_processing(instance_key, resource_type, resource_id, "c1", "2026-04-28T10:00:00Z")
        comments.mark_comment_processed(instance_key, resource_type, resource_id, "c1")

        # Second check: should be unchanged
        detection2 = comments.fetch_and_detect_comments(
            instance_key, platform, resource_type, resource_id
        )
        assert len(detection2["new"]) == 0
        assert len(detection2["edited"]) == 0
        assert detection2["unchanged_count"] == 1

        # Third check: should still be unchanged (idempotency)
        detection3 = comments.fetch_and_detect_comments(
            instance_key, platform, resource_type, resource_id
        )
        assert len(detection3["new"]) == 0
        assert len(detection3["edited"]) == 0
        assert detection3["unchanged_count"] == 1


class TestDeletedComments:
    """Test handling of deleted comments."""

    def test_mark_comment_deleted(self, instance_key, resource_type, resource_id):
        """Deleted comments should be marked with state='deleted'."""
        comments.mark_comment_processing(instance_key, resource_type, resource_id, "c1", "2026-04-28T10:00:00Z")
        comments.mark_comment_deleted(instance_key, resource_type, resource_id, "c1")

        row = db.query_one(
            "SELECT state, processed_at FROM comment_state WHERE comment_id = ?",
            ("c1",),
        )
        assert row["state"] == "deleted"
        assert row["processed_at"] is not None

    def test_deleted_comment_not_in_deleted_list(self, instance_key, resource_type, resource_id):
        """When platform still returns comment, it's not in 'deleted' list."""
        # First, process a comment
        comments.mark_comment_processing(instance_key, resource_type, resource_id, "c1", "2026-04-28T10:00:00Z")
        comments.mark_comment_processed(instance_key, resource_type, resource_id, "c1")

        # Platform still returns it
        platform = MockPlatform(
            pr_comments=[
                {
                    "id": "c1",
                    "body": "Text",
                    "author_id": "u1",
                    "created_at": "2026-04-28T10:00:00Z",
                    "updated_at": None,
                }
            ]
        )
        detection = comments.fetch_and_detect_comments(
            instance_key, platform, resource_type, resource_id
        )

        assert len(detection["deleted"]) == 0


class TestErrorHandling:
    """Test error tracking and retry logic."""

    def test_mark_error_increments_count(self, instance_key, resource_type, resource_id):
        """Errors should increment error_count."""
        comments.mark_comment_processing(instance_key, resource_type, resource_id, "c1", "2026-04-28T10:00:00Z")
        comments.mark_comment_error(instance_key, resource_type, resource_id, "c1", "Test error 1")

        row = db.query_one(
            "SELECT error_count, last_error, state FROM comment_state WHERE comment_id = ?",
            ("c1",),
        )
        assert row["error_count"] == 1
        assert row["last_error"] == "Test error 1"
        assert row["state"] == "new"  # Reverted for retry

    def test_multiple_errors_increment(self, instance_key, resource_type, resource_id):
        """Multiple errors should increment count."""
        comments.mark_comment_processing(instance_key, resource_type, resource_id, "c1", "2026-04-28T10:00:00Z")
        comments.mark_comment_error(instance_key, resource_type, resource_id, "c1", "Error 1")
        comments.mark_comment_error(instance_key, resource_type, resource_id, "c1", "Error 2")

        row = db.query_one(
            "SELECT error_count FROM comment_state WHERE comment_id = ?",
            ("c1",),
        )
        assert row["error_count"] == 2

    def test_successful_processing_clears_errors(self, instance_key, resource_type, resource_id):
        """Successful processing should clear error count."""
        comments.mark_comment_processing(instance_key, resource_type, resource_id, "c1", "2026-04-28T10:00:00Z")
        comments.mark_comment_error(instance_key, resource_type, resource_id, "c1", "Error")
        comments.mark_comment_processing(instance_key, resource_type, resource_id, "c1", "2026-04-28T10:00:00Z")
        comments.mark_comment_processed(instance_key, resource_type, resource_id, "c1")

        row = db.query_one(
            "SELECT error_count, last_error FROM comment_state WHERE comment_id = ?",
            ("c1",),
        )
        assert row["error_count"] == 0
        assert row["last_error"] is None


class TestGetUnprocessed:
    """Test querying unprocessed comments."""

    def test_get_unprocessed_returns_new_and_processing(self, instance_key, resource_type, resource_id):
        """get_unprocessed_comments should return 'new' and 'processing' states."""
        # Insert comments in various states
        for i, state in enumerate(["new", "processing", "processed", "deleted"]):
            comments.mark_comment_processing(instance_key, resource_type, resource_id, f"c{i}", "2026-04-28T10:00:00Z")
            if state == "processed":
                comments.mark_comment_processed(instance_key, resource_type, resource_id, f"c{i}")
            elif state == "deleted":
                comments.mark_comment_deleted(instance_key, resource_type, resource_id, f"c{i}")

        unprocessed = comments.get_unprocessed_comments(instance_key, resource_type, resource_id)

        # Should have c0 (new) and c1 (processing), but not c2 (processed) or c3 (deleted)
        unprocessed_ids = {c["comment_id"] for c in unprocessed}
        assert unprocessed_ids == {"c0", "c1"}

    def test_get_unprocessed_sorted_by_error_count(self, instance_key, resource_type, resource_id):
        """Unprocessed should be sorted by error_count descending."""
        # Create comments with different error counts
        for i in range(3):
            comments.mark_comment_processing(instance_key, resource_type, resource_id, f"c{i}", "2026-04-28T10:00:00Z")
            for _ in range(i):
                comments.mark_comment_error(instance_key, resource_type, resource_id, f"c{i}", "Error")

        unprocessed = comments.get_unprocessed_comments(instance_key, resource_type, resource_id)

        # Should be ordered by error count: c2 (2), c1 (1), c0 (0)
        assert [c["comment_id"] for c in unprocessed] == ["c2", "c1", "c0"]


class TestMultipleResources:
    """Test isolation between different resources."""

    def test_comments_isolated_by_resource(self, instance_key, resource_type):
        """Comments for different resources should be isolated."""
        # Process comment for resource 1
        comments.mark_comment_processing(instance_key, resource_type, "repo/1", "c1", "2026-04-28T10:00:00Z")
        comments.mark_comment_processed(instance_key, resource_type, "repo/1", "c1")

        # Check that resource 2 has no comments
        unprocessed = comments.get_unprocessed_comments(instance_key, resource_type, "repo/2")
        assert len(unprocessed) == 0

    def test_comments_isolated_by_type(self, instance_key):
        """Comments for different resource types should be isolated."""
        # Process PR comment
        comments.mark_comment_processing(instance_key, "pr", "repo/1", "c1", "2026-04-28T10:00:00Z")
        comments.mark_comment_processed(instance_key, "pr", "repo/1", "c1")

        # Check that ticket comments have none
        unprocessed = comments.get_unprocessed_comments(instance_key, "ticket", "JIRA-123")
        assert len(unprocessed) == 0


class TestTimestampNormalization:
    """Test that timestamps are handled consistently."""

    def test_uses_updated_at_if_present(self, instance_key, resource_type, resource_id):
        """Should use updated_at if available."""
        platform = MockPlatform(
            pr_comments=[
                {
                    "id": "c1",
                    "body": "Edited comment",
                    "author_id": "u1",
                    "created_at": "2026-04-28T10:00:00Z",
                    "updated_at": "2026-04-28T15:00:00Z",
                }
            ]
        )

        comments.fetch_and_detect_comments(instance_key, platform, resource_type, resource_id)
        row = db.query_one(
            "SELECT comment_edited_at FROM comment_state WHERE comment_id = ?",
            ("c1",),
        )
        # Should store updated_at, not created_at
        assert row["comment_edited_at"] == "2026-04-28T15:00:00Z"

    def test_falls_back_to_created_at(self, instance_key, resource_type, resource_id):
        """Should use created_at when updated_at is None."""
        platform = MockPlatform(
            pr_comments=[
                {
                    "id": "c1",
                    "body": "Text",
                    "author_id": "u1",
                    "created_at": "2026-04-28T10:00:00Z",
                    "updated_at": None,
                }
            ]
        )

        comments.fetch_and_detect_comments(instance_key, platform, resource_type, resource_id)
        row = db.query_one(
            "SELECT comment_edited_at FROM comment_state WHERE comment_id = ?",
            ("c1",),
        )
        # Should store created_at
        assert row["comment_edited_at"] == "2026-04-28T10:00:00Z"
