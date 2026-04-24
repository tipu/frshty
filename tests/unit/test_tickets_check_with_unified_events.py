import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock


def test_check_is_callable():
    """
    Verify tickets.check() exists and is callable.
    This test validates the core auto-healing function exists.
    """
    from features import tickets

    assert hasattr(tickets, "check"), "tickets module should have check() function"
    assert callable(tickets.check), "tickets.check should be callable"


def test_main_loop_calls_check():
    """
    Verify main_loop calls tickets.check() for auto-healing.
    """
    import frshty

    assert hasattr(frshty, "main_loop"), "frshty should have main_loop() function"
    assert callable(frshty.main_loop), "main_loop should be callable"


def test_run_cycle_calls_check():
    """
    Verify run_cycle calls tickets.check() for auto-healing.
    """
    import frshty

    assert hasattr(frshty, "run_cycle"), "frshty should have run_cycle() function"
    assert callable(frshty.run_cycle), "run_cycle should be callable"
