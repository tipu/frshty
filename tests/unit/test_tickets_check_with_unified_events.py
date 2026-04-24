from pathlib import Path


def test_check_is_callable():
    """
    Verify tickets.check() exists and is callable.
    This test validates the core auto-healing function exists.
    """
    from features import tickets

    assert hasattr(tickets, "check"), "tickets module should have check() function"
    assert callable(tickets.check), "tickets.check should be callable"


def test_main_loop_function_exists():
    """
    Verify main_loop function exists in frshty.py.
    """
    frshty_path = Path(__file__).parent.parent.parent / "frshty.py"
    source = frshty_path.read_text()

    assert "def main_loop(" in source, "frshty should have main_loop() function"


def test_run_cycle_function_exists():
    """
    Verify run_cycle function exists in frshty.py.
    """
    frshty_path = Path(__file__).parent.parent.parent / "frshty.py"
    source = frshty_path.read_text()

    assert "def run_cycle(" in source, "frshty should have run_cycle() function"
