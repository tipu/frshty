"""Test to verify discover_instances finds all running instances"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from core.discovery import discover_instances


def test_discover_instances_finds_all():
    """Test that discover_instances returns all configured instances"""
    instances = discover_instances()
    instance_keys = {i["key"] for i in instances}

    print(f"Found instances: {instance_keys}")

    # Expected instances based on frshty setup
    expected = {"aimyable", "atropos", "nectar"}

    missing = expected - instance_keys
    if missing:
        print(f"❌ FAIL: Missing instances: {missing}")
        print(f"\nFull discovery results:")
        for inst in instances:
            print(f"  - {inst['key']}: {inst['base_url']}")
        return False

    print(f"✓ PASS: All instances discovered")
    return True


if __name__ == "__main__":
    success = test_discover_instances_finds_all()
    sys.exit(0 if success else 1)
