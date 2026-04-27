"""Test to verify global events includes other instances"""
import sys
from pathlib import Path
import json

sys.path.insert(0, str(Path(__file__).parent))

from core.discovery import discover_instances


def test_global_events_includes_other_instances():
    """Test that /api/global/events returns events from other instances"""
    import httpx

    instances = discover_instances()
    nectar = next((i for i in instances if i["key"] == "nectar"), None)

    if not nectar:
        print("❌ nectar instance not found")
        return False

    print(f"Testing global events from: {nectar['base_url']}")

    try:
        # Call nectar's global events API
        response = httpx.get(
            f"{nectar['base_url']}/api/global/events?limit=100",
            verify=False,
            timeout=10
        )
        response.raise_for_status()
        events = response.json()

        if not isinstance(events, dict) or "events" not in events:
            print(f"❌ Unexpected response format: {type(events)}")
            return False

        events_list = events.get("events", [])
        instance_keys = {e.get("instance_key") for e in events_list if e.get("instance_key")}

        print(f"Found events from instances: {instance_keys}")

        # Should have events from multiple instances
        if len(instance_keys) <= 1:
            print(f"❌ FAIL: Only showing events from {len(instance_keys)} instance(s)")
            print(f"\nSample events:")
            for e in events_list[:3]:
                print(f"  instance_key={e.get('instance_key')}, event={e.get('event')}")
            return False

        print(f"✓ PASS: Found events from {len(instance_keys)} instances")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    success = test_global_events_includes_other_instances()
    sys.exit(0 if success else 1)
