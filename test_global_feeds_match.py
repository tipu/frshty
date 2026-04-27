"""Test to verify global feeds on all instances show the same events from all instances"""
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright


async def test_global_feeds_match():
    """Verify aimyable and nectar global feeds both show events from all instances"""

    urls = {
        'aimyable': 'https://aimyable.localhost/global',
        'nectar': 'https://nectar.localhost/global'
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch()

        results = {}
        for name, url in urls.items():
            page = await browser.new_page(ignore_https_errors=True)
            await page.goto(url, wait_until='networkidle')

            # Wait for events to load
            await page.wait_for_selector('#event-feed .feed-row', timeout=5000)

            # Get all event rows and extract instance keys
            rows = await page.locator('.feed-row').all()
            instance_keys = []
            for row in rows[:50]:  # Check first 50 events
                # Instance key is in the second badge of each row
                badges = await row.locator('.badge').all()
                if len(badges) >= 2:
                    instance_text = await badges[1].text_content()
                    if instance_text and instance_text.strip() not in ['●', '○', '']:
                        instance_keys.append(instance_text.strip())

            # Get unique instances shown
            unique_instances = sorted(set(instance_keys))

            results[name] = {
                'url': url,
                'instances_shown': unique_instances,
                'total_events': await page.locator('#event-feed .feed-row').count()
            }

            # Take screenshot
            screenshot_path = f'/tmp/global_feed_{name}.png'
            await page.screenshot(path=screenshot_path)
            print(f"Screenshot saved: {screenshot_path}")

            await page.close()

        await browser.close()

    # Print results
    print("\n" + "="*60)
    print("Global Feed Comparison")
    print("="*60)

    for name, result in results.items():
        print(f"\n{name.upper()}:")
        print(f"  URL: {result['url']}")
        print(f"  Instances shown: {result['instances_shown']}")
        print(f"  Total events: {result['total_events']}")

    # Check if they match
    aimyable_instances = set(results['aimyable']['instances_shown'])
    nectar_instances = set(results['nectar']['instances_shown'])

    print("\n" + "="*60)
    if aimyable_instances == nectar_instances:
        print("✓ PASS: Both feeds show the same instances")
        return True
    else:
        print("❌ FAIL: Feeds show different instances")
        print(f"  Aimyable only: {aimyable_instances - nectar_instances}")
        print(f"  Nectar only: {nectar_instances - aimyable_instances}")
        return False


if __name__ == '__main__':
    success = asyncio.run(test_global_feeds_match())
    exit(0 if success else 1)
