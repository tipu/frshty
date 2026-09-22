import asyncio
import os
import sys
from pathlib import Path
from playwright.async_api import async_playwright

BASE = (sys.argv[1] if len(sys.argv) > 1
        else os.environ.get("FRSHTY_BASE_URL", "")).rstrip("/")
if not BASE:
    raise SystemExit("usage: explore_remaining.py <frshty base url>")
SCREENSHOTS_DIR = Path(os.environ.get("FRSHTY_SCREENSHOTS_DIR",
                                      "/tmp/frshty_screenshots"))

async def take_screenshot(page, name):
    path = SCREENSHOTS_DIR / f"{name}.png"
    await page.screenshot(path=str(path), full_page=True)
    print(f"✓ {name}")

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        
        pages = [
            (f"{BASE}/timesheet", "09_timesheet"),
            (f"{BASE}/billing", "10_billing"),
            (f"{BASE}/config", "11_config"),
        ]
        
        for url, name in pages:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(500)
                await take_screenshot(page, name)
            except Exception as e:
                print(f"✗ {name}: {e}")
        
        await browser.close()

asyncio.run(main())
