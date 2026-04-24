import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

SCREENSHOTS_DIR = Path("/tmp/aimyable_screenshots")
SCREENSHOTS_DIR.mkdir(exist_ok=True)

async def take_screenshot(page, name, description=""):
    path = SCREENSHOTS_DIR / f"{name}.png"
    await page.screenshot(path=str(path), full_page=True)
    print(f"Screenshot: {name}")
    return str(path)

async def get_page_elements(page):
    """Get all interactive elements"""
    return await page.evaluate("""() => {
        const links = Array.from(document.querySelectorAll('a')).map(a => ({
            text: a.textContent.trim().substring(0, 100),
            href: a.href
        }));
        const buttons = Array.from(document.querySelectorAll('button')).map(b => ({
            text: b.textContent.trim().substring(0, 100)
        }));
        return { links, buttons };
    }""")

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        
        # Explore main pages
        await page.goto("https://aimyable.localhost/", wait_until="networkidle")
        await take_screenshot(page, "01_home", "Home page - main feed")
        
        await page.goto("https://aimyable.localhost/global", wait_until="networkidle")
        await take_screenshot(page, "02_global", "Global feed - events from all instances")
        
        await page.goto("https://aimyable.localhost/reviews", wait_until="networkidle")
        await take_screenshot(page, "03_reviews_list", "Reviews list view")
        
        # Try to click on a review
        try:
            reviews = await page.evaluate("() => Array.from(document.querySelectorAll('a')).filter(a => a.href.includes('/reviews/')).map(a => a.href)")
            if reviews:
                print(f"Found {len(reviews)} review links")
                await page.goto(reviews[0], wait_until="networkidle")
                await take_screenshot(page, "04_review_detail", "Review detail page")
        except Exception as e:
            print(f"Could not explore review detail: {e}")
        
        await page.goto("https://aimyable.localhost/tickets", wait_until="networkidle")
        await take_screenshot(page, "05_tickets_list", "Tickets list view")
        
        # Try to click on a ticket
        try:
            tickets = await page.evaluate("() => Array.from(document.querySelectorAll('a')).filter(a => a.href.includes('/tickets/')).map(a => a.href)")
            if tickets:
                print(f"Found {len(tickets)} ticket links")
                await page.goto(tickets[0], wait_until="networkidle")
                await take_screenshot(page, "06_ticket_detail", "Ticket detail page")
        except Exception as e:
            print(f"Could not explore ticket detail: {e}")
        
        await page.goto("https://aimyable.localhost/scheduled", wait_until="networkidle")
        await take_screenshot(page, "07_scheduled", "Scheduled jobs view")
        
        await page.goto("https://aimyable.localhost/slack", wait_until="networkidle")
        await take_screenshot(page, "08_slack", "Slack messages view")
        
        await page.goto("https://aimyable.localhost/timesheet", wait_until="networkidle")
        await take_screenshot(page, "09_timesheet", "Timesheet view")
        
        await page.goto("https://aimyable.localhost/billing", wait_until="networkidle")
        await take_screenshot(page, "10_billing", "Billing view")
        
        await page.goto("https://aimyable.localhost/config", wait_until="networkidle")
        await take_screenshot(page, "11_config", "Configuration page")
        
        await browser.close()

asyncio.run(main())
