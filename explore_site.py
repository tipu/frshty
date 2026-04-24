import asyncio
from pathlib import Path
from playwright.async_api import async_playwright
import json

SCREENSHOTS_DIR = Path("/tmp/aimyable_screenshots")
SCREENSHOTS_DIR.mkdir(exist_ok=True)

async def take_screenshot(page, name, description=""):
    """Take a screenshot and return metadata"""
    path = SCREENSHOTS_DIR / f"{name}.png"
    await page.screenshot(path=str(path), full_page=True)
    return {"file": str(path), "name": name, "description": description}

async def get_page_links(page):
    """Extract all links from page"""
    links = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('a')).map(a => ({
            text: a.textContent.trim(),
            href: a.href,
            title: a.title
        }));
    }""")
    return links

async def get_page_buttons(page):
    """Extract all buttons from page"""
    buttons = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('button')).map(b => ({
            text: b.textContent.trim(),
            class: b.className,
            title: b.title
        }));
    }""")
    return buttons

async def get_form_inputs(page):
    """Extract all form inputs"""
    inputs = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('input, select, textarea')).map(el => ({
            type: el.type || el.tagName,
            placeholder: el.placeholder,
            name: el.name,
            value: el.value,
            label: document.querySelector(`label[for="${el.id}"]`)?.textContent.trim()
        }));
    }""")
    return inputs

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        
        pages_to_explore = [
            "/",
            "/global",
            "/reviews",
            "/tickets",
            "/slack",
            "/config",
        ]
        
        documentation = {}
        
        for page_path in pages_to_explore:
            try:
                url = f"https://aimyable.localhost{page_path}"
                print(f"\n{'='*60}")
                print(f"Exploring: {url}")
                print(f"{'='*60}")
                
                await page.goto(url, wait_until="networkidle")
                await page.wait_for_timeout(1000)  # Extra wait for JS to settle
                
                # Get page title and content
                title = await page.title()
                
                # Take screenshot
                screenshot = await take_screenshot(page, f"page_{page_path.replace('/', '_') or 'home'}", f"Page: {page_path}")
                
                # Get all interactive elements
                links = await get_page_links(page)
                buttons = await get_page_buttons(page)
                inputs = await get_form_inputs(page)
                
                # Get page content
                content = await page.content()
                
                page_doc = {
                    "url": url,
                    "title": title,
                    "screenshot": screenshot,
                    "links": links,
                    "buttons": buttons,
                    "form_inputs": inputs,
                }
                
                documentation[page_path] = page_doc
                
                print(f"Title: {title}")
                print(f"Links found: {len(links)}")
                print(f"Buttons found: {len(buttons)}")
                print(f"Form inputs: {len(inputs)}")
                
                if links:
                    print("\nLinks:")
                    for link in links[:10]:  # First 10
                        print(f"  - {link['text']}: {link['href']}")
                    if len(links) > 10:
                        print(f"  ... and {len(links) - 10} more")
                
                if buttons:
                    print("\nButtons:")
                    for btn in buttons[:10]:
                        print(f"  - {btn['text']}")
                    if len(buttons) > 10:
                        print(f"  ... and {len(buttons) - 10} more")
                
            except Exception as e:
                print(f"Error exploring {page_path}: {e}")
                documentation[page_path] = {"error": str(e)}
        
        await browser.close()
        
        # Save documentation
        doc_file = SCREENSHOTS_DIR / "documentation.json"
        with open(doc_file, "w") as f:
            json.dump(documentation, f, indent=2, default=str)
        
        print(f"\n\nDocumentation saved to: {doc_file}")
        print(f"Screenshots saved to: {SCREENSHOTS_DIR}")

asyncio.run(main())
