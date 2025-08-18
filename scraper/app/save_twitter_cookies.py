import asyncio
import json
from playwright.async_api import async_playwright

COOKIES_FILE = "twitter_cookies.json"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto("https://x.com/login")
        print("Please log in manually in the opened browser window.")
        input("Press Enter here after you have logged in and see your home feed...")

        cookies = await context.cookies()
        with open(COOKIES_FILE, "w") as f:
            json.dump(cookies, f)
        print(f"Cookies saved to {COOKIES_FILE}")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
