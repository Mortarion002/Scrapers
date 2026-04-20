"""
G2 + Capterra Delighted Review Scraper  (v3)
=============================================
Run from your LOCAL machine in VS Code terminal.

Install once:
    pip install playwright
    playwright install chromium

Run:
    python g2_capterra_scraper.py

Two CSV files will appear in the SAME FOLDER as this script.
"""

import asyncio, csv, json, re, os, pathlib

from playwright.async_api import async_playwright

# ── Output files go to the same folder as this script ────────────────────────
HERE            = pathlib.Path(__file__).parent
OUTPUT_G2       = HERE / "g2_delighted_reviews.csv"
OUTPUT_CAPTERRA = HERE / "capterra_delighted_reviews.csv"

TARGET_STARS    = {1, 2, 3}          # set to set() to collect ALL ratings

# ── Stealth JS (no external library needed) ───────────────────────────────────
STEALTH = """
    Object.defineProperty(navigator, 'webdriver',  { get: () => undefined });
    Object.defineProperty(navigator, 'plugins',    { get: () => [1,2,3,4,5] });
    Object.defineProperty(navigator, 'languages',  { get: () => ['en-US','en'] });
    window.chrome = { runtime: {} };
    const _orig = window.navigator.permissions.query.bind(navigator.permissions);
    window.navigator.permissions.query = (p) =>
        p.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : _orig(p);
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

async def make_page(pw):
    browser = await pw.chromium.launch(
        headless=False,
        args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
    )
    ctx = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="en-US",
        viewport={"width": 1280, "height": 900},
    )
    await ctx.add_init_script(STEALTH)
    return browser, await ctx.new_page()


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  ✅ Saved {len(rows)} rows → {path}")


def star_from_text(text):
    """Extract integer star count from strings like '4 out of 5', '3.0', '2 stars'."""
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:out of|/|\s*stars?)?", str(text))
    return int(float(m.group(1))) if m else 0


# ── G2 ────────────────────────────────────────────────────────────────────────

async def scrape_g2():
    print("\n[G2] Starting — a Chrome window will open…")
    results = []

    async with async_playwright() as pw:
        browser, page = await make_page(pw)

        # ── 1. Load the page and wait for it to fully render ─────────────
        await page.goto(
            "https://www.g2.com/products/delighted/reviews",
            wait_until="domcontentloaded", timeout=60_000
        )
        # Wait for any CAPTCHA redirect to fully settle before touching the DOM
        print("  Waiting for page to fully settle (solve any CAPTCHA if prompted)…")
        try:
            await page.wait_for_load_state("networkidle", timeout=60_000)
        except Exception:
            pass   # networkidle can time out on heavy pages — that's fine
        await page.wait_for_timeout(3_000)

        # ── Dismiss G2 login popup if it appears ─────────────────────────
        for dismiss_sel in [
            'button:has-text("Stay logged out")',
            'button[aria-label="Close"]',
            '[data-testid="modal-close"]',
            'button:has-text("×")',
        ]:
            try:
                btn = page.locator(dismiss_sel).first
                if await btn.count() > 0:
                    await btn.click()
                    print("  Dismissed G2 login popup ✅")
                    await page.wait_for_timeout(2_000)
                    break
            except Exception:
                pass

        # If G2 redirected away from reviews page, navigate back
        if "delighted/reviews" not in page.url:
            print(f"  Redirected to {page.url} — navigating back to reviews page…")
            await page.goto(
                "https://www.g2.com/products/delighted/reviews",
                wait_until="domcontentloaded", timeout=30_000
            )
            await page.wait_for_timeout(3_000)

        # ── 2. Try to extract embedded __NEXT_DATA__ JSON (most reliable) ─
        next_data_raw = None
        for attempt in range(3):
            try:
                next_data_raw = await page.evaluate("""
                    () => {
                        const el = document.getElementById('__NEXT_DATA__');
                        return el ? el.textContent : null;
                    }
                """)
                break   # success
            except Exception as e:
                print(f"  JS evaluate attempt {attempt+1} failed ({e}) — waiting…")
                await page.wait_for_load_state("domcontentloaded", timeout=15_000)
                await page.wait_for_timeout(2_000)

        if next_data_raw:
            print("  Found __NEXT_DATA__ — extracting reviews from JSON…")
            try:
                nd = json.loads(next_data_raw)
                # Walk the JSON tree looking for arrays that contain review objects
                def find_reviews(obj, depth=0):
                    if depth > 10: return []
                    found = []
                    if isinstance(obj, list):
                        for item in obj:
                            if isinstance(item, dict) and (
                                "rating" in item or "star_rating" in item or
                                "review_body" in item or "body" in item
                            ):
                                found.append(item)
                            else:
                                found += find_reviews(item, depth+1)
                    elif isinstance(obj, dict):
                        for v in obj.values():
                            found += find_reviews(v, depth+1)
                    return found

                raw_reviews = find_reviews(nd)
                print(f"  Found {len(raw_reviews)} review objects in JSON")

                for r in raw_reviews:
                    star = int(r.get("rating") or r.get("star_rating") or
                               r.get("overall_rating") or 0)
                    if TARGET_STARS and star not in TARGET_STARS:
                        continue
                    results.append({
                        "Source":            "G2",
                        "Reviewer_Name":     r.get("reviewer_name") or r.get("name") or
                                             (r.get("author") or {}).get("name", ""),
                        "Job_Title":         r.get("job_title") or r.get("title") or
                                             r.get("reviewer_title", ""),
                        "Company_Name":      r.get("company_name") or r.get("company", ""),
                        "Company_Size":      r.get("company_size") or
                                             r.get("company", {}).get("size", "") if isinstance(r.get("company"), dict) else "",
                        "Industry":          r.get("industry", ""),
                        "Star_Rating":       star,
                        "Review_Date":       r.get("submitted_at") or r.get("created_at") or
                                             r.get("date", ""),
                        "Review_Title":      r.get("title") or r.get("summary", ""),
                        "Review_Body":       str(r.get("body") or r.get("review_body") or
                                             r.get("comment", ""))[:600],
                        "Outreach_Priority": "HIGH" if star in {1, 2} else "MEDIUM",
                    })
            except Exception as e:
                print(f"  JSON parse error: {e}")

        # ── 3. Fall back: scroll + DOM scrape ─────────────────────────────
        if not results:
            print("  No JSON data found — falling back to DOM scraping…")

            # Scroll to reviews section
            await page.evaluate("window.scrollBy(0, 800)")
            await page.wait_for_timeout(2_000)

            # Click "Reviews" tab if present
            for txt in ["Reviews", "All Reviews"]:
                try:
                    btn = page.get_by_role("link", name=txt).first
                    if await btn.count() > 0:
                        await btn.click()
                        await page.wait_for_timeout(2_000)
                        break
                except Exception:
                    pass

            # Scroll through the page to trigger lazy loading
            for _ in range(5):
                await page.evaluate("window.scrollBy(0, 600)")
                await page.wait_for_timeout(800)

            # Take screenshot for debugging
            shot = HERE / "g2_debug.png"
            await page.screenshot(path=str(shot), full_page=False)
            print(f"  Debug screenshot saved → {shot}")

            # Try every selector pattern we know about (including Capterra's Tailwind classes)
            selector_hits = {}
            for sel in [
                "div.flex.flex-col.gap-y-6",  # Capterra's actual review card container
                "[itemprop='review']",
                "[data-testid='review']",
                ".paper.paper--white",
                "article[class*='review']",
                "div[class*='review-card']",
                "li[class*='review']",
                ".reviews-list article",
                "section article",
            ]:
                count = await page.locator(sel).count()
                if count > 0:
                    selector_hits[sel] = count

            if selector_hits:
                best_sel = max(selector_hits, key=selector_hits.get)
                print(f"  Best selector: '{best_sel}' ({selector_hits[best_sel]} cards)")
                cards = await page.locator(best_sel).all()

                for card in cards:
                    try:
                        text = await card.inner_text()
                        # Rough parse: grab what we can from text content
                        results.append({
                            "Source":            "G2",
                            "Reviewer_Name":     "",
                            "Job_Title":         "",
                            "Company_Name":      "",
                            "Company_Size":      "",
                            "Industry":          "",
                            "Star_Rating":       star_from_text(
                                await card.locator("[aria-label*='star'], [class*='star']")
                                          .first.get_attribute("aria-label")
                                          if await card.locator("[aria-label*='star'], [class*='star']").count() > 0
                                          else ""
                            ),
                            "Review_Date":       "",
                            "Review_Title":      "",
                            "Review_Body":       text[:600],
                            "Outreach_Priority": "",
                        })
                    except Exception:
                        pass
            else:
                print("  ⚠  No review cards matched any selector.")
                print("     Open g2_debug.png to see what loaded.")
                print("     Tip: G2 may need you to be logged in to see reviews.")

        await browser.close()

    # Always write CSV (even if empty, so you know the file is there)
    fields = ["Source","Reviewer_Name","Job_Title","Company_Name","Company_Size",
              "Industry","Star_Rating","Review_Date","Review_Title",
              "Review_Body","Outreach_Priority"]
    write_csv(OUTPUT_G2, results, fields)
    print(f"[G2] Done — {len(results)} records.")
    return results


# ── CAPTERRA ──────────────────────────────────────────────────────────────────

async def scrape_capterra():
    print("\n[Capterra] Starting — a Chrome window will open…")
    results = []

    async with async_playwright() as pw:
        browser, page = await make_page(pw)

        await page.goto(
            "https://www.capterra.com/p/157973/Delighted/reviews/",
            wait_until="domcontentloaded", timeout=60_000
        )

        # ── Wait for Cloudflare to clear (up to 30 sec) ───────────────────
        print("  Waiting for Cloudflare verification to clear (up to 30s)…")
        for i in range(30):
            try:
                title = await page.title()
                url   = page.url
                if "capterra" in title.lower() or "delighted" in title.lower():
                    print(f"  Cloudflare cleared after {i}s ✅")
                    break
            except Exception:
                pass   # page is mid-navigation — just keep waiting
            if i == 15:
                print("  Still waiting… (if a CAPTCHA appeared, solve it in the window)")
            await page.wait_for_timeout(1_000)
        else:
            print("  ⚠  Cloudflare did not clear — Capterra may be inaccessible from this IP.")

        await page.wait_for_timeout(3_000)

        # Screenshot for debugging
        shot = HERE / "capterra_debug.png"
        await page.screenshot(path=str(shot), full_page=False)
        print(f"  Debug screenshot → {shot}")

        page_num = 1
        while True:
            print(f"  Scraping page {page_num}…")

            cards = []
            for sel in [
                "div.flex.flex-col.gap-y-6",  # Capterra's actual review container (Tailwind classes)
                "[data-testid='review-card']",
                "[data-testid='review']",
                "article[class*='review']",
                ".review-card",
                "div[class*='ReviewCard']",
                "div[class*='review-listing']",
                "li[class*='review']",
            ]:
                cards = await page.locator(sel).all()
                if cards:
                    print(f"  Found {len(cards)} cards with '{sel}'")
                    break

            if not cards:
                print(f"  ⚠  No review cards on page {page_num} — check capterra_debug.png")
                break

            for card in cards:
                try:
                    async def t(sel):
                        try: return (await card.locator(sel).first.text_content(timeout=2000) or "").strip()
                        except: return ""
                    async def a(sel, attr):
                        try: return (await card.locator(sel).first.get_attribute(attr, timeout=2000) or "").strip()
                        except: return ""

                    name  = await t("[data-testid='reviewer-name'], h3, .reviewer-name")
                    title = await t("[data-testid='reviewer-title'], .reviewer-title, .job-title")
                    co    = await t("[data-testid='reviewer-company'], .company-name")
                    rating_label = await a("[aria-label*='star'], [class*='star-rating'], [class*='StarRating']", "aria-label")
                    date  = await t("time, [data-testid='review-date'], .review-date")
                    rev_t = await t("h4, [data-testid='review-title']")
                    body  = await t("[data-testid='review-body'], .review-body, p")

                    star = star_from_text(rating_label)
                    if TARGET_STARS and star not in TARGET_STARS and star != 0:
                        continue

                    results.append({
                        "Source":        "Capterra",
                        "Reviewer_Name": name,
                        "Job_Title":     title,
                        "Company_Name":  co,
                        "Star_Rating":   star or rating_label,
                        "Review_Date":   date,
                        "Review_Title":  rev_t,
                        "Review_Body":   body[:600],
                    })
                except Exception as e:
                    print(f"  Card parse error: {e}")

            # Next page
            try:
                nxt = page.locator('[aria-label="Next page"], button:has-text("Next")').first
                if await nxt.count() > 0 and await nxt.is_enabled():
                    await nxt.click()
                    page_num += 1
                    await page.wait_for_timeout(3_000)
                else:
                    break
            except Exception:
                break

        await browser.close()

    fields = ["Source","Reviewer_Name","Job_Title","Company_Name",
              "Star_Rating","Review_Date","Review_Title","Review_Body"]
    write_csv(OUTPUT_CAPTERRA, results, fields)
    print(f"[Capterra] Done — {len(results)} records.")
    return results


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    print("=" * 60)
    print("Delighted Review Scraper  v3")
    print(f"Target ratings : {sorted(TARGET_STARS) if TARGET_STARS else 'ALL'}")
    print(f"Output folder  : {HERE}")
    print("=" * 60)

    await scrape_g2()
    await scrape_capterra()

    print("\n" + "=" * 60)
    print("All done!")
    print(f"  {OUTPUT_G2}")
    print(f"  {OUTPUT_CAPTERRA}")
    print("\nEven if 0 reviews were found, both CSV files have been created.")
    print("Open them to check — or open the debug .png screenshots to see")
    print("exactly what loaded in the browser window.")


if __name__ == "__main__":
    asyncio.run(main())