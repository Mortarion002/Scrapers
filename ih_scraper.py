"""
ih_scraper.py
=============
Scrapes Indie Hackers products with revenue >= $2000/mo
using their internal Algolia search API.

No browser needed — pure HTTP calls.

Requirements:
    pip install requests

Usage:
    python ih_scraper.py

Output:
    indiehackers_products_2000plus.csv
"""

import requests
import csv
import time

# ── Algolia credentials (public read-only) ─────────────────────────────────────
APP_ID  = 
API_KEY = 
URL     = f"https://{APP_ID.lower()}-3.algolianet.com/1/indexes/*/queries"

HEADERS = {
    "x-algolia-application-id": APP_ID,
    "x-algolia-api-key":        API_KEY,
    "Content-Type":             "application/json",
}

# Revenue bands to bypass Algolia's 1000-result cap
# Each band has < 1000 results so we get everything
BANDS = [
    {"min": 2000,   "max": 4999,    "pages": 9},
    {"min": 5000,   "max": 9999,    "pages": 5},
    {"min": 10000,  "max": 24999,   "pages": 7},
    {"min": 25000,  "max": 49999,   "pages": 3},
    {"min": 50000,  "max": 99999,   "pages": 2},
    {"min": 100000, "max": 9999999, "pages": 3},
]

OUTPUT_FILE = "indiehackers_products_2000plus.csv"

CSV_FIELDS = [
    "name", "productId", "tagline", "revenue", "description",
    "websiteUrl", "startDateStr", "userIds", "numFollowers",
    "ih_profile_url", "created_date", "updated_date",
]


def fetch_page(band_min, band_max, page):
    resp = requests.post(URL, headers=HEADERS, json={
        "requests": [{
            "indexName": "products",
            "params": (
                f"hitsPerPage=100&page={page}"
                f"&numericFilters=revenue>={band_min},revenue<={band_max}"
            )
        }]
    }, timeout=15)
    resp.raise_for_status()
    return resp.json()["results"][0]["hits"]


def ms_to_date(ms):
    if not ms:
        return ""
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def flatten(hit):
    user_ids = hit.get("userIds") or []
    return {
        "name":           hit.get("name", ""),
        "productId":      hit.get("productId", ""),
        "tagline":        hit.get("tagline", ""),
        "revenue":        hit.get("revenue", ""),
        "description":    (hit.get("description") or "").replace("\n", " ").strip(),
        "websiteUrl":     hit.get("websiteUrl", ""),
        "startDateStr":   hit.get("startDateStr", ""),
        "userIds":        " | ".join(user_ids),
        "numFollowers":   hit.get("numFollowers", ""),
        "ih_profile_url": f"https://www.indiehackers.com/product/{hit.get('productId', '')}",
        "created_date":   ms_to_date(hit.get("createdTimestamp")),
        "updated_date":   ms_to_date(hit.get("updatedTimestamp")),
    }


def scrape():
    all_products = []
    seen = set()

    print(f"\nScraping Indie Hackers — revenue >= $2,000/mo\n")

    for band in BANDS:
        label = f"${band['min']:,}–${band['max']:,}"
        print(f"  Band {label}:")
        for page in range(band["pages"]):
            try:
                hits = fetch_page(band["min"], band["max"], page)
                new = 0
                for hit in hits:
                    pid = hit.get("productId") or hit.get("objectID")
                    if pid and pid not in seen:
                        seen.add(pid)
                        all_products.append(flatten(hit))
                        new += 1
                print(f"    Page {page+1}/{band['pages']} — {new} new products (total: {len(all_products)})")
                time.sleep(0.25)
            except Exception as e:
                print(f"    ERROR on page {page}: {e}")

    return all_products


def save(products):
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(products)
    print(f"\nSaved {len(products):,} products → {OUTPUT_FILE}")


if __name__ == "__main__":
    products = scrape()
    if products:
        save(products)
        print("\nSample:")
        for p in products[:3]:
            print(f"  • {p['name'][:35]:35s} ${p['revenue']:>8}/mo  {p['websiteUrl']}")
    else:
        print("No products found.")
