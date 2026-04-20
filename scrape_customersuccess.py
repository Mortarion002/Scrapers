import requests
import csv
import time
from datetime import datetime, timedelta

SUBREDDIT = "CustomerSuccess"
KEYWORDS = [
    "NPS", "CSAT", "net promoter", "customer feedback", "survey tool",
    "Delighted", "survey platform", "feedback tool", "customer satisfaction",
    "switching from", "alternatives", "Medallia", "Qualtrics", "Survicate",
    "Zonka", "customer effort score", "CES", "feedback software"
]

HEADERS = {"User-Agent": "elvan-scraper/1.0"}
ONE_MONTH_AGO = datetime.utcnow() - timedelta(days=30)
OUTPUT_FILE = "customersuccess_posts.csv"


def search_keyword(keyword, existing_ids):
    posts = []
    after = None
    page = 0

    while True:
        url = f"https://www.reddit.com/r/{SUBREDDIT}/search.json"
        params = {
            "q": keyword,
            "restrict_sr": 1,
            "sort": "new",
            "limit": 100,
            "t": "year",  # search within last year, we filter by month below
        }
        if after:
            params["after"] = after

        try:
            res = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if res.status_code == 429:
                print(f"  Rate limited, sleeping 60s...")
                time.sleep(60)
                continue
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            print(f"  Error fetching '{keyword}' page {page}: {e}")
            break

        children = data.get("data", {}).get("children", [])
        if not children:
            break

        stop = False
        for child in children:
            p = child["data"]
            created = datetime.utcfromtimestamp(p["created_utc"])

            if created < ONE_MONTH_AGO:
                stop = True
                break

            post_id = p["id"]
            if post_id in existing_ids:
                continue

            existing_ids.add(post_id)
            posts.append({
                "id": post_id,
                "title": p.get("title", ""),
                "body": p.get("selftext", "")[:1000],  # truncate long posts
                "author": p.get("author", ""),
                "url": f"https://reddit.com{p.get('permalink', '')}",
                "upvotes": p.get("score", 0),
                "comments": p.get("num_comments", 0),
                "created_utc": created.strftime("%Y-%m-%d %H:%M"),
                "keyword_matched": keyword,
                "flair": p.get("link_flair_text", ""),
            })

        after = data.get("data", {}).get("after")
        page += 1

        if stop or not after:
            break

        time.sleep(2)  # be polite to Reddit

    return posts


def main():
    print(f"Scraping r/{SUBREDDIT} for NPS/CSAT posts from last 30 days...\n")
    all_posts = []
    seen_ids = set()

    for kw in KEYWORDS:
        print(f"Searching: '{kw}'...")
        found = search_keyword(kw, seen_ids)
        print(f"  → {len(found)} new posts")
        all_posts.extend(found)
        time.sleep(3)  # pause between keywords

    # Sort by date descending
    all_posts.sort(key=lambda x: x["created_utc"], reverse=True)

    # Write CSV
    if all_posts:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_posts[0].keys())
            writer.writeheader()
            writer.writerows(all_posts)
        print(f"\n✓ Saved {len(all_posts)} posts to '{OUTPUT_FILE}'")
    else:
        print("\nNo posts found in the last 30 days.")

    # Summary
    print(f"\n--- Summary ---")
    print(f"Total unique posts: {len(all_posts)}")
    if all_posts:
        print(f"Date range: {all_posts[-1]['created_utc']} → {all_posts[0]['created_utc']}")
        kw_counts = {}
        for p in all_posts:
            kw_counts[p["keyword_matched"]] = kw_counts.get(p["keyword_matched"], 0) + 1
        print("\nPosts per keyword:")
        for kw, count in sorted(kw_counts.items(), key=lambda x: -x[1]):
            print(f"  {kw}: {count}")


if __name__ == "__main__":
    main()