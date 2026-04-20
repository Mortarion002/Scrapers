import requests
import json
import csv
from datetime import datetime

# ─────────────────────────────────────────────
#  CONFIG — paste your Bloomberry API key here
# ─────────────────────────────────────────────
API_KEY =    # <── replace this

BASE_URL = "https://api.revealera.com/accounts/tech.json"

BASE_PARAMS = {
    "api_key":     API_KEY,
    "vendor_name": "Delighted by Qualtrics",  # exact vendor name
    "list_mode":   "false",
}

OUTPUT_JSON = "delighted_customers.json"
OUTPUT_CSV  = "delighted_customers.csv"


def fetch_all_customers():
    all_customers = []
    page = 3

    print("Fetching companies using Delighted by Qualtrics...\n")

    while True:
        params = {**BASE_PARAMS, "page": page}

        try:
            response = requests.get(BASE_URL, params=params, timeout=30)

            if page == 1:
                safe_url = response.url.replace(API_KEY, "***")
                print(f"  Calling: {safe_url}\n")

            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            print(f"[HTTP Error] {e}")
            print(f"Response: {response.text[:300]}")
            break
        except requests.exceptions.RequestException as e:
            print(f"[Request Error] {e}")
            break

        # Show remaining credits
        remaining = response.headers.get("X-Credits-Month-Remaining")
        if remaining and page == 1:
            print(f"  Credits remaining this month: {remaining}\n")

        data = response.json()

        # Debug: print raw response on page 1
        if page == 1:
            print(f"  Raw response preview: {str(data)[:300]}\n")
            print(f"  Top-level keys in response: {list(data.keys()) if isinstance(data, dict) else 'list'}\n")

        # ✅ FIX: API returns data under "users" key — check all possible keys
        if isinstance(data, list):
            customers = data
        else:
            customers = (
                data.get("users")       # ← THIS was the missing key!
                or data.get("accounts")
                or data.get("customers")
                or data.get("results")
                or data.get("data")
                or []
            )

        if not customers:
            print(f"No more results on page {page}. Done.")
            break

        all_customers.extend(customers)
        print(f"  Page {page}: {len(customers)} companies  (total: {len(all_customers)})")

        # Pagination — stop if fewer than 100 results (last page)
        if isinstance(data, dict):
            total_pages = data.get("total_pages") or data.get("pages")
            if total_pages and page >= int(total_pages):
                print(f"  Reached last page ({total_pages}). Done.")
                break

        if len(customers) < 100:
            break

        page += 1

    return all_customers


def save_json(customers):
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "total":       len(customers),
            "exported_at": datetime.utcnow().isoformat(),
            "customers":   customers
        }, f, indent=2)
    print(f"\n✅ JSON saved → {OUTPUT_JSON}")


def save_csv(customers):
    if not customers:
        return
    fieldnames = sorted({key for c in customers for key in c.keys()})
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(customers)
    print(f"✅ CSV saved  → {OUTPUT_CSV}")


def print_sample(customers, n=5):
    print(f"\n── Sample ({min(n, len(customers))} of {len(customers)} companies) ──")
    for c in customers[:n]:
        name   = c.get("company_name") or c.get("account_name") or c.get("name") or "Unknown"
        domain = c.get("domain") or c.get("website") or "—"
        print(f"  • {name:45s}  {domain}")


if __name__ == "__main__":
    customers = fetch_all_customers()

    if customers:
        print_sample(customers)
        save_json(customers)
        save_csv(customers)
        print(f"\nTotal companies extracted: {len(customers)}")
    else:
        print("\n⚠️  No customers returned.")
        print("Check the 'Top-level keys' line above to see what the API returned.")