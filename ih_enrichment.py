"""
ih_enrichment.py
================
Enriches Indie Hackers product CSV with founder data from Apollo.io.

Takes your cleaned IH CSV → searches Apollo by company domain
→ returns founder name, title, LinkedIn URL, email status.

Requirements:
    pip install requests pandas

Usage:
    1. Set your APOLLO_API_KEY below
    2. Run: python ih_enrichment.py

Input:
    indiehackers_products_2000plus.csv   (your cleaned IH CSV)

Output:
    ih_enriched.csv   (original columns + Apollo founder data)
"""

import requests
import pandas as pd
import time
import re
import os

# ── Config ─────────────────────────────────────────────────────────────────────
APOLLO_API_KEY = "YOUR_APOLLO_API_KEY_HERE"   # ← paste your key here
INPUT_FILE     = "indiehackers_products_2000plus.csv"
OUTPUT_FILE    = "ih_enriched.csv"
DELAY_SECONDS  = 0.5    # polite delay between API calls

APOLLO_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/search"

FOUNDER_TITLES = [
    "founder", "co-founder", "cofounder",
    "ceo", "chief executive officer",
    "owner", "creator", "maker",
    "indie hacker", "solopreneur",
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def extract_domain(url):
    """Pull clean domain from a URL string."""
    if not url or pd.isna(url):
        return None
    url = str(url).strip()
    match = re.search(r'https?://(?:www\.)?([^/?\s]+)', url)
    return match.group(1).lower() if match else None


def search_apollo(domain, company_name):
    """Search Apollo for founders at a given domain."""
    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": APOLLO_API_KEY,
    }

    payload = {
        "q_organization_domains_list": [domain],
        "person_titles": FOUNDER_TITLES,
        "per_page": 5,
        "page": 1,
    }

    try:
        resp = requests.post(APOLLO_SEARCH_URL, json=payload, headers=headers, timeout=15)
        if resp.status_code == 429:
            print(f"    Rate limited — waiting 10s...")
            time.sleep(10)
            resp = requests.post(APOLLO_SEARCH_URL, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("people", [])
    except Exception as e:
        print(f"    Apollo error for {domain}: {e}")
        return []


def format_people(people):
    """Flatten Apollo people results into CSV-friendly strings."""
    if not people:
        return {
            "apollo_founder_names":    "",
            "apollo_founder_titles":   "",
            "apollo_linkedin_urls":    "",
            "apollo_email_status":     "",
            "apollo_locations":        "",
            "apollo_match_count":      0,
        }

    names    = []
    titles   = []
    linkedins = []
    statuses = []
    locations = []

    for p in people:
        names.append(p.get("name", ""))
        titles.append(p.get("title", ""))
        linkedins.append(p.get("linkedin_url", ""))
        statuses.append(p.get("email_status", ""))
        city    = p.get("city", "")
        country = p.get("country", "")
        locations.append(f"{city}, {country}".strip(", "))

    return {
        "apollo_founder_names":  " | ".join(filter(None, names)),
        "apollo_founder_titles": " | ".join(filter(None, titles)),
        "apollo_linkedin_urls":  " | ".join(filter(None, linkedins)),
        "apollo_email_status":   " | ".join(filter(None, statuses)),
        "apollo_locations":      " | ".join(filter(None, locations)),
        "apollo_match_count":    len(people),
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def enrich():
    print(f"\nLoading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df):,} products\n")

    # Extract domain from websiteUrl
    df["domain"] = df["websiteUrl"].apply(extract_domain)

    # Skip rows with no domain
    has_domain = df["domain"].notna() & (df["domain"] != "")
    print(f"Products with a website domain: {has_domain.sum():,}")
    print(f"Products without domain (will be skipped): {(~has_domain).sum():,}\n")

    # Add enrichment columns
    enriched_cols = [
        "apollo_founder_names", "apollo_founder_titles",
        "apollo_linkedin_urls", "apollo_email_status",
        "apollo_locations", "apollo_match_count",
    ]
    for col in enriched_cols:
        df[col] = ""
    df["apollo_match_count"] = 0

    # Resume support — skip already enriched rows
    done = 0
    skipped = 0

    for idx, row in df.iterrows():
        domain = row.get("domain")
        name   = row.get("name", "")

        if not domain:
            skipped += 1
            continue

        print(f"  [{done+1}/{has_domain.sum()}] {name[:35]:35s} → {domain}", end="", flush=True)

        people = search_apollo(domain, name)
        result = format_people(people)

        for col, val in result.items():
            df.at[idx, col] = val

        found = result["apollo_match_count"]
        print(f"  {'✓ ' + str(found) + ' founder(s) found' if found else '— no match'}")

        done += 1
        time.sleep(DELAY_SECONDS)

        # Save progress every 50 rows
        if done % 50 == 0:
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"\n  [Progress saved — {done} done]\n")

    # Final save
    df.to_csv(OUTPUT_FILE, index=False)
    matched = (df["apollo_match_count"] > 0).sum()
    print(f"\nDone! {matched:,}/{len(df):,} products matched on Apollo")
    print(f"Saved → {OUTPUT_FILE}")


if __name__ == "__main__":
    if APOLLO_API_KEY == "YOUR_APOLLO_API_KEY_HERE":
        print("ERROR: Set your APOLLO_API_KEY in the script first!")
        print("  Get it from: https://app.apollo.io/#/settings/integrations/api-keys")
    else:
        enrich()
