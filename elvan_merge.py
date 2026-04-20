import sys
import pandas as pd
import os
import re
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

WORK_DIR = Path(r"c:\Users\resoa\Videos\Elvan-click")

# Target _Report CSVs only
REPORT_FILES = [f for f in WORK_DIR.glob("*.csv") if "_Report" in f.name or "Report" in f.name]

def load_csv(path):
    for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(path, encoding=enc)
            return df
        except Exception:
            continue
    return None

def extract_domain(email):
    if pd.isna(email) or "@" not in str(email):
        return ""
    return str(email).split("@")[-1].strip().lower()

def extract_company_hint(domain):
    # Strip TLD suffixes for a rough company name
    parts = domain.split(".")
    if len(parts) >= 2:
        return parts[-2]  # e.g. uplinkinternet from uplinkinternet.net
    return domain

GENERIC_PREFIXES = {"info", "hello", "support", "contact", "admin", "sales", "help", "team", "noreply", "no-reply", "enquiries", "enquiry"}

all_clickers = []
campaign_counts = {}
total_unsubscribed = 0
total_replied = 0
total_rows = 0

for fpath in sorted(REPORT_FILES):
    df = load_csv(fpath)
    if df is None:
        print(f"  ⚠️  Could not load: {fpath.name}")
        continue

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    required = {"Lead Name", "Lead Email", "Clicked Time"}
    if not required.issubset(set(df.columns)):
        print(f"  ⚠️  Skipping {fpath.name} — missing expected columns (found: {list(df.columns)[:6]})")
        continue

    total_rows += len(df)

    # Filter unsubscribed
    if "Is Unsubscribed" in df.columns:
        unsub_mask = df["Is Unsubscribed"].astype(str).str.upper().isin(["TRUE", "1", "YES"])
        total_unsubscribed += unsub_mask.sum()
        df = df[~unsub_mask]

    # Filter already replied
    if "Replied Time" in df.columns:
        replied_mask = df["Replied Time"].notna() & (df["Replied Time"].astype(str).str.strip() != "")
        total_replied += replied_mask.sum()
        df = df[~replied_mask]

    # Keep only clickers
    clicked_mask = df["Clicked Time"].notna() & (df["Clicked Time"].astype(str).str.strip() != "")
    clickers = df[clicked_mask].copy()

    campaign_name = fpath.stem  # filename without extension
    clickers["Source Campaign"] = campaign_name
    campaign_counts[campaign_name] = len(clickers)

    all_clickers.append(clickers)

if not all_clickers:
    print("No clicker data found across all CSVs.")
    exit(1)

merged = pd.concat(all_clickers, ignore_index=True)
before_dedup = len(merged)

# Deduplicate by email — keep first occurrence (preserves campaign attribution order)
merged = merged.drop_duplicates(subset=["Lead Email"], keep="first")
duplicates_removed = before_dedup - len(merged)

# Extract fields
def split_name(full_name):
    if pd.isna(full_name) or str(full_name).strip() == "":
        return "", ""
    parts = str(full_name).strip().split(" ", 1)
    first = parts[0]
    last = parts[1] if len(parts) > 1 else ""
    return first, last

merged["First Name"] = merged["Lead Name"].apply(lambda n: split_name(n)[0])
merged["Last Name"] = merged["Lead Name"].apply(lambda n: split_name(n)[1])
merged["Email"] = merged["Lead Email"]
merged["Domain"] = merged["Lead Email"].apply(extract_domain)
merged["Company"] = merged["Domain"].apply(extract_company_hint)

# Flag generic emails
merged["Generic Flag"] = merged["Email"].apply(
    lambda e: "GENERIC" if str(e).split("@")[0].lower() in GENERIC_PREFIXES else ""
)

click_col = "Click Count" if "Click Count" in merged.columns else None

output_cols = ["First Name", "Last Name", "Email", "Company", "Domain", "Source Campaign"]
if click_col:
    output_cols.append("Click Count")
output_cols.append("Generic Flag")

output = merged[output_cols].copy()
if click_col:
    output["Click Count"] = pd.to_numeric(output["Click Count"], errors="coerce").fillna(1).astype(int)

out_path = WORK_DIR / "apollo_ready_clickers.csv"
output.to_csv(out_path, index=False)

# --- Print summary ---
print("\n✅ elvan-merge complete\n")
print("📊 Campaign Summary:")
for campaign, count in sorted(campaign_counts.items(), key=lambda x: -x[1]):
    print(f"  {campaign:<45} →  {count} clickers")

print(f"\nTotal clickers merged:   {len(output)}")
print(f"Duplicates removed:       {duplicates_removed}")
print(f"Unsubscribed skipped:     {total_unsubscribed}")
print(f"Already replied skipped:  {total_replied}")

if click_col:
    hot = output[output["Click Count"] >= 2].sort_values("Click Count", ascending=False)
    if not hot.empty:
        print(f"\n🔥 Hottest leads (clicked 2+ times):")
        for _, row in hot.iterrows():
            print(f"  {row['Email']:<40} — clicked {int(row['Click Count'])}x  ({row['Source Campaign']})")

generics = output[output["Generic Flag"] == "GENERIC"]
if not generics.empty:
    print(f"\n⚠️  Generic emails flagged (kept, review before outreach):")
    for _, row in generics.iterrows():
        print(f"  {row['Email']}")

print(f"\n📁 Output: {out_path.name}")
print("\n👉 Next step: Upload to Apollo → find matching contacts → export enriched CSV → run /elvan-followup")
