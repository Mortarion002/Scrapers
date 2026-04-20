import requests
import csv
from datetime import datetime

# --- Configuration ---
API_KEY = # <-- Paste your API key here!
BASE_URL = "https://trustmrr.com/api/v1/startups" 

# Create a unique filename based on the exact date and time
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE = f"trustmrr_apollo_companies_{current_time}.csv"

def fetch_trustmrr_leads():
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    apollo_ready_companies = []
    
    # THE BOUNCER: A set to keep track of companies we've already seen
    seen_companies = set() 
    
    # THE PAGINATION TRACKER
    page = 1 
    max_pages = 5 # Let's limit it to 5 pages for safety

    print("Initiating TrustMRR API request with pagination...")
    
    while page <= max_pages:
        # We add the 'page' parameter to our request
        params = {
            "limit": 100,
            "page": page 
        }
        
        try:
            print(f"Fetching Page {page}...")
            response = requests.get(BASE_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            startups = data.get("data", []) 
            
            # If the API returns an empty list, we've reached the end of their database!
            if not startups:
                print("No more data found. Exiting pagination loop.")
                break 
            
            for startup in startups:
                company_name = startup.get("name", "Unknown")
                website = startup.get("website", "")
                
                # --- DEDUPLICATION CHECK ---
                # If the company is already in our set, skip the rest of this loop!
                if company_name in seen_companies:
                    continue 
                
                revenue_data = startup.get("revenue", {})
                mrr = revenue_data.get("mrr", 0)
                
                if mrr is not None and 5000 <= mrr <= 100000 and website:
                    # Add to our "Bouncer" list so we never add it again
                    seen_companies.add(company_name)
                    
                    x_handle = startup.get("xHandle", "")
                    twitter_url = f"https://twitter.com/{x_handle}" if x_handle else ""
                    
                    apollo_ready_companies.append({
                        "Company Name": company_name,
                        "Company Website": website,
                        "Twitter URL": twitter_url,
                        "MRR": f"${mrr:,.0f}"
                    })
            
            # Move on to the next page!
            page += 1

        except requests.exceptions.RequestException as e:
            print(f"API Connection Error on page {page}: {e}")
            break # Stop the loop if there's an error

    # This runs AFTER the while loop finishes
    print(f"\nExtraction complete. Found {len(apollo_ready_companies)} UNIQUE qualified companies across {page - 1} pages.")
    return apollo_ready_companies

def export_to_csv(companies, filename):
    if not companies:
        print("No companies matched the ICP criteria in this batch. Aborting export.")
        return

    # These headers map to Apollo's Company Import tool
    fieldnames = ["Company Name", "Company Website", "Twitter URL", "MRR"]
    
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(companies)
        
    print(f"Success! {len(companies)} companies exported to {filename}.")

if __name__ == "__main__":
    companies = fetch_trustmrr_leads()
    export_to_csv(companies, OUTPUT_FILE)