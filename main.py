import requests
import time
import csv
import google.generativeai as genai

# --- CONFIGURATION ---
APOLLO_API_KEY = 
GEMINI_API_KEY = 

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

PRIMARY_TITLES = [
    "Head of Customer", "VP of Customer", "Director of Customer", 
    "Senior Director Customer", "Chief Customer", "Head of CX", 
    "VP of CX", "Director of CX", "Senior Manager CX", 
    "Head of Experience", "VP Experience"
]
FALLBACK_TITLES = ["CEO", "Founder", "Co-Founder"]
SUBJECT_LINE = "Qualtrics shut down Delighted. We built the replacement."

# --- HELPER FUNCTIONS ---

def get_domains_from_csv(file_path, domain_column_name="Domain"):
    """Reads your local CSV and extracts a list of company domains."""
    domains = []
    try:
        with open(file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row.get(domain_column_name):
                    raw_domain = row[domain_column_name].strip()
                    clean_domain = raw_domain.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
                    domains.append(clean_domain)
        return domains
    except Exception as e:
        print(f"Error reading local CSV: {e}")
        return []

def search_apollo_leads(titles, domain):
    """Searches Apollo for specific titles within a SINGLE domain. Tech filter removed."""
    url = "https://api.apollo.io/v1/mixed_people/api_search"
    payload = {
        "api_key": APOLLO_API_KEY,
        "person_titles": titles,
        "q_organization_domains": domain,
        "page": 1,
        "per_page": 5 
    }
    
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json().get('people', [])
    return []

def enrich_lead(person_id):
    """Spends 1 credit to reveal email and deeper context."""
    url = "https://api.apollo.io/v1/people/match"
    payload = {
        "api_key": APOLLO_API_KEY,
        "id": person_id,
        "reveal_personal_emails": True 
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json().get('person', {})
    return {}

def generate_personalized_intro(first_name, title, company, bio):
    prompt = f"""
    You are an SDR writing a highly personalized opening line for a cold email.
    Target: {first_name}, the {title} at {company}.
    Company Context: {bio}
    
    Write exactly ONE personalized opening sentence (max 25 words). 
    It must mention something specific about what their company does based on the context.
    Do NOT include a greeting like "Hi Name". 
    Do NOT offer a product. 
    The very next sentence in the email will be: "Elvan replaces Delighted cleanly: NPS and CSAT surveys you can trigger..." 
    Make sure your opening line flows naturally into that next sentence.
    """
    try:
        response = model.generate_content(prompt)
        return response.text.strip().replace('"', '')
    except Exception as e:
        return f"Noticed {company} has been scaling its operations recently."

# --- MAIN EXECUTION ---

def run_campaign_builder():
    # Make sure this matches your exact filename
    file_name = 'Delighted - 11-50 employees.csv'
    target_domains = get_domains_from_csv(file_name, domain_column_name='Domain')
    
    if not target_domains:
        print(f"No domains found. Check if '{file_name}' is in the exact same folder as this script.")
        return

    smartlead_data = []
    
    print(f"Loaded {len(target_domains)} domains from {file_name}. Starting automated search...\n")

    for domain in target_domains:
        print(f"Scouting: {domain}")
        
        leads = search_apollo_leads(PRIMARY_TITLES, domain)
        
        if not leads:
            print(f"  -> No primary CX titles found. Trying Fallback titles...")
            leads = search_apollo_leads(FALLBACK_TITLES, domain)
            
        if not leads:
            print(f"  -> No suitable contacts found for {domain}. Skipping.")
            continue
            
        for lead in leads:
            if lead.get('linkedin_url'):
                print(f"  -> Found {lead.get('title')}: {lead.get('first_name')}. Enriching...")
                
                enriched_person = enrich_lead(lead['id'])
                email = enriched_person.get('email')
                
                if email:
                    first_name = enriched_person.get('first_name', 'there')
                    last_name = enriched_person.get('last_name', '')
                    title = enriched_person.get('title', 'Leader')
                    company_name = enriched_person.get('organization', {}).get('name', domain)
                    company_bio = enriched_person.get('organization', {}).get('short_description', 'a growing business')
                    
                    custom_intro = generate_personalized_intro(first_name, title, company_name, company_bio)
                    
                    smartlead_data.append({
                        "First Name": first_name,
                        "Last Name": last_name,
                        "Email": email,
                        "Company Name": company_name,
                        "Personalized Intro": custom_intro,
                        "Subject Line": SUBJECT_LINE
                    })
                    print(f"  -> Success! Generated intro for {email}")
                    time.sleep(1) # Safety pause for API limits
                    break 
                else:
                    print("  -> Could not find a verified email. Moving on.")
                    time.sleep(1)

    # EXPORT TO SMARTLEAD
    if smartlead_data:
        csv_headers = ["First Name", "Last Name", "Email", "Company Name", "Personalized Intro", "Subject Line"]
        with open('smartlead_import.csv', 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=csv_headers)
            dict_writer.writeheader()
            dict_writer.writerows(smartlead_data)
        print(f"\nFinished! Saved {len(smartlead_data)} leads to smartlead_import.csv")
    else:
        print("\nNo valid leads were found.")

if __name__ == "__main__":
    run_campaign_builder()