import asyncio
import json
import os
import csv
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from dbfread import DBF
from playwright.async_api import async_playwright

# ==========================================
# CONFIGURATION
# ==========================================
CLERK_PORTAL_URL = "https://greenecountymo.gov/recorder/real_estate_search/type.php"
PARCEL_DATA_URL = "https://greenecountymo.gov/assessor/bulk_data/parcels.dbf" 

LOOKBACK_DAYS = 365 
OUTPUT_JSON_DASHBOARD = "dashboard/records.json"
OUTPUT_JSON_DATA = "data/records.json"
OUTPUT_CSV_GHL = "ghl_export.csv"

DOC_TYPES = {
    "LPEN - LIS PENDENS": "LP", "NOFC - NOTICE OF FORECLOSURE": "NOFC",
    "TAXDEED - TAX DEED": "TAXDEED", "JUD - JUDGMENT": "JUD",
    "CCJ - CERTIFIED JUDGMENT": "JUD", "DRJUD - DOMESTIC JUDGMENT": "JUD",
    "LNCORPTX - CORP TAX LIEN": "LNCORPTX", "LNIRS - IRS LIEN": "LNIRS",
    "LNFED - FEDERAL LIEN": "LNFED", "LN - LIEN": "LN",
    "LNMECH - MECHANIC LIEN": "LNMECH", "LNHOA - HOA LIEN": "LNHOA",
    "MEDLN - MEDICAID LIEN": "MEDLN", "PROBATE - PROBATE": "PRO",
    "NOC - NOTICE OF COMMENCEMENT": "NOC", "RELLP - RELEASE LIS PENDENS": "RELLP"
}

class ParcelLookup:
    def __init__(self, dbf_url):
        self.lookup = {}
        self.download_and_process(dbf_url)

    def download_and_process(self, url):
        print(f"[*] Checking Parcel Data: {url}")
        try:
            r = requests.get(url, timeout=30)
            if r.status_code != 200:
                print(f"[!] DBF FAILURE: Server returned {r.status_code}. The link might be dead.")
                return
            with open("temp_parcels.dbf", "wb") as f: f.write(r.content)
            table = DBF("temp_parcels.dbf", load=True)
            count = 0
            for record in table:
                name = str(record.get('OWNER', record.get('OWN1', ''))).strip().upper()
                if not name: continue
                self.lookup[name] = {"prop_addr": record.get('SITE_ADDR', ''), "mail_addr": record.get('ADDR_1', '')}
                count += 1
            print(f"[+] SUCCESS: Loaded {count} owners.")
        except Exception as e: print(f"[!] DBF ERROR: {e}")

    def get_address(self, name):
        return self.lookup.get(str(name).strip().upper(), {})

def calculate_score(record):
    return 30, ["Diagnostic Run"]

async def scrape_clerk():
    async with async_playwright() as p:
        print("[*] Launching Log-Diagnostic Browser...")
        browser = await p.chromium.launch(headless=True) 
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0 Safari/537.36")
        page = await context.new_page()
        
        parcel_sys = ParcelLookup(PARCEL_DATA_URL)
        all_records = []
        
        end_date = datetime.now().strftime("%m/%d/%Y")
        start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
        
        for doc_label, doc_code in DOC_TYPES.items():
            print(f"[*] Checking {doc_label}...", end=" ")
            try:
                await asyncio.sleep(2)
                await page.goto(CLERK_PORTAL_URL, wait_until="domcontentloaded")
                
                # DIAGNOSTIC: Print what the bot actually sees
                title = await page.title()
                content = await page.content()
                print(f"\n    -> Page Title: {title}")
                print(f"    -> Page Snippet: {content[:500].replace('\n', ' ')}...")
                
                try:
                    await page.wait_for_selector('select[name="doc_type"]', timeout=10000)
                except:
                    print("    -> RESULT: Search box NOT found. Likely BLOCKED.")
                    break # Stop here, the others will fail too.
                
                await page.select_option('select[name="doc_type"]', label=doc_label)
                await page.fill('input[name="begin_date"]', start_date)
                await page.fill('input[name="end_date"]', end_date)
                await page.click('input[type="submit"]')
                
                try:
                    await page.wait_for_selector('#resultsTable', timeout=10000)
                    print("    -> Found data table!")
                except:
                    print("    -> 0 results found.")
                    continue
                
                soup = BeautifulSoup(await page.content(), 'lxml')
                rows = soup.find('table', {'id': 'resultsTable'}).find_all('tr')[1:]
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 5: continue
                    owner = cols[2].text.strip()
                    addr = parcel_sys.get_address(owner)
                    all_records.append({"owner": owner, "prop_address": addr.get("prop_addr", ""), "cat_label": doc_label})
                
            except Exception as e: print(f"Error: {e}")
        
        await browser.close()
        return all_records

async def main():
    print("[*] Starting FlowX Log-Diagnostic...")
    records = await scrape_clerk()
    output = {"fetched_at": datetime.now().isoformat(), "total": len(records), "records": records}
    os.makedirs("dashboard", exist_ok=True); os.makedirs("data", exist_ok=True)
    with open(OUTPUT_JSON_DASHBOARD, 'w') as f: json.dump(output, f, indent=4)
    with open(OUTPUT_JSON_DATA, 'w') as f: json.dump(output, f, indent=4)
    print(f"[+] DONE. Found {len(records)} records.")

if __name__ == "__main__":
    asyncio.run(main())
