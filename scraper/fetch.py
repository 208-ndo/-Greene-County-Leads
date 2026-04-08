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
                print(f"[!] DBF FAILURE: Server returned {r.status_code}. Addresses will be blank.")
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
    return 30, ["Lead Found"]

async def scrape_clerk():
    async with async_playwright() as p:
        print("[*] Launching Smart-Selector Browser...")
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
                
                # SMART SELECTOR: Try multiple ways to find the dropdown
                dropdown = None
                selectors = ['select[name="doc_type"]', 'select#doc_type', 'select']
                for s in selectors:
                    try:
                        dropdown = await page.wait_for_selector(s, timeout=5000)
                        if dropdown: break
                    except: continue
                
                if not dropdown:
                    print("FAILED (Selector not found).")
                    continue
                
                await dropdown.select_option(label=doc_label)
                
                # SMART FILL: Find date inputs regardless of name
                date_inputs = await page.query_selector_all('input[type="text"]')
                if len(date_inputs) >= 2:
                    await date_inputs[0].fill(start_date)
                    await date_inputs[1].fill(end_date)
                
                await page.click('input[type="submit"]')
                
                try:
                    await page.wait_for_selector('#resultsTable', timeout=10000)
                    print("Found data!")
                except:
                    print("0 found.")
                    continue
                
                soup = BeautifulSoup(await page.content(), 'lxml')
                table = soup.find('table', {'id': 'resultsTable'})
                if not table: continue
                    
                rows = table.find_all('tr')[1:]
                found_count = 0
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 5: continue
                    owner = cols[2].text.strip()
                    addr = parcel_sys.get_address(owner)
                    all_records.append({
                        "owner": owner, "prop_address": addr.get("prop_addr", ""), 
                        "cat_label": doc_label, "filed": cols[1].text.strip(),
                        "doc_num": cols[0].text.strip(), "amount": cols[4].text.strip() if len(cols)>4 else "0"
                    })
                    found_count += 1
                print(f"{found_count} found.")
            except Exception as e: print(f"Error: {e}")
        
        await browser.close()
        return all_records

def export_ghl(records):
    with open(OUTPUT_CSV_GHL, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["First Name", "Last Name", "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip", 
                         "Property Address", "Property City", "Property State", "Property Zip", "Lead Type", 
                         "Document Type", "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score", 
                         "Motivated Seller Flags", "Source", "Public Records URL"])
        for r in records:
            name_parts = r['owner'].split(' ', 1)
            first = name_parts[0] if len(name_parts) > 0 else ""
            last = name_parts[1] if len(name_parts) > 1 else ""
            writer.writerow([first, last, "", "", "MO", "", r['prop_address'], "", "MO", "", r['cat_label'], 
                             r['cat_label'], r['filed'], r['doc_num'], r['amount'], "30", "Lead Found", "Greene County", ""])

async def main():
    print("[*] Starting FlowX Smart-Scrape...")
    records = await scrape_clerk()
    output = {"fetched_at": datetime.now().isoformat(), "total": len(records), "records": records}
    os.makedirs("dashboard", exist_ok=True); os.makedirs("data", exist_ok=True)
    with open(OUTPUT_JSON_DASHBOARD, 'w') as f: json.dump(output, f, indent=4)
    with open(OUTPUT_JSON_DATA, 'w') as f: json.dump(output, f, indent=4)
    export_ghl(records)
    print(f"[+] DONE. Found {len(records)} records.")

if __name__ == "__main__":
    asyncio.run(main())
