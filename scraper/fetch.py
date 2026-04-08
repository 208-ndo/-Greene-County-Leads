import asyncio
import json
import os
import csv
import subprocess
import sys
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from dbfread import DBF
from playwright.async_api import async_playwright

# ==========================================
# AUTO-INSTALLER: Fixes the "ModuleNotFoundError" automatically
# ==========================================
try:
    from playwright_stealth import stealth
except ImportError:
    print("[!] Stealth module missing. Installing now...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright-stealth"])
    from playwright_stealth import stealth

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
        print(f"[*] Checking Parcel Data...")
        try:
            r = requests.get(url, timeout=20)
            if r.status_code != 200: return
            with open("temp_parcels.dbf", "wb") as f: f.write(r.content)
            table = DBF("temp_parcels.dbf", load=True)
            for record in table:
                name = str(record.get('OWNER', record.get('OWN1', ''))).strip().upper()
                if not name: continue
                self.lookup[name] = {"prop_addr": record.get('SITE_ADDR', ''), "mail_addr": record.get('ADDR_1', '')}
        except: pass

    def get_address(self, name):
        return self.lookup.get(str(name).strip().upper(), {})

async def scrape_clerk():
    async with async_playwright() as p:
        print("[*] Launching FINAL-STEALTH Browser...")
        # Using a slightly different browser launch to avoid detection
        browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"]) 
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        
        # APPLY STEALTH PLUGIN - Fixed call
        await stealth(page)
        
        parcel_sys = ParcelLookup(PARCEL_DATA_URL)
        all_records = []
        
        end_date = datetime.now().strftime("%m/%d/%Y")
        start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
        
        for doc_label, doc_code in DOC_TYPES.items():
            print(f"[*] Checking {doc_label}...", end=" ")
            try:
                await asyncio.sleep(4) # Extra delay to look more human
                await page.goto(CLERK_PORTAL_URL, wait_until="networkidle")
                
                # Captcha check
                page_content = await page.content()
                if "recaptcha" in page_content.lower() or "g-recaptcha" in page_content.lower():
                    print("CAPTCHA DETECTED. GitHub IP is blocked.")
                    break

                # SMART SEARCH FOR DROPDOWN
                dropdown = None
                for selector in ['select[name="doc_type"]', 'select', '.dropdown']:
                    try:
                        dropdown = await page.wait_for_selector(selector, timeout=5000)
                        if dropdown: break
                    except: continue
                
                if not dropdown:
                    print("FAILED (No search box).")
                    continue
                
                await dropdown.select_option(label=doc_label)
                
                # Find date inputs by type
                date_inputs = await page.query_selector_all('input[type="text"]')
                if len(date_inputs) >= 2:
                    await date_inputs[0].fill(start_date)
                    await date_inputs[1].fill(end_date)
                
                # Find the search button
                search_btn = await page.query_selector('input[type="submit"]')
                if search_btn:
                    await search_btn.click()
                else:
                    print("No search button found.")
                    continue
                
                try:
                    await page.wait_for_selector('#resultsTable', timeout=15000)
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
                    all_records.append({"owner": owner, "prop_address": addr.get("prop_addr", ""), "cat_label": doc_label})
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
                             r['cat_label'], "Unknown", "Unknown", "0", "30", "Lead Found", "Greene County", ""])

async def main():
    print("[*] Starting FlowX Final-Stealth...")
    records = await scrape_clerk()
    output = {"fetched_at": datetime.now().isoformat(), "total": len(records), "records": records}
    os.makedirs("dashboard", exist_ok=True); os.makedirs("data", exist_ok=True)
    with open(OUTPUT_JSON_DASHBOARD, 'w') as f: json.dump(output, f, indent=4)
    with open(OUTPUT_JSON_DATA, 'w') as f: json.dump(output, f, indent=4)
    export_ghl(records)
    print(f"[+] DONE. Found {len(records)} records.")

if __name__ == "__main__":
    asyncio.run(main())
