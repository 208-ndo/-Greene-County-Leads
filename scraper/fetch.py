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
# REPLACE THIS with the actual direct link to the .dbf file from the Assessor's site
PARCEL_DATA_URL = "https://greenecountymo.gov/assessor/bulk_data/parcels.dbf" 

LOOKBACK_DAYS = 365
OUTPUT_JSON_DASHBOARD = "dashboard/records.json"
OUTPUT_JSON_DATA = "data/records.json"
OUTPUT_CSV_GHL = "ghl_export.csv"

DOC_TYPES = {
    "LPEN - LIS PENDENS": "LP",
    "NOFC - NOTICE OF FORECLOSURE": "NOFC",
    "TAXDEED - TAX DEED": "TAXDEED",
    "JUD - JUDGMENT": "JUD",
    "CCJ - CERTIFIED JUDGMENT": "JUD",
    "DRJUD - DOMESTIC JUDGMENT": "JUD",
    "LNCORPTX - CORP TAX LIEN": "LNCORPTX",
    "LNIRS - IRS LIEN": "LNIRS",
    "LNFED - FEDERAL LIEN": "LNFED",
    "LN - LIEN": "LN",
    "LNMECH - MECHANIC LIEN": "LNMECH",
    "LNHOA - HOA LIEN": "LNHOA",
    "MEDLN - MEDICAID LIEN": "MEDLN",
    "PROBATE - PROBATE": "PRO",
    "NOC - NOTICE OF COMMENCEMENT": "NOC",
    "RELLP - RELEASE LIS PENDENS": "RELLP"
}

# ==========================================
# HELPER: DATA LOOKUP
# ==========================================
class ParcelLookup:
    def __init__(self, dbf_url):
        self.lookup = {}
        self.download_and_process(dbf_url)

    def download_and_process(self, url):
        print(f"[*] Downloading bulk parcel data from {url}...")
        try:
            r = requests.get(url, timeout=30)
            with open("temp_parcels.dbf", "wb") as f:
                f.write(r.content)
            
            table = DBF("temp_parcels.dbf", load=True)
            for record in table:
                # Normalize name variants for lookup
                name = str(record.get('OWNER', record.get('OWN1', ''))).strip().upper()
                if not name: continue
                
                # Store variants: "FIRST LAST", "LAST, FIRST", "LAST FIRST"
                variants = [name]
                if ',' in name:
                    variants.append(name.replace(',', '').strip())
                else:
                    parts = name.split(' ')
                    if len(parts) >= 2:
                        variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
                
                for v in variants:
                    self.lookup[v] = {
                        "prop_addr": record.get('SITE_ADDR', record.get('SITEADDR', '')),
                        "prop_city": record.get('SITE_CITY', ''),
                        "prop_zip": record.get('SITE_ZIP', ''),
                        "mail_addr": record.get('ADDR_1', record.get('MAILADR1', '')),
                        "mail_city": record.get('CITY', record.get('MAILCITY', '')),
                        "mail_state": record.get('STATE', 'MO'),
                        "mail_zip": record.get('ZIP', record.get('MAILZIP', ''))
                    }
            print(f"[+] Loaded {len(self.lookup)} owner records into memory.")
        except Exception as e:
            print(f"[!] Error processing DBF: {e}")

    def get_address(self, name):
        name = str(name).strip().upper()
        return self.lookup.get(name, {})

# ==========================================
# SCORING ENGINE
# ==========================================
def calculate_score(record):
    score = 30
    flags = []
    
    cat = record['cat']
    if cat == "LP": flags.append("Lis pendens"); score += 10
    if cat == "NOFC": flags.append("Pre-foreclosure"); score += 10
    if cat == "JUD": flags.append("Judgment lien"); score += 10
    if cat == "TAXDEED" or cat == "LNCORPTX": flags.append("Tax lien"); score += 10
    if cat == "LNMECH": flags.append("Mechanic lien"); score += 10
    if cat == "PRO": flags.append("Probate / estate"); score += 10
    
    if "LLC" in record['owner'].upper() or "CORP" in record['owner'].upper():
        flags.append("LLC / corp owner"); score += 10
    
    # High Value Debt
    try:
        amt = float(record['amount'].replace('$', '').replace(',', ''))
        if amt > 100000: score += 15
        elif amt > 50000: score += 10
    except: pass

    if record['prop_address']: score += 5
    
    return score, flags

# ==========================================
# MAIN SCRAPER
# ==========================================
async def scrape_clerk():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        parcel_sys = ParcelLookup(PARCEL_DATA_URL)
        all_records = []
        
        end_date = datetime.now().strftime("%m/%d/%Y")
        start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
        
        for doc_label, doc_code in DOC_TYPES.items():
            print(f"[*] Scraping {doc_label}...")
            try:
                await page.goto(CLERK_PORTAL_URL)
                await page.select_option('select[name="doc_type"]', label=doc_label)
                await page.fill('input[name="begin_date"]', start_date)
                await page.fill('input[name="end_date"]', end_date)
                await page.click('input[type="submit"]')
                
                await page.wait_for_load_state("networkidle")
                
                content = await page.content()
                soup = BeautifulSoup(content, 'lxml')
                table = soup.find('table', {'id': 'resultsTable'}) # Update ID based on actual site
                
                if not table:
                    continue
                
                rows = table.find_all('tr')[1:] # Skip header
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 5: continue
                    
                    owner_name = cols[2].text.strip()
                    address_data = parcel_sys.get_address(owner_name)
                    
                    rec = {
                        "doc_num": cols[0].text.strip(),
                        "doc_type": doc_label,
                        "filed": cols[1].text.strip(),
                        "cat": doc_code,
                        "cat_label": doc_label,
                        "owner": owner_name,
                        "grantee": cols[3].text.strip(),
                        "amount": cols[4].text.strip() if len(cols)>4 else "0",
                        "legal": cols[5].text.strip() if len(cols)>5 else "",
                        "prop_address": address_data.get("prop_addr", ""),
                        "prop_city": address_data.get("prop_city", ""),
                        "prop_state": address_data.get("prop_state", "MO"),
                        "prop_zip": address_data.get("prop_zip", ""),
                        "mail_address": address_data.get("mail_addr", ""),
                        "mail_city": address_data.get("mail_city", ""),
                        "mail_state": address_data.get("mail_state", "MO"),
                        "mail_zip": address_data.get("mail_zip", ""),
                        "clerk_url": page.url,
                    }
                    
                    score, flags = calculate_score(rec)
                    rec['score'] = score
                    rec['flags'] = flags
                    all_records.append(rec)
                    
            except Exception as e:
                print(f"[!] Error scraping {doc_label}: {e}")
        
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
            
            writer.writerow([
                first, last, r['mail_address'], r['mail_city'], r['mail_state'], r['mail_zip'],
                r['prop_address'], r['prop_city'], r['prop_state'], r['prop_zip'],
                r['cat_label'], r['doc_type'], r['filed'], r['doc_num'], r['amount'],
                r['score'], ", ".join(r['flags']), "Greene County", r['clerk_url']
            ])

async def main():
    print("[*] Starting FlowX Lead Engine...")
    records = await scrape_clerk()
    
    output = {
        "fetched_at": datetime.now().isoformat(),
        "source": "Greene County",
        "date_range": f"{LOOKBACK_DAYS} days",
        "total": len(records),
        "with_address": len([r for r in records if r['mail_address']]),
        "records": records
    }
    
    os.makedirs("dashboard", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    
    with open(OUTPUT_JSON_DASHBOARD, 'w') as f: json.dump(output, f, indent=4)
    with open(OUTPUT_JSON_DATA, 'w') as f: json.dump(output, f, indent=4)
    
    export_ghl(records)
    print(f"[+] Success. {len(records)} leads captured. GHL CSV exported.")

if __name__ == "__main__":
    asyncio.run(main())
