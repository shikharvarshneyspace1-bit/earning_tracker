import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
import time
from datetime import datetime, timedelta
import sys
import threading
from flask import Flask, render_template_string, request, jsonify
from jinja2 import DictLoader, ChoiceLoader
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# CONFIGURATION
# ==========================================
DATABASE_URL = os.getenv("DATABASE_URL") # This will come from Neon.tech
TELEGRAM_BOT_TOKEN = os.getenv("RESULT_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
CRON_SECRET_KEY = os.getenv("CRON_SECRET_KEY", "my_super_secret_cron_key_123")

app = Flask(__name__)
app.secret_key = "super_secret_key_for_flask_flashes"

def get_db_connection():
    """Connects to the remote PostgreSQL Database"""
    if not DATABASE_URL:
        raise ValueError("CRITICAL: DATABASE_URL environment variable is missing.")
    return psycopg2.connect(DATABASE_URL, sslmode='require')

# ==========================================
# 1. AUTO-QUARTER LOGIC
# ==========================================
def get_target_quarter():
    today = datetime.today()
    year = today.year
    month = today.month
    
    if month in [1, 2, 3]: return f"Dec {year - 1}"
    elif month in [4, 5, 6]: return f"Mar {year}"
    elif month in [7, 8, 9]: return f"Jun {year}"
    else: return f"Sep {year}"

TARGET_QUARTER = get_target_quarter()

def matches_target_quarter(headline, target_quarter):
    if not headline: return True
    hl_lower = headline.lower()
    month, year = target_quarter.split()
    
    # Expanded aliases to catch '31.3.26', 'Q4', '4th quarter', 'year ended', etc.
    month_map = {
        "Mar": ["mar", "03", ".3.", "-3-", "/3/", "q4", "4th quarter", "fourth quarter", "year ended", "annual"], 
        "Jun": ["jun", "06", ".6.", "-6-", "/6/", "q1", "1st quarter", "first quarter"], 
        "Sep": ["sep", "09", ".9.", "-9-", "/9/", "q2", "2nd quarter", "second quarter", "half year"], 
        "Dec": ["dec", "12", "q3", "3rd quarter", "third quarter", "nine month"]
    }
    month_aliases = month_map.get(month, [month.lower()])
    has_month = any(m in hl_lower for m in month_aliases)
    
    # Check for full year "2026" or short year "26"
    has_year = (year in hl_lower) or (year[-2:] in hl_lower)
    
    if "result" in hl_lower and not (has_month and has_year): return False
    return True

# ==========================================
# DATABASE SETUP & UTILS
# ==========================================
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS earnings_tracker (
            Scrip_Code TEXT, Company_Name TEXT, Target_Quarter TEXT, BSE_Announcement_Date TEXT, Headline TEXT,
            Attachment_Name TEXT, NSE_Symbol TEXT, Screener_Status TEXT, Telegram_Status TEXT, Rating TEXT,
            Sales_YoY REAL, PAT_YoY REAL, Margin_Change REAL, Sales_CQ REAL, Sales_PQ REAL, Sales_YQ REAL,
            PAT_CQ REAL, PAT_PQ REAL, PAT_YQ REAL, Last_Checked TIMESTAMP, PRIMARY KEY (Scrip_Code, Target_Quarter)
        )
    ''')
    
    # Create Sectors mapping table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sectors (
            Identifier TEXT PRIMARY KEY,
            Sector_Name TEXT
        )
    ''')
    
    cols = ['Headline', 'Attachment_Name', 'NSE_Symbol', 'Sales_CQ', 'Sales_PQ', 'Sales_YQ', 
            'PAT_CQ', 'PAT_PQ', 'PAT_YQ', 'Sales_QoQ', 'PAT_QoQ', 'Margin_QoQ', 
            'Margin_CQ', 'Margin_PQ', 'Margin_YQ', 'Margin_Name']
    
    for col in cols:
        try:
            cursor.execute(f"ALTER TABLE earnings_tracker ADD COLUMN IF NOT EXISTS {col} TEXT")
        except Exception:
            conn.rollback()

    conn.commit()
    conn.close()

def delete_scrip_from_db(scrip_code):
    """Instead of actually deleting (which causes the scraper to re-fetch it), we softly mark it as Deleted."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE earnings_tracker SET Telegram_Status = 'Deleted' WHERE Scrip_Code = %s OR NSE_Symbol = %s", (str(scrip_code), str(scrip_code)))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    if deleted > 0: print(f"✅ Successfully hid {scrip_code} from the tracking database.")
    else: print(f"⚠️ {scrip_code} was not found in the database.")
    return deleted

# ==========================================
# 2. BSE FETCHING LOGIC
# ==========================================
def fetch_new_bse_announcements(start_date_str=None, end_date_str=None):
    print(f"Checking BSE for new announcements (Target Quarter: {TARGET_QUARTER})...")
    if not start_date_str or not end_date_str:
        today = datetime.today()
        yesterday = today - timedelta(days=1)
        start_date_str = yesterday.strftime("%Y%m%d")
        end_date_str = today.strftime("%Y%m%d")
    
    url = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.bseindia.com",
        "Referer": "https://www.bseindia.com/"
    }
    params = {"strCat": "Result", "strPrevDate": start_date_str, "strScrip": "", "strSearch": "P", "strToDate": end_date_str, "strType": "C", "pageno": 1}
    all_announcements = []
    
    with requests.Session() as session:
        session.get("https://www.bseindia.com/", headers=headers)
        while True:
            response = session.get(url, headers=headers, params=params)
            if response.status_code != 200: break
            try: data = response.json()
            except requests.exceptions.JSONDecodeError: break
            if not data or 'Table' not in data or len(data['Table']) == 0: break
            all_announcements.extend(data['Table'])
            params["pageno"] += 1
            time.sleep(1)
            
    if not all_announcements:
        print("No new announcements found.")
        return

    df_results = pd.DataFrame(all_announcements)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    added_count = 0
    for index, row in df_results.iterrows():
        scrip = str(row["SCRIP_CD"]); name = str(row["SLONGNAME"]); date = str(row["NEWS_DT"])
        headline = str(row["HEADLINE"]); attach = str(row["ATTACHMENTNAME"])
        
        if not matches_target_quarter(headline, TARGET_QUARTER): continue
        
        # Postgres uses %s instead of ? and ON CONFLICT to prevent duplicates
        cursor.execute('''
            INSERT INTO earnings_tracker 
            (Scrip_Code, Company_Name, Target_Quarter, BSE_Announcement_Date, Headline, Attachment_Name, Screener_Status, Telegram_Status, Last_Checked)
            VALUES (%s, %s, %s, %s, %s, %s, 'Pending', 'Unsent', %s)
            ON CONFLICT (Scrip_Code, Target_Quarter) DO NOTHING
        ''', (scrip, name, TARGET_QUARTER, date, headline, attach, datetime.now()))
        
        if cursor.rowcount > 0:
            added_count += 1

    conn.commit()
    conn.close()
    print(f"Added {added_count} new announcements to tracking queue.")

# ==========================================
# 3. SCREENER SCRAPING (IN-MEMORY)
# ==========================================
def scrape_screener(ticker_code):
    url = f"https://www.screener.in/company/{ticker_code}/"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200: return None
        soup = BeautifulSoup(response.text, 'html.parser')
        data = {"ticker_bse": ticker_code, "last_scraped": str(pd.Timestamp.now())}

        name_div = soup.find('h1')
        data['company_name'] = name_div.text.strip() if name_div else "Unknown"

        try:
            top_links = soup.find('div', class_='company-links')
            if top_links:
                nse_span = top_links.find('span', string=lambda t: t and 'NSE' in t)
                if nse_span: data['nse_symbol'] = nse_span.text.replace('NSE:', '').strip()
        except Exception: data['nse_symbol'] = None

        try:
            mcap = 0.0
            top_ratios = soup.find('ul', id='top-ratios')
            if top_ratios:
                for li in top_ratios.find_all('li'):
                    name_span = li.find('span', class_='name')
                    num_span = li.find('span', class_='number')
                    if name_span and num_span and 'Market Cap' in name_span.text:
                        mcap = float(num_span.text.replace(',', '').strip())
                        break
            data['market_cap'] = mcap
        except Exception: data['market_cap'] = 0.0

        sections = ['quarters', 'profit-loss', 'balance-sheet', 'cash-flow', 'ratios', 'shareholding']
        for sid in sections:
            section = soup.find('section', id=sid)
            if section:
                table = section.find('table', class_='data-table')
                if table:
                    headers = [th.text.strip() for th in table.find_all('th') if th.text.strip()]
                    rows = [[td.text.strip() for td in tr.find_all('td')] for tr in table.find_all('tr') if tr.find_all('td')]
                    data[f'{sid}_table'] = {"headers": headers, "rows": rows}
        return data 
    except Exception: return None

def check_screener_for_quarter(data, target_quarter):
    if not data or 'quarters_table' not in data: return False
    return target_quarter in data['quarters_table']['headers']

# ==========================================
# 4. EVALUATION & TELEGRAM
# ==========================================
def clean_num(val):
    if not val or val == '': return 0.0
    try: return float(str(val).replace(',', '').replace('%', ''))
    except: return 0.0

def evaluate_financials(data):
    if not data or 'quarters_table' not in data: return None
        
    headers = data['quarters_table']['headers']
    rows = data['quarters_table']['rows']
    CQ_idx = -1; PQ_idx = -2; YQ_idx = -5 
    
    metrics = {'Company': data.get('company_name', 'Unknown'), 'Quarter': headers[CQ_idx], 'Margin_Name': 'OPM'}
    metrics['PBT_CQ'] = 0.0; metrics['PBT_PQ'] = 0.0; metrics['PBT_YQ'] = 0.0; metrics['Tax_CQ'] = 0.0
    
    for row in rows:
        if not row: continue
        label = str(row[0]).replace('\xa0+', '').strip()
        
        if 'Sales' in label or 'Revenue' in label:
            metrics['Sales_CQ'] = clean_num(row[CQ_idx]); metrics['Sales_PQ'] = clean_num(row[PQ_idx]); metrics['Sales_YQ'] = clean_num(row[YQ_idx])
        elif 'OPM %' in label or 'Financing Margin %' in label:
            metrics['Margin_Name'] = 'Fin Margin' if 'Financing' in label else 'OPM'
            metrics['Margin_CQ'] = clean_num(row[CQ_idx]); metrics['Margin_PQ'] = clean_num(row[PQ_idx]); metrics['Margin_YQ'] = clean_num(row[YQ_idx])
        elif 'Profit before tax' in label:
            metrics['PBT_CQ'] = clean_num(row[CQ_idx]); metrics['PBT_PQ'] = clean_num(row[PQ_idx]); metrics['PBT_YQ'] = clean_num(row[YQ_idx])
        elif 'Net Profit' in label:
            metrics['PAT_CQ'] = clean_num(row[CQ_idx]); metrics['PAT_PQ'] = clean_num(row[PQ_idx]); metrics['PAT_YQ'] = clean_num(row[YQ_idx])
        elif 'Tax %' in label:
            metrics['Tax_CQ'] = clean_num(row[CQ_idx])
            
    def calc_growth(current, past):
        if past == 0: return 100.0 if current > 0 else 0.0
        return ((current - past) / abs(past)) * 100
        
    sales_yoy = calc_growth(metrics.get('Sales_CQ', 0), metrics.get('Sales_YQ', 0))
    sales_qoq = calc_growth(metrics.get('Sales_CQ', 0), metrics.get('Sales_PQ', 0))
    pbt_yoy = calc_growth(metrics.get('PBT_CQ', 0), metrics.get('PBT_YQ', 0))
    pat_yoy = calc_growth(metrics.get('PAT_CQ', 0), metrics.get('PAT_YQ', 0))
    pat_qoq = calc_growth(metrics.get('PAT_CQ', 0), metrics.get('PAT_PQ', 0))
    margin_yoy = metrics.get('Margin_CQ', 0) - metrics.get('Margin_YQ', 0) 
    margin_qoq = metrics.get('Margin_CQ', 0) - metrics.get('Margin_PQ', 0)
    
    count = 0
    if sales_yoy > 20: count += 1
    if sales_qoq > -10: count += 1
    if margin_yoy >= 0: count += 1
    if margin_qoq > -20: count += 1
    if metrics.get('Tax_CQ', 0) >= 0: count += 1
    if metrics.get('PBT_CQ', 0) > 0: count += 1
    if pbt_yoy > sales_yoy: count += 1
    if metrics.get('PAT_CQ', 0) > 0: count += 1
    if pat_yoy > 20: count += 1
    
    if count >= 8: rating = "Excellent"
    elif sales_yoy > 10 and sales_qoq > -25 and margin_yoy >= -20 and pbt_yoy > 10 and pat_yoy > 10: rating = "Good"
    elif sales_yoy > -5 and margin_yoy >= -20 and pbt_yoy > -5 and pat_yoy > -5: rating = "Flat"
    else: rating = "Bad"
        
    metrics.update({
        "Sales_YoY": sales_yoy, "Sales_QoQ": sales_qoq, "PAT_YoY": pat_yoy, "PAT_QoQ": pat_qoq,
        "Margin_YoY": margin_yoy, "Margin_QoQ": margin_qoq, "Rating": rating
    })
    return metrics

def format_telegram_alert(metrics_dict, company_name, nse_symbol, attachment_name):
    rating = metrics_dict.get('Rating', 'Unknown')
    summaries = {
        "Excellent": "Outstanding performance this quarter! 🚀", "Good": "A solid and steady quarter! 👍",
        "Flat": "Performance remained flat this quarter. ⚖️", "Bad": "Challenging quarter with declining metrics. ⚠️"
    }
    summary = summaries.get(rating, "Results announced for this quarter.")
    
    def fmt(val, is_pct=False): return f"{val:.1f}%" if is_pct else (f"{val:.1f}" if val is not None else "-")
    def fmt_trend(val):
        if val is None: return "-"
        return f"🟢 ▲ {val:.1f}%" if val >= 0 else f"🔴 ▼ {abs(val):.1f}%"

    margin_label = metrics_dict.get('Margin_Name', 'OPM')
    display_name = nse_symbol if nse_symbol else company_name
    pdf_link = f"https://www.bseindia.com/xml-data/corpfiling/AttachHis/{attachment_name}" if attachment_name else "#"

    return f"""🏢 <b><a href="{pdf_link}">{display_name}</a></b> | <b>{rating.upper()}</b> Results!

<b>Sales:</b> {fmt(metrics_dict.get('Sales_CQ'))}
<i>(YoY: {fmt_trend(metrics_dict.get('Sales_YoY'))} | QoQ: {fmt_trend(metrics_dict.get('Sales_QoQ'))})</i>

<b>Net Profit:</b> {fmt(metrics_dict.get('PAT_CQ'))}
<i>(YoY: {fmt_trend(metrics_dict.get('PAT_YoY'))} | QoQ: {fmt_trend(metrics_dict.get('PAT_QoQ'))})</i>

<b>{margin_label}:</b> {fmt(metrics_dict.get('Margin_CQ'), True)}
<i>(YoY: {fmt_trend(metrics_dict.get('Margin_YoY'))} | QoQ: {fmt_trend(metrics_dict.get('Margin_QoQ'))})</i>

{summary} #FinancialAlert"""

def send_to_telegram(message_text, scrip_code, nse_symbol):
    if not TELEGRAM_CHANNEL_ID: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    ticker = nse_symbol if nse_symbol else scrip_code
    screener_url = f"https://www.screener.in/company/{ticker}/"
    full_text = message_text + f"\n\n🔗 <a href='{screener_url}'>Check details on Screener</a>"
    
    payload = {"chat_id": TELEGRAM_CHANNEL_ID, "text": full_text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print(f"✅ Sent to Telegram Channel ({scrip_code})")
            return True
        else:
            print(f"❌ Telegram Error for {scrip_code}: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Telegram Connection Error for {scrip_code}: {e}")
        return False

# ==========================================
# 5. MAIN ORCHESTRATOR LOOP
# ==========================================
def process_pending_results():
    print("Processing pending results...")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Scrip_Code, Attachment_Name, Company_Name FROM earnings_tracker WHERE Target_Quarter = %s AND Telegram_Status = 'Unsent'", (TARGET_QUARTER,))
    pending_stocks = cursor.fetchall()
    
    for scrip, attachment, company_name in pending_stocks:
        data = scrape_screener(scrip)
        if check_screener_for_quarter(data, TARGET_QUARTER):
            
            # Soft Delete if Market Cap < 500 Cr
            if data.get('market_cap', 0) < 500:
                print(f"⏳ Filtering {scrip}: Market Cap ({data.get('market_cap')} Cr) is below 500 Cr.")
                cursor.execute('''
                    UPDATE earnings_tracker 
                    SET Screener_Status = 'Available', Telegram_Status = 'Filtered (<500Cr)', Last_Checked = %s
                    WHERE Scrip_Code = %s AND Target_Quarter = %s
                ''', (datetime.now(), scrip, TARGET_QUARTER))
                conn.commit()
                time.sleep(2)
                continue

            nse_symbol = data.get('nse_symbol')
            metrics = evaluate_financials(data)
            
            if metrics:
                msg = format_telegram_alert(metrics, company_name, nse_symbol, attachment)
                if send_to_telegram(msg, scrip, nse_symbol):
                    cursor.execute('''
                        UPDATE earnings_tracker 
                        SET Screener_Status = 'Available', Telegram_Status = 'Sent',
                            Rating = %s, Sales_YoY = %s, PAT_YoY = %s, Margin_Change = %s, Last_Checked = %s, NSE_Symbol = %s,
                            Sales_CQ = %s, Sales_PQ = %s, Sales_YQ = %s, PAT_CQ = %s, PAT_PQ = %s, PAT_YQ = %s,
                            Sales_QoQ = %s, PAT_QoQ = %s, Margin_QoQ = %s, Margin_CQ = %s, Margin_PQ = %s, Margin_YQ = %s, Margin_Name = %s
                        WHERE Scrip_Code = %s AND Target_Quarter = %s
                    ''', (metrics['Rating'], metrics['Sales_YoY'], metrics['PAT_YoY'], 
                          metrics['Margin_YoY'], datetime.now(), nse_symbol, 
                          metrics.get('Sales_CQ'), metrics.get('Sales_PQ'), metrics.get('Sales_YQ'), 
                          metrics.get('PAT_CQ'), metrics.get('PAT_PQ'), metrics.get('PAT_YQ'), 
                          metrics.get('Sales_QoQ'), metrics.get('PAT_QoQ'), metrics.get('Margin_QoQ'),
                          metrics.get('Margin_CQ'), metrics.get('Margin_PQ'), metrics.get('Margin_YQ'),
                          metrics.get('Margin_Name'), scrip, TARGET_QUARTER))
                    conn.commit()
        else:
            cursor.execute("UPDATE earnings_tracker SET Last_Checked = %s WHERE Scrip_Code = %s AND Target_Quarter = %s", (datetime.now(), scrip, TARGET_QUARTER))
            conn.commit()
        time.sleep(2) 
    conn.close()

# ==========================================
# 6. FLASK WEB UI DASHBOARD & WEBHOOKS
# ==========================================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Earnings Tracker Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
    <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
    <style>
        .tooltip { display: inline-block; cursor: help; position: relative; }
        .tooltip .tooltiptext {
            visibility: hidden; width: max-content; min-width: 400px;
            background-color: #1f2937; color: #fff; text-align: left; border-radius: 8px; padding: 16px;
            position: fixed; z-index: 9999; top: 50%; left: 50%; transform: translate(-50%, -50%);
            opacity: 0; transition: opacity 0.2s ease-in-out; font-size: 0.95rem; font-weight: normal;
            box-shadow: 0 25px 50px -12px rgba(0,0,0,0.5), 0 10px 15px -3px rgba(0,0,0,0.3); pointer-events: none;
        }
        .tooltip:hover .tooltiptext { visibility: visible; opacity: 1; }
    </style>
</head>
<body class="bg-gray-100 min-h-screen">
    <nav class="bg-blue-800 text-white p-4 shadow-md">
        <div class="max-w-7xl mx-auto flex justify-between items-center">
            <h1 class="text-2xl font-bold">📈 Earnings Tracker</h1>
        </div>
    </nav>
    <div class="max-w-7xl mx-auto mt-8 p-6 bg-white rounded-lg shadow-md mb-8">
        {% block content %}{% endblock %}
    </div>
    <script>
    $(document).ready(function() { 
        var table = $('#dataTable').DataTable({"pageLength": 25, "order": [[ 4, "desc" ]]}); 
        var sectorTable = $('#sectorFinTable').DataTable({"pageLength": 25});
        var mapTable = $('#mappingTable').DataTable({"pageLength": 25});
        
        // Tab Navigation
        $('.tab-btn').click(function() {
            $('.tab-btn').removeClass('border-blue-600 text-blue-600 active').addClass('border-transparent text-gray-500');
            $(this).removeClass('border-transparent text-gray-500').addClass('border-blue-600 text-blue-600 active');
            
            $('.tab-content').addClass('hidden');
            var target = $(this).data('target');
            $('#' + target).removeClass('hidden');
        });

        // Handle manual fetch
        $('#manualFetchBtn').click(function() {
            var scripCode = $('#manualScrip').val();
            if(!scripCode) {
                alert("Please enter a Scrip Code or NSE Symbol.");
                return;
            }
            
            var enteredKey = prompt('Authentication required. Enter secret key to manually fetch ' + scripCode + ':');
            if(enteredKey !== null && enteredKey.trim() !== "") {
                var btn = $(this);
                var originalText = btn.text();
                btn.text("Fetching...").prop("disabled", true).addClass("opacity-50 cursor-not-allowed");
                
                $.ajax({
                    url: '/api/manual-fetch',
                    type: 'POST',
                    data: { scrip: scripCode, key: enteredKey },
                    success: function(result) {
                        btn.text(originalText).prop("disabled", false).removeClass("opacity-50 cursor-not-allowed");
                        if(result.status === 'success' || result.status === 'info') {
                            alert(result.message);
                            if(result.status === 'success') {
                                location.reload(); // Reload page to see new row
                            }
                        } else {
                            alert('Error: ' + result.message);
                        }
                    },
                    error: function(xhr) {
                        btn.text(originalText).prop("disabled", false).removeClass("opacity-50 cursor-not-allowed");
                        alert('Error fetching scrip. Unauthorized or invalid key.');
                    }
                });
            }
        });

        // Handle Tracker Delete
        $('#dataTable tbody').on('click', '.delete-btn', function() {
            var scripCode = $(this).data('scrip');
            var row = $(this).closest('tr');
            
            var enteredKey = prompt('Authentication required. Enter secret key to delete ' + scripCode + ':');
            if(enteredKey !== null && enteredKey.trim() !== "") {
                $.ajax({
                    url: '/api/delete-scrip',
                    type: 'POST',
                    data: { scrip: scripCode, key: enteredKey },
                    success: function(result) {
                        if(result.status === 'success') {
                            table.row(row).remove().draw();
                        } else { alert('Error: ' + result.message); }
                    },
                    error: function() { alert('Error deleting scrip. Unauthorized or invalid key.'); }
                });
            }
        });

        // Add/Edit Sector Mapping
        $('#addMappingBtn').click(function() {
            var identifier = $('#mapIdentifier').val().trim();
            var sectorName = $('#mapSector').val().trim();
            
            if(!identifier || !sectorName) {
                alert("Both fields are required.");
                return;
            }
            
            var enteredKey = prompt('Authentication required. Enter secret key to update mapping:');
            if(enteredKey !== null && enteredKey.trim() !== "") {
                $.ajax({
                    url: '/api/save-sector',
                    type: 'POST',
                    data: { identifier: identifier, sector: sectorName, key: enteredKey },
                    success: function(result) {
                        if(result.status === 'success') {
                            alert('Mapping saved successfully!');
                            location.reload();
                        } else { alert('Error: ' + result.message); }
                    },
                    error: function() { alert('Error saving mapping. Unauthorized or invalid key.'); }
                });
            }
        });

        // Delete Sector Mapping
        $('#mappingTable tbody').on('click', '.delete-map-btn', function() {
            var identifier = $(this).data('id');
            var row = $(this).closest('tr');
            
            var enteredKey = prompt('Authentication required. Enter secret key to delete mapping for ' + identifier + ':');
            if(enteredKey !== null && enteredKey.trim() !== "") {
                $.ajax({
                    url: '/api/delete-sector',
                    type: 'POST',
                    data: { identifier: identifier, key: enteredKey },
                    success: function(result) {
                        if(result.status === 'success') {
                            mapTable.row(row).remove().draw();
                        } else { alert('Error: ' + result.message); }
                    },
                    error: function() { alert('Error deleting mapping. Unauthorized or invalid key.'); }
                });
            }
        });

    });
    </script>
</body>
</html>
"""

DASHBOARD_PAGE = """
{% extends "base" %}
{% block content %}

<!-- Tabs Navigation -->
<div class="border-b border-gray-200 mb-6">
    <ul class="flex flex-wrap -mb-px text-sm font-medium text-center text-gray-500">
        <li class="mr-2">
            <button data-target="tab-tracker" class="tab-btn inline-block p-4 border-b-2 border-blue-600 rounded-t-lg active text-blue-600 focus:outline-none" aria-current="page">Main Tracker</button>
        </li>
        <li class="mr-2">
            <button data-target="tab-sector" class="tab-btn inline-block p-4 border-b-2 border-transparent rounded-t-lg hover:text-gray-600 hover:border-gray-300 focus:outline-none">Sector View</button>
        </li>
        <li class="mr-2">
            <button data-target="tab-mapping" class="tab-btn inline-block p-4 border-b-2 border-transparent rounded-t-lg hover:text-gray-600 hover:border-gray-300 focus:outline-none">Sector Mapping</button>
        </li>
        <li class="mr-2">
            <button data-target="tab-methodology" class="tab-btn inline-block p-4 border-b-2 border-transparent rounded-t-lg hover:text-gray-600 hover:border-gray-300 focus:outline-none">Methodology</button>
        </li>
    </ul>
</div>

<!-- TAB 1: Main Tracker -->
<div id="tab-tracker" class="tab-content block">
    <div class="flex justify-between items-center mb-6">
        <h2 class="text-xl font-bold text-gray-800">Tracked Announcements ({{ target_quarter }})</h2>
        <div class="flex items-center space-x-2 bg-gray-50 p-2 rounded border shadow-sm">
            <input type="text" id="manualScrip" placeholder="NSE Symbol or BSE Code..." class="border p-2 rounded w-48 text-sm focus:outline-none focus:border-blue-500">
            <button id="manualFetchBtn" class="bg-green-600 text-white px-4 py-2 text-sm rounded hover:bg-green-700 transition font-semibold">🔍 Fetch</button>
        </div>
    </div>

    <div class="overflow-x-auto" style="min-height: 400px;">
        <table id="dataTable" class="display w-full text-sm text-left text-gray-500">
            <thead class="text-xs text-gray-700 uppercase bg-gray-50">
                <tr>
                    <th>Scrip / NSE</th>
                    <th>Company</th>
                    <th>Sector</th>
                    <th>Announcement Date</th>
                    <th>Rating</th>
                    <th>Telegram</th>
                    <th>Sales YoY</th>
                    <th>PAT YoY</th>
                    <th>Sales QoQ</th>
                    <th>PAT QoQ</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                {% for row in rows %}
                <tr class="border-b">
                    <td class="px-4 py-3 font-medium text-gray-900">
                        <div class="tooltip">
                            <span class="border-b border-dotted border-gray-500">{{ row[8] or row[0] }}</span>
                            <div class="tooltiptext">
                                <div class="mb-3 font-bold text-lg border-b border-gray-600 pb-2">{{ row[1] }} ({{ row[8] or row[0] }})</div>
                                <table class="w-full text-left text-gray-200 text-sm">
                                    <thead>
                                        <tr class="border-b border-gray-600">
                                            <th class="pb-1 pr-3">Metric</th><th class="pb-1 pr-3">CQ</th><th class="pb-1 pr-3">PQ</th>
                                            <th class="pb-1 pr-3">YQ</th><th class="pb-1 pr-3">QoQ%</th><th class="pb-1">YoY%</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td class="pr-3 py-1 font-semibold text-gray-400">Sales</td>
                                            <td class="pr-3">{% if row[9] is not none %}{{ row[9] }}{% else %}-{% endif %}</td>
                                            <td class="pr-3">{% if row[10] is not none %}{{ row[10] }}{% else %}-{% endif %}</td>
                                            <td class="pr-3">{% if row[11] is not none %}{{ row[11] }}{% else %}-{% endif %}</td>
                                            <td class="pr-3 {% if row[15] is not none and row[15]|float > 0 %}text-green-400{% elif row[15] is not none and row[15]|float < 0 %}text-red-400{% endif %}">
                                                {% if row[15] is not none %}{{ "%.2f"|format(row[15]|float) }}%{% else %}-{% endif %}</td>
                                            <td class="{% if row[6] is not none and row[6]|float > 0 %}text-green-400{% elif row[6] is not none and row[6]|float < 0 %}text-red-400{% endif %}">
                                                {% if row[6] is not none %}{{ "%.2f"|format(row[6]|float) }}%{% else %}-{% endif %}</td>
                                        </tr>
                                        <tr>
                                            <td class="pr-3 py-1 font-semibold text-gray-400">Net Profit</td>
                                            <td class="pr-3">{% if row[12] is not none %}{{ row[12] }}{% else %}-{% endif %}</td>
                                            <td class="pr-3">{% if row[13] is not none %}{{ row[13] }}{% else %}-{% endif %}</td>
                                            <td class="pr-3">{% if row[14] is not none %}{{ row[14] }}{% else %}-{% endif %}</td>
                                            <td class="pr-3 {% if row[16] is not none and row[16]|float > 0 %}text-green-400{% elif row[16] is not none and row[16]|float < 0 %}text-red-400{% endif %}">
                                                {% if row[16] is not none %}{{ "%.2f"|format(row[16]|float) }}%{% else %}-{% endif %}</td>
                                            <td class="{% if row[7] is not none and row[7]|float > 0 %}text-green-400{% elif row[7] is not none and row[7]|float < 0 %}text-red-400{% endif %}">
                                                {% if row[7] is not none %}{{ "%.2f"|format(row[7]|float) }}%{% else %}-{% endif %}</td>
                                        </tr>
                                        <tr>
                                            <td class="pr-3 py-1 font-semibold text-gray-400">{{ row[21] or 'Margin' }}</td>
                                            <td class="pr-3">{% if row[18] is not none %}{{ "%.1f"|format(row[18]|float) }}%{% else %}-{% endif %}</td>
                                            <td class="pr-3">{% if row[19] is not none %}{{ "%.1f"|format(row[19]|float) }}%{% else %}-{% endif %}</td>
                                            <td class="pr-3">{% if row[20] is not none %}{{ "%.1f"|format(row[20]|float) }}%{% else %}-{% endif %}</td>
                                            <td class="pr-3 {% if row[17] is not none and row[17]|float > 0 %}text-green-400{% elif row[17] is not none and row[17]|float < 0 %}text-red-400{% endif %}">
                                                {% if row[17] is not none %}{{ "%.2f"|format(row[17]|float) }}%{% else %}-{% endif %}</td>
                                            <td class="{% if row[22] is not none and row[22]|float > 0 %}text-green-400{% elif row[22] is not none and row[22]|float < 0 %}text-red-400{% endif %}">
                                                {% if row[22] is not none %}{{ "%.2f"|format(row[22]|float) }}%{% else %}-{% endif %}</td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </td>
                    <td class="px-4 py-3 font-medium text-gray-900"><a href="https://www.screener.in/company/{{ row[8] or row[0] }}/" target="_blank" class="text-blue-600 hover:underline">{{ row[1] }}</a></td>
                    <td class="px-4 py-3 whitespace-nowrap">{{ row[23] }}</td>
                    <td class="px-4 py-3">{{ row[2] }}</td>
                    <td class="px-4 py-3">
                        {% if row[3] == 'Excellent' %}<span class="px-2 py-1 bg-green-100 text-green-800 rounded">Excellent</span>
                        {% elif row[3] == 'Good' %}<span class="px-2 py-1 bg-blue-100 text-blue-800 rounded">Good</span>
                        {% elif row[3] == 'Flat' %}<span class="px-2 py-1 bg-gray-100 text-gray-800 rounded">Flat</span>
                        {% elif row[3] == 'Bad' %}<span class="px-2 py-1 bg-red-100 text-red-800 rounded">Bad</span>
                        {% else %}-{% endif %}
                    </td>
                    <td class="px-4 py-3">{{ row[5] }}</td>
                    
                    <td class="px-4 py-3 whitespace-nowrap" data-order="{{ row[6] if row[6] is not none else -999999 }}">
                        {% if row[6] is not none %}
                            {% if row[6]|float > 0 %}<span class="text-green-600 font-medium">▲ {{ "%.2f"|format(row[6]|float) }}%</span>
                            {% elif row[6]|float < 0 %}<span class="text-red-600 font-medium">▼ {{ "%.2f"|format(row[6]|float|abs) }}%</span>
                            {% else %}<span class="text-gray-600">{{ "%.2f"|format(row[6]|float) }}%</span>{% endif %}
                        {% else %}-{% endif %}
                    </td>
                    <td class="px-4 py-3 whitespace-nowrap" data-order="{{ row[7] if row[7] is not none else -999999 }}">
                        {% if row[7] is not none %}
                            {% if row[7]|float > 0 %}<span class="text-green-600 font-medium">▲ {{ "%.2f"|format(row[7]|float) }}%</span>
                            {% elif row[7]|float < 0 %}<span class="text-red-600 font-medium">▼ {{ "%.2f"|format(row[7]|float|abs) }}%</span>
                            {% else %}<span class="text-gray-600">{{ "%.2f"|format(row[7]|float) }}%</span>{% endif %}
                        {% else %}-{% endif %}
                    </td>
                    <td class="px-4 py-3 whitespace-nowrap" data-order="{{ row[15] if row[15] is not none else -999999 }}">
                        {% if row[15] is not none %}
                            {% if row[15]|float > 0 %}<span class="text-green-600 font-medium">▲ {{ "%.2f"|format(row[15]|float) }}%</span>
                            {% elif row[15]|float < 0 %}<span class="text-red-600 font-medium">▼ {{ "%.2f"|format(row[15]|float|abs) }}%</span>
                            {% else %}<span class="text-gray-600">{{ "%.2f"|format(row[15]|float) }}%</span>{% endif %}
                        {% else %}-{% endif %}
                    </td>
                    <td class="px-4 py-3 whitespace-nowrap" data-order="{{ row[16] if row[16] is not none else -999999 }}">
                        {% if row[16] is not none %}
                            {% if row[16]|float > 0 %}<span class="text-green-600 font-medium">▲ {{ "%.2f"|format(row[16]|float) }}%</span>
                            {% elif row[16]|float < 0 %}<span class="text-red-600 font-medium">▼ {{ "%.2f"|format(row[16]|float|abs) }}%</span>
                            {% else %}<span class="text-gray-600">{{ "%.2f"|format(row[16]|float) }}%</span>{% endif %}
                        {% else %}-{% endif %}
                    </td>
                    <td class="px-4 py-3 text-center">
                        <button class="delete-btn text-red-500 hover:text-red-700 font-bold" data-scrip="{{ row[0] }}" title="Delete from tracker">
                            🗑️
                        </button>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</div>

<!-- TAB 2: Sector View -->
<div id="tab-sector" class="tab-content hidden">
    <div id="sectorFinContainer">
        <h2 class="text-xl font-bold text-gray-800 mb-4">Financial Aggregates & Ratings by Sector</h2>
        <div class="overflow-x-auto">
            <table id="sectorFinTable" class="display w-full text-sm text-left text-gray-500">
                <thead class="text-xs text-gray-700 uppercase bg-gray-50">
                    <tr>
                        <th>Sector</th>
                        <th>Sector Rating</th>
                        <th>Sales YoY</th>
                        <th>PAT YoY</th>
                        <th>Sales QoQ</th>
                        <th>PAT QoQ</th>
                        <th>Excellent</th>
                        <th>Good</th>
                        <th>Flat</th>
                        <th>Bad</th>
                    </tr>
                </thead>
                <tbody>
                    {% for item in sector_data %}
                    <tr class="border-b">
                        <td class="px-4 py-3 font-medium text-gray-900">{{ item['Sector'] }}</td>
                        <td class="px-4 py-3" data-order="{{ item['Sector_Rating'] }}">
                            {% if item['Sector_Rating'] == 'Excellent' %}<span class="px-2 py-1 bg-green-100 text-green-800 rounded text-xs font-semibold">Excellent</span>
                            {% elif item['Sector_Rating'] == 'Good' %}<span class="px-2 py-1 bg-blue-100 text-blue-800 rounded text-xs font-semibold">Good</span>
                            {% elif item['Sector_Rating'] == 'Flat' %}<span class="px-2 py-1 bg-gray-100 text-gray-800 rounded text-xs font-semibold">Flat</span>
                            {% elif item['Sector_Rating'] == 'Bad' %}<span class="px-2 py-1 bg-red-100 text-red-800 rounded text-xs font-semibold">Bad</span>
                            {% else %}-{% endif %}
                        </td>
                        <td class="px-4 py-3 font-medium whitespace-nowrap {% if item['Sales_YoY'] > 0 %}text-green-600{% elif item['Sales_YoY'] < 0 %}text-red-600{% else %}text-gray-600{% endif %}" data-order="{{ item['Sales_YoY'] }}">
                            {% if item['Sales_YoY'] > 0 %}▲ {% elif item['Sales_YoY'] < 0 %}▼ {% endif %}{{ "{:,.2f}".format(item['Sales_YoY']|abs) }}%
                        </td>
                        <td class="px-4 py-3 font-medium whitespace-nowrap {% if item['PAT_YoY'] > 0 %}text-green-600{% elif item['PAT_YoY'] < 0 %}text-red-600{% else %}text-gray-600{% endif %}" data-order="{{ item['PAT_YoY'] }}">
                            {% if item['PAT_YoY'] > 0 %}▲ {% elif item['PAT_YoY'] < 0 %}▼ {% endif %}{{ "{:,.2f}".format(item['PAT_YoY']|abs) }}%
                        </td>
                        <td class="px-4 py-3 font-medium whitespace-nowrap {% if item['Sales_QoQ'] > 0 %}text-green-600{% elif item['Sales_QoQ'] < 0 %}text-red-600{% else %}text-gray-600{% endif %}" data-order="{{ item['Sales_QoQ'] }}">
                            {% if item['Sales_QoQ'] > 0 %}▲ {% elif item['Sales_QoQ'] < 0 %}▼ {% endif %}{{ "{:,.2f}".format(item['Sales_QoQ']|abs) }}%
                        </td>
                        <td class="px-4 py-3 font-medium whitespace-nowrap {% if item['PAT_QoQ'] > 0 %}text-green-600{% elif item['PAT_QoQ'] < 0 %}text-red-600{% else %}text-gray-600{% endif %}" data-order="{{ item['PAT_QoQ'] }}">
                            {% if item['PAT_QoQ'] > 0 %}▲ {% elif item['PAT_QoQ'] < 0 %}▼ {% endif %}{{ "{:,.2f}".format(item['PAT_QoQ']|abs) }}%
                        </td>
                        <td class="px-4 py-3 font-bold text-green-600 whitespace-nowrap" data-order="{{ item.get('Excellent', 0) }}">{{ item['Exc_Str'] }}</td>
                        <td class="px-4 py-3 font-bold text-blue-600 whitespace-nowrap" data-order="{{ item.get('Good', 0) }}">{{ item['Good_Str'] }}</td>
                        <td class="px-4 py-3 font-bold text-gray-600 whitespace-nowrap" data-order="{{ item.get('Flat', 0) }}">{{ item['Flat_Str'] }}</td>
                        <td class="px-4 py-3 font-bold text-red-600 whitespace-nowrap" data-order="{{ item.get('Bad', 0) }}">{{ item['Bad_Str'] }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
</div>

<!-- TAB 3: Sector Mapping -->
<div id="tab-mapping" class="tab-content hidden">
    <div class="flex justify-between items-center mb-6">
        <h2 class="text-xl font-bold text-gray-800">Company to Sector Mappings</h2>
        <div class="flex items-center space-x-2 bg-gray-50 p-2 rounded border shadow-sm">
            <input type="text" id="mapIdentifier" placeholder="Symbol/Scrip (e.g., RELIANCE)" class="border p-2 rounded w-48 text-sm focus:outline-none focus:border-blue-500">
            <input type="text" id="mapSector" placeholder="Sector Name (e.g., IT)" class="border p-2 rounded w-48 text-sm focus:outline-none focus:border-blue-500">
            <button id="addMappingBtn" class="bg-blue-600 text-white px-4 py-2 text-sm rounded hover:bg-blue-700 transition font-semibold">💾 Save</button>
        </div>
    </div>

    <div class="overflow-x-auto bg-white p-4 rounded-lg shadow border border-gray-100">
        <table id="mappingTable" class="display w-full text-sm text-left text-gray-500">
            <thead class="text-xs text-gray-700 uppercase bg-gray-50">
                <tr>
                    <th>Identifier (Scrip/Symbol)</th>
                    <th>Mapped Sector</th>
                    <th class="text-center">Action</th>
                </tr>
            </thead>
            <tbody>
                {% for map in mappings %}
                <tr class="border-b hover:bg-gray-50 transition">
                    <td class="px-4 py-3 font-medium text-gray-900">{{ map[0] }}</td>
                    <td class="px-4 py-3">{{ map[1] }}</td>
                    <td class="px-4 py-3 text-center">
                        <button class="delete-map-btn text-red-500 hover:text-red-700 font-bold" data-id="{{ map[0] }}" title="Delete Mapping">
                            🗑️
                        </button>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</div>

<!-- TAB 4: Rating Methodology -->
<div id="tab-methodology" class="tab-content hidden">
    <!-- Company Logic -->
    <div class="bg-white rounded-lg p-6 mb-6 shadow-sm border border-gray-100">
        <h3 class="text-lg font-bold text-gray-800 mb-4 border-b pb-2">🏢 Individual Company Rating Logic</h3>
        <p class="text-sm text-gray-600 mb-6">Results are systematically analyzed and categorized into <strong>Excellent</strong>, <strong>Good</strong>, <strong>Flat</strong>, or <strong>Bad</strong> based on the following criteria evaluation.</p>
        
        <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
            <div>
                <h4 class="font-semibold text-gray-700 mb-3">The 9-Point Checklist</h4>
                <ul class="list-none text-sm text-gray-600 space-y-2">
                    <li><span class="text-blue-500 font-bold mr-2">1.</span>Sales Growth YoY > 20%</li>
                    <li><span class="text-blue-500 font-bold mr-2">2.</span>Sales Growth QoQ > -10%</li>
                    <li><span class="text-blue-500 font-bold mr-2">3.</span>Margin Change YoY >= 0%</li>
                    <li><span class="text-blue-500 font-bold mr-2">4.</span>Margin Change QoQ > -20%</li>
                    <li><span class="text-blue-500 font-bold mr-2">5.</span>Positive Tax (Tax % >= 0)</li>
                    <li><span class="text-blue-500 font-bold mr-2">6.</span>Positive Profit Before Tax (PBT > 0)</li>
                    <li><span class="text-blue-500 font-bold mr-2">7.</span>Operating Leverage (PBT YoY Growth > Sales YoY Growth)</li>
                    <li><span class="text-blue-500 font-bold mr-2">8.</span>Positive Net Profit (PAT > 0)</li>
                    <li><span class="text-blue-500 font-bold mr-2">9.</span>PAT Growth YoY > 20%</li>
                </ul>
            </div>
            <div>
                <h4 class="font-semibold text-gray-700 mb-3">Rating Assignment</h4>
                <ul class="text-sm text-gray-600 space-y-4">
                    <li class="flex items-start">
                        <span class="px-2 py-1 bg-green-100 text-green-800 rounded font-medium text-xs mr-3 mt-0.5 whitespace-nowrap">Excellent</span> 
                        <span>Achieves <strong>8 or more points</strong> on the 9-point checklist.</span>
                    </li>
                    <li class="flex items-start">
                        <span class="px-2 py-1 bg-blue-100 text-blue-800 rounded font-medium text-xs mr-3 mt-0.5 whitespace-nowrap">Good</span> 
                        <span>Sales YoY > 10%, Sales QoQ > -25%, Margin YoY >= -20%, PBT YoY > 10%, and PAT YoY > 10%.</span>
                    </li>
                    <li class="flex items-start">
                        <span class="px-2 py-1 bg-gray-100 text-gray-800 rounded font-medium text-xs mr-3 mt-0.5 whitespace-nowrap">Flat</span> 
                        <span>Sales YoY > -5%, Margin YoY >= -20%, PBT YoY > -5%, and PAT YoY > -5%.</span>
                    </li>
                    <li class="flex items-start">
                        <span class="px-2 py-1 bg-red-100 text-red-800 rounded font-medium text-xs mr-3 mt-0.5 whitespace-nowrap">Bad</span> 
                        <span>Fails to meet any of the above minimum thresholds.</span>
                    </li>
                </ul>
            </div>
        </div>
    </div>

    <!-- Sector Logic -->
    <div class="bg-blue-50 rounded-lg p-6 shadow-sm border border-blue-100">
        <h3 class="text-lg font-bold text-blue-900 mb-4 border-b border-blue-200 pb-2">🌐 Lenient & Proportionate Sector Rating Logic</h3>
        <p class="text-sm text-gray-700 mb-6">Sector ratings balance aggregated financial sums with the proportion of underlying companies performing well, ensuring a single bad company does not drag down the entire sector.</p>
        
        <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
            <div>
                <h4 class="font-semibold text-gray-800 mb-3">The Sector Checklist (Max 7 pts)</h4>
                <div class="mb-4">
                    <p class="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2">Financial Points</p>
                    <ul class="list-none text-sm text-gray-700 space-y-1">
                        <li><span class="text-blue-600 font-bold mr-2">+1 pt:</span>Aggregated Sales Growth YoY >= 10%</li>
                        <li><span class="text-blue-600 font-bold mr-2">+1 pt:</span>Aggregated Sales Growth QoQ >= -10%</li>
                        <li><span class="text-blue-600 font-bold mr-2">+1 pt:</span>Average Sector Margin YoY >= -2.0%</li>
                        <li><span class="text-blue-600 font-bold mr-2">+1 pt:</span>Aggregated PAT Growth YoY >= 10%</li>
                    </ul>
                </div>
                <div>
                    <p class="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2">Proportion Points</p>
                    <ul class="list-none text-sm text-gray-700 space-y-1">
                        <li><span class="text-blue-600 font-bold mr-2">+1 pt:</span>>= 50% of companies are Good or Excellent</li>
                        <li><span class="text-blue-600 font-bold mr-2">+1 pt:</span>>= 25% of companies are Good or Excellent</li>
                        <li><span class="text-blue-600 font-bold mr-2">+1 pt:</span><= 25% of companies are Bad</li>
                    </ul>
                </div>
                <div class="mt-4 border-t border-blue-200 pt-2">
                    <span class="text-green-600 font-bold mr-2">★</span><strong>Mandatory Check:</strong> Sector Total PAT CQ > 0
                </div>
            </div>
            <div>
                <h4 class="font-semibold text-gray-800 mb-3">Sector Rating Assignment</h4>
                <ul class="text-sm text-gray-700 space-y-4">
                    <li class="flex items-start">
                        <span class="px-2 py-1 bg-green-200 text-green-900 rounded font-medium text-xs mr-3 mt-0.5 whitespace-nowrap">Excellent</span> 
                        <span>Achieves <strong>5 or more points</strong> and is overall profitable.</span>
                    </li>
                    <li class="flex items-start">
                        <span class="px-2 py-1 bg-blue-200 text-blue-900 rounded font-medium text-xs mr-3 mt-0.5 whitespace-nowrap">Good</span> 
                        <span>Achieves <strong>3 or 4 points</strong> and is overall profitable.</span>
                    </li>
                    <li class="flex items-start">
                        <span class="px-2 py-1 bg-gray-200 text-gray-800 rounded font-medium text-xs mr-3 mt-0.5 whitespace-nowrap">Flat</span> 
                        <span>Achieves <strong>1 or 2 points</strong>.</span>
                    </li>
                    <li class="flex items-start">
                        <span class="px-2 py-1 bg-red-200 text-red-900 rounded font-medium text-xs mr-3 mt-0.5 whitespace-nowrap">Bad</span> 
                        <span>Fails to meet any points or the sector is operating at an overall net loss.</span>
                    </li>
                </ul>
            </div>
        </div>
    </div>
</div>
{% endblock %}
"""

app.jinja_env.loader = ChoiceLoader([DictLoader({'base': HTML_TEMPLATE}), app.jinja_env.loader])

@app.route("/")
def dashboard():
    init_db()
    conn = get_db_connection()
    cursor = conn.cursor()
    # Execute query joining the new 'sectors' mapping table
    cursor.execute("""
        SELECT e.Scrip_Code, e.Company_Name, e.BSE_Announcement_Date, e.Rating, e.Screener_Status, e.Telegram_Status, 
               e.Sales_YoY, e.PAT_YoY, e.NSE_Symbol, e.Sales_CQ, e.Sales_PQ, e.Sales_YQ, e.PAT_CQ, e.PAT_PQ, e.PAT_YQ,
               e.Sales_QoQ, e.PAT_QoQ, e.Margin_QoQ, e.Margin_CQ, e.Margin_PQ, e.Margin_YQ, e.Margin_Name, e.Margin_Change,
               COALESCE(s1.Sector_Name, s2.Sector_Name, 'Unknown') as Sector
        FROM earnings_tracker e
        LEFT JOIN sectors s1 ON e.Scrip_Code = s1.Identifier
        LEFT JOIN sectors s2 ON e.NSE_Symbol = s2.Identifier
        WHERE e.Target_Quarter = %s AND e.Telegram_Status NOT IN ('Deleted', 'Filtered (<500Cr)')
    """, (TARGET_QUARTER,))
    rows = cursor.fetchall()
    
    # Fetch all sector mappings for the new tab
    cursor.execute("SELECT Identifier, Sector_Name FROM sectors ORDER BY Sector_Name")
    mappings = cursor.fetchall()
    conn.close()
    
    # Process Pivot Views using Pandas
    df = pd.DataFrame(rows, columns=[
        'Scrip_Code', 'Company_Name', 'BSE_Announcement_Date', 'Rating', 'Screener_Status', 'Telegram_Status', 
        'Sales_YoY', 'PAT_YoY', 'NSE_Symbol', 'Sales_CQ', 'Sales_PQ', 'Sales_YQ', 'PAT_CQ', 'PAT_PQ', 'PAT_YQ',
        'Sales_QoQ', 'PAT_QoQ', 'Margin_QoQ', 'Margin_CQ', 'Margin_PQ', 'Margin_YQ', 'Margin_Name', 'Margin_Change', 'Sector'
    ])
    
    if not df.empty:
        # Pivot 1: Ratings Count by Sector
        rating_pivot = pd.crosstab(df['Sector'], df['Rating']).reset_index()
        for cat in ['Excellent', 'Good', 'Flat', 'Bad']:
            if cat not in rating_pivot.columns:
                rating_pivot[cat] = 0
                
        # Prep for Pivot 2: Convert metrics to numeric
        for col in ['Sales_CQ', 'Sales_PQ', 'Sales_YQ', 'PAT_CQ', 'PAT_PQ', 'PAT_YQ', 'Margin_CQ', 'Margin_PQ', 'Margin_YQ']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        # Group by Sector: Sum Sales & PAT, Average the Margins
        fin_agg = df.groupby('Sector').agg({
            'Sales_CQ': 'sum', 'Sales_PQ': 'sum', 'Sales_YQ': 'sum',
            'PAT_CQ': 'sum', 'PAT_PQ': 'sum', 'PAT_YQ': 'sum',
            'Margin_CQ': 'mean', 'Margin_PQ': 'mean', 'Margin_YQ': 'mean'
        }).reset_index()
        
        def safe_pct(curr, past):
            if past == 0: return 0.0
            return ((curr - past) / abs(past)) * 100
            
        fin_agg['Sales_QoQ'] = fin_agg.apply(lambda x: safe_pct(x['Sales_CQ'], x['Sales_PQ']), axis=1)
        fin_agg['Sales_YoY'] = fin_agg.apply(lambda x: safe_pct(x['Sales_CQ'], x['Sales_YQ']), axis=1)
        fin_agg['PAT_QoQ'] = fin_agg.apply(lambda x: safe_pct(x['PAT_CQ'], x['PAT_PQ']), axis=1)
        fin_agg['PAT_YoY'] = fin_agg.apply(lambda x: safe_pct(x['PAT_CQ'], x['PAT_YQ']), axis=1)
        
        fin_agg['Margin_QoQ'] = fin_agg['Margin_CQ'] - fin_agg['Margin_PQ']
        fin_agg['Margin_YoY'] = fin_agg['Margin_CQ'] - fin_agg['Margin_YQ']
        
        # Merge financial aggregates with Rating Counts
        sector_data = pd.merge(fin_agg, rating_pivot, on='Sector', how='left')

        # Advanced Lenient Sector Rating Calculation & String Formatting
        def calculate_sector_metrics(r):
            exc = int(r.get('Excellent', 0))
            good = int(r.get('Good', 0))
            flat = int(r.get('Flat', 0))
            bad = int(r.get('Bad', 0))
            total = exc + good + flat + bad
            
            # Formatted strings with percentages
            r['Exc_Str'] = f"{exc} ({exc/total*100:.0f}%)" if total > 0 else "0 (0%)"
            r['Good_Str'] = f"{good} ({good/total*100:.0f}%)" if total > 0 else "0 (0%)"
            r['Flat_Str'] = f"{flat} ({flat/total*100:.0f}%)" if total > 0 else "0 (0%)"
            r['Bad_Str'] = f"{bad} ({bad/total*100:.0f}%)" if total > 0 else "0 (0%)"
            
            # Calculate Rating
            count = 0
            # Financial Points (Lenient)
            if r['Sales_YoY'] >= 10: count += 1
            if r['Sales_QoQ'] >= -10.0: count += 1
            if r['Margin_YoY'] >= -2.0: count += 1
            if r['PAT_YoY'] >= 10: count += 1
            
            # Proportion Points
            good_exc_pct = (exc + good) / total * 100 if total > 0 else 0
            bad_pct = bad / total * 100 if total > 0 else 0
            
            if good_exc_pct >= 50: count += 1
            if good_exc_pct >= 25: count += 1
            if bad_pct <= 25: count += 1
            
            is_profitable = r['PAT_CQ'] > 0
            
            if count >= 5 and is_profitable: r['Sector_Rating'] = "Excellent"
            elif count >= 3 and is_profitable: r['Sector_Rating'] = "Good"
            elif count >= 1: r['Sector_Rating'] = "Flat"
            else: r['Sector_Rating'] = "Bad"
            
            return r

        sector_data = sector_data.apply(calculate_sector_metrics, axis=1)
        sector_data_records = sector_data.to_dict('records')
    else:
        sector_data_records = []

    return render_template_string(
        DASHBOARD_PAGE, 
        rows=rows, 
        target_quarter=TARGET_QUARTER,
        sector_data=sector_data_records,
        mappings=mappings
    )

@app.route("/api/trigger-cron", methods=["GET"])
def trigger_cron():
    provided_key = request.args.get("key")
    if provided_key != CRON_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    
    start_date = request.args.get("start")
    end_date = request.args.get("end")
    
    if not start_date or not end_date:
        today = datetime.today()
        yesterday = today - timedelta(days=1)
        start_date = yesterday.strftime("%Y%m%d")
        end_date = today.strftime("%Y%m%d")
    
    def run_pipeline():
        try:
            fetch_new_bse_announcements(start_date, end_date)
            process_pending_results()
        except Exception as e:
            print(f"Pipeline error: {e}")
            
    thread = threading.Thread(target=run_pipeline)
    thread.start()
    
    msg = f"Pipeline started. Fetching from {start_date} to {end_date}."
    return jsonify({"status": "success", "message": msg})

@app.route("/api/delete-scrip", methods=["GET", "POST", "DELETE"])
def api_delete_scrip():
    provided_key = request.args.get("key") or request.form.get("key")
    if provided_key != CRON_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 401
        
    scrip_code = request.args.get("scrip") or request.form.get("scrip")
    if not scrip_code:
         return jsonify({"status": "error", "message": "Scrip code not provided."}), 400
         
    try:
        deleted = delete_scrip_from_db(scrip_code)
        if deleted > 0:
             return jsonify({"status": "success", "message": f"Successfully deleted {scrip_code}."})
        else:
             return jsonify({"status": "error", "message": f"Scrip {scrip_code} not found in DB."}), 404
    except Exception as e:
         return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/manual-fetch", methods=["POST"])
def manual_fetch():
    provided_key = request.form.get("key")
    if provided_key != CRON_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 401
        
    scrip_code = request.form.get("scrip")
    if not scrip_code:
         return jsonify({"status": "error", "message": "Scrip code not provided."}), 400
         
    scrip_code = scrip_code.upper().strip()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Check if already published
    cursor.execute("SELECT Telegram_Status FROM earnings_tracker WHERE (Scrip_Code = %s OR NSE_Symbol = %s) AND Target_Quarter = %s", (scrip_code, scrip_code, TARGET_QUARTER))
    existing = cursor.fetchone()
    
    if existing and existing[0] == 'Sent':
        conn.close()
        return jsonify({"status": "info", "message": f"{scrip_code} results are already published in the tracker!"})

    # 2. Scrape Screener directly
    data = scrape_screener(scrip_code)
    if not data:
        conn.close()
        return jsonify({"status": "error", "message": f"Could not find valid company data for '{scrip_code}' on Screener.in."})
        
    if not check_screener_for_quarter(data, TARGET_QUARTER):
        conn.close()
        return jsonify({"status": "error", "message": f"Results for {TARGET_QUARTER} are not yet available on Screener for {scrip_code}. Try again later."})
        
    company_name = data.get('company_name', 'Unknown')
    nse_symbol = data.get('nse_symbol')

    # 3. Market Cap Filter
    if data.get('market_cap', 0) < 500:
        cursor.execute('''
            INSERT INTO earnings_tracker (Scrip_Code, Company_Name, Target_Quarter, BSE_Announcement_Date, Headline, Attachment_Name, Screener_Status, Telegram_Status, Last_Checked)
            VALUES (%s, %s, %s, %s, %s, %s, 'Available', 'Filtered (<500Cr)', %s)
            ON CONFLICT (Scrip_Code, Target_Quarter) DO UPDATE SET Screener_Status='Available', Telegram_Status='Filtered (<500Cr)', Last_Checked=%s
        ''', (scrip_code, company_name, TARGET_QUARTER, 'Manual', 'Manual Override', '', datetime.now(), datetime.now()))
        conn.commit()
        conn.close()
        return jsonify({"status": "info", "message": f"Results bypassed: {company_name} Market Cap is < 500 Cr."})

    # 4. Evaluate & Send
    metrics = evaluate_financials(data)
    if not metrics:
        conn.close()
        return jsonify({"status": "error", "message": "Failed to parse financial tables from Screener."})

    msg = format_telegram_alert(metrics, company_name, nse_symbol, "") # Empty attachment/PDF for manual fetch
    
    if send_to_telegram(msg, scrip_code, nse_symbol):
        # 5. Save to DB
        cursor.execute('''
            INSERT INTO earnings_tracker 
            (Scrip_Code, Company_Name, Target_Quarter, BSE_Announcement_Date, Headline, Attachment_Name, Screener_Status, Telegram_Status, 
             Rating, Sales_YoY, PAT_YoY, Margin_Change, Last_Checked, NSE_Symbol,
             Sales_CQ, Sales_PQ, Sales_YQ, PAT_CQ, PAT_PQ, PAT_YQ,
             Sales_QoQ, PAT_QoQ, Margin_QoQ, Margin_CQ, Margin_PQ, Margin_YQ, Margin_Name)
            VALUES (%s, %s, %s, %s, %s, %s, 'Available', 'Sent', 
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Scrip_Code, Target_Quarter) DO UPDATE SET 
                Screener_Status = 'Available', Telegram_Status = 'Sent',
                Rating = EXCLUDED.Rating, Sales_YoY = EXCLUDED.Sales_YoY, PAT_YoY = EXCLUDED.PAT_YoY, Margin_Change = EXCLUDED.Margin_Change,
                Last_Checked = EXCLUDED.Last_Checked, NSE_Symbol = EXCLUDED.NSE_Symbol,
                Sales_CQ = EXCLUDED.Sales_CQ, Sales_PQ = EXCLUDED.Sales_PQ, Sales_YQ = EXCLUDED.Sales_YQ,
                PAT_CQ = EXCLUDED.PAT_CQ, PAT_PQ = EXCLUDED.PAT_PQ, PAT_YQ = EXCLUDED.PAT_YQ,
                Sales_QoQ = EXCLUDED.Sales_QoQ, PAT_QoQ = EXCLUDED.PAT_QoQ, Margin_QoQ = EXCLUDED.Margin_QoQ,
                Margin_CQ = EXCLUDED.Margin_CQ, Margin_PQ = EXCLUDED.Margin_PQ, Margin_YQ = EXCLUDED.Margin_YQ, Margin_Name = EXCLUDED.Margin_Name
        ''', (scrip_code, company_name, TARGET_QUARTER, 'Manual', 'Manual Override Fetch', '', 
              metrics['Rating'], metrics['Sales_YoY'], metrics['PAT_YoY'], metrics['Margin_YoY'], datetime.now(), nse_symbol,
              metrics.get('Sales_CQ'), metrics.get('Sales_PQ'), metrics.get('Sales_YQ'), 
              metrics.get('PAT_CQ'), metrics.get('PAT_PQ'), metrics.get('PAT_YQ'), 
              metrics.get('Sales_QoQ'), metrics.get('PAT_QoQ'), metrics.get('Margin_QoQ'),
              metrics.get('Margin_CQ'), metrics.get('Margin_PQ'), metrics.get('Margin_YQ'), metrics.get('Margin_Name')))
        conn.commit()
        conn.close()
        return jsonify({"status": "success", "message": f"Success! Fetched {company_name} and sent alert."})
    else:
        conn.close()
        return jsonify({"status": "error", "message": "Failed to send the Telegram alert."})

@app.route("/api/save-sector", methods=["POST"])
def api_save_sector():
    provided_key = request.form.get("key")
    if provided_key != CRON_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 401
        
    identifier = request.form.get("identifier")
    sector_name = request.form.get("sector")
    
    if not identifier or not sector_name:
         return jsonify({"status": "error", "message": "Missing Identifier or Sector Name."}), 400
         
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO sectors (Identifier, Sector_Name) VALUES (%s, %s)
            ON CONFLICT (Identifier) DO UPDATE SET Sector_Name = EXCLUDED.Sector_Name
        ''', (identifier.upper().strip(), sector_name.strip()))
        conn.commit()
        conn.close()
        return jsonify({"status": "success", "message": f"Mapping saved for {identifier}."})
    except Exception as e:
         return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/delete-sector", methods=["POST"])
def api_delete_sector():
    provided_key = request.form.get("key")
    if provided_key != CRON_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 401
        
    identifier = request.form.get("identifier")
    if not identifier:
         return jsonify({"status": "error", "message": "Missing Identifier."}), 400
         
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM sectors WHERE Identifier = %s', (identifier.upper().strip(),))
        conn.commit()
        conn.close()
        return jsonify({"status": "success", "message": f"Mapping deleted for {identifier}."})
    except Exception as e:
         return jsonify({"status": "error", "message": str(e)}), 500

# ==========================================
# RUN LOGIC
# ==========================================
if __name__ == "__main__":
    init_db()
    if len(sys.argv) == 3 and sys.argv[1] == '--delete':
        delete_scrip_from_db(sys.argv[2])
        sys.exit(0)
    elif len(sys.argv) == 3 and sys.argv[1] == '--upload-sectors':
        csv_file = sys.argv[2]
        print(f"Uploading sectors from {csv_file}...")
        try:
            df = pd.read_csv(csv_file, header=None)
            conn = get_db_connection()
            cursor = conn.cursor()
            count = 0
            for _, row in df.iterrows():
                ident = str(row[0]).upper().strip()
                sec = str(row[1]).strip()
                cursor.execute("INSERT INTO sectors (Identifier, Sector_Name) VALUES (%s, %s) ON CONFLICT (Identifier) DO UPDATE SET Sector_Name = EXCLUDED.Sector_Name", (ident, sec))
                count += 1
            conn.commit()
            conn.close()
            print(f"✅ Successfully inserted/updated {count} sector mappings!")
        except Exception as e:
            print(f"❌ Failed to upload sectors: {e}")
        sys.exit(0)
    elif len(sys.argv) == 4 and sys.argv[1] == '--test-dates':
        fetch_new_bse_announcements(sys.argv[2], sys.argv[3])
        process_pending_results()
    elif len(sys.argv) > 1 and sys.argv[1] == '--cron':
        fetch_new_bse_announcements()
        process_pending_results()
    else:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
