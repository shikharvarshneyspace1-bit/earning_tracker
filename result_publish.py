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
    month_map = {"Mar": ["mar", "03"], "Jun": ["jun", "06"], "Sep": ["sep", "09"], "Dec": ["dec", "12"]}
    month_aliases = month_map.get(month, [month.lower()])
    has_month = any(m in hl_lower for m in month_aliases)
    has_year = year in hl_lower
    if "result" in hl_lower and not (has_month and has_year): return False
    return True

# ==========================================
# DATABASE SETUP & UTILS
# ==========================================
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create main table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS earnings_tracker (
            Scrip_Code TEXT, Company_Name TEXT, Target_Quarter TEXT, BSE_Announcement_Date TEXT, Headline TEXT,
            Attachment_Name TEXT, NSE_Symbol TEXT, Screener_Status TEXT, Telegram_Status TEXT, Rating TEXT,
            Sales_YoY REAL, PAT_YoY REAL, Margin_Change REAL, Sales_CQ REAL, Sales_PQ REAL, Sales_YQ REAL,
            PAT_CQ REAL, PAT_PQ REAL, PAT_YQ REAL, Last_Checked TIMESTAMP, PRIMARY KEY (Scrip_Code, Target_Quarter)
        )
    ''')
    
    # Safe migrations for columns (PostgreSQL syntax)
    cols = ['Headline', 'Attachment_Name', 'NSE_Symbol', 'Sales_CQ', 'Sales_PQ', 'Sales_YQ', 
            'PAT_CQ', 'PAT_PQ', 'PAT_YQ', 'Sales_QoQ', 'PAT_QoQ', 'Margin_QoQ', 
            'Margin_CQ', 'Margin_PQ', 'Margin_YQ', 'Margin_Name']
    
    for col in cols:
        try:
            cursor.execute(f"ALTER TABLE earnings_tracker ADD COLUMN IF NOT EXISTS {col} TEXT")
        except Exception:
            conn.rollback() # Rollback required in postgres if an error occurs

    conn.commit()
    conn.close()

def delete_scrip_from_db(scrip_code):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM earnings_tracker WHERE Scrip_Code = %s OR NSE_Symbol = %s", (str(scrip_code), str(scrip_code)))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    if deleted > 0: print(f"✅ Successfully removed {scrip_code} from the tracking database.")
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
    df_results = df_results.drop_duplicates(subset=['ATTACHMENTNAME']).copy()
    
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
            if data.get('market_cap', 0) < 500:
                print(f"⏳ Deleting {scrip}: Market Cap ({data.get('market_cap')} Cr) is below 500 Cr.")
                cursor.execute("DELETE FROM earnings_tracker WHERE Scrip_Code = %s AND Target_Quarter = %s", (scrip, TARGET_QUARTER))
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
    <div class="max-w-7xl mx-auto mt-8 p-6 bg-white rounded-lg shadow-md">
        {% block content %}{% endblock %}
    </div>
    <script>
    $(document).ready(function() { 
        var table = $('#dataTable').DataTable({"pageLength": 25, "order": [[ 4, "desc" ]]}); 
        
        // Handle delete button clicks securely
        $('#dataTable tbody').on('click', '.delete-btn', function() {
            var scripCode = $(this).data('scrip');
            var row = $(this).closest('tr');
            
            if(confirm('Are you sure you want to delete ' + scripCode + '?')) {
                $.ajax({
                    url: '/api/delete-scrip',
                    type: 'POST',
                    data: { 
                        scrip: scripCode,
                        key: '{{ secret_key }}' // Injected securely via Jinja template
                    },
                    success: function(result) {
                        if(result.status === 'success') {
                            table.row(row).remove().draw();
                        } else {
                            alert('Error: ' + result.message);
                        }
                    },
                    error: function(xhr) {
                        alert('Error deleting scrip. Unauthorized or invalid key.');
                    }
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
<h2 class="text-xl font-bold mb-4 text-gray-800">Tracked Announcements ({{ target_quarter }})</h2>
<div class="overflow-x-auto" style="min-height: 400px;">
    <table id="dataTable" class="display w-full text-sm text-left text-gray-500">
        <thead class="text-xs text-gray-700 uppercase bg-gray-50">
            <tr>
                <th>Scrip / NSE</th>
                <th>Company</th>
                <th>Announcement Date</th>
                <th>Rating</th>
                <th>Screener Status</th>
                <th>Telegram</th>
                <th>Sales YoY</th>
                <th>PAT YoY</th>
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
                <td class="px-4 py-3">{{ row[2] }}</td>
                <td class="px-4 py-3">
                    {% if row[3] == 'Excellent' %}<span class="px-2 py-1 bg-green-100 text-green-800 rounded">Excellent</span>
                    {% elif row[3] == 'Good' %}<span class="px-2 py-1 bg-blue-100 text-blue-800 rounded">Good</span>
                    {% elif row[3] == 'Flat' %}<span class="px-2 py-1 bg-gray-100 text-gray-800 rounded">Flat</span>
                    {% elif row[3] == 'Bad' %}<span class="px-2 py-1 bg-red-100 text-red-800 rounded">Bad</span>
                    {% else %}-{% endif %}
                </td>
                <td class="px-4 py-3">{{ row[4] }}</td>
                <td class="px-4 py-3">{{ row[5] }}</td>
                <td class="px-4 py-3 whitespace-nowrap">
                    {% if row[6] is not none %}
                        {% if row[6]|float > 0 %}<span class="text-green-600 font-medium">▲ {{ "%.2f"|format(row[6]|float) }}%</span>
                        {% elif row[6]|float < 0 %}<span class="text-red-600 font-medium">▼ {{ "%.2f"|format(row[6]|float|abs) }}%</span>
                        {% else %}<span class="text-gray-600">{{ "%.2f"|format(row[6]|float) }}%</span>{% endif %}
                    {% else %}-{% endif %}
                </td>
                <td class="px-4 py-3 whitespace-nowrap">
                    {% if row[7] is not none %}
                        {% if row[7]|float > 0 %}<span class="text-green-600 font-medium">▲ {{ "%.2f"|format(row[7]|float) }}%</span>
                        {% elif row[7]|float < 0 %}<span class="text-red-600 font-medium">▼ {{ "%.2f"|format(row[7]|float|abs) }}%</span>
                        {% else %}<span class="text-gray-600">{{ "%.2f"|format(row[7]|float) }}%</span>{% endif %}
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
{% endblock %}
"""

app.jinja_env.loader = ChoiceLoader([DictLoader({'base': HTML_TEMPLATE}), app.jinja_env.loader])

@app.route("/")
def dashboard():
    init_db()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT Scrip_Code, Company_Name, BSE_Announcement_Date, Rating, Screener_Status, Telegram_Status, 
               Sales_YoY, PAT_YoY, NSE_Symbol, Sales_CQ, Sales_PQ, Sales_YQ, PAT_CQ, PAT_PQ, PAT_YQ,
               Sales_QoQ, PAT_QoQ, Margin_QoQ, Margin_CQ, Margin_PQ, Margin_YQ, Margin_Name, Margin_Change
        FROM earnings_tracker WHERE Target_Quarter = %s
    """, (TARGET_QUARTER,))
    rows = cursor.fetchall()
    conn.close()
    return render_template_string(DASHBOARD_PAGE, rows=rows, target_quarter=TARGET_QUARTER, secret_key=CRON_SECRET_KEY)

@app.route("/api/trigger-cron", methods=["GET"])
def trigger_cron():
    provided_key = request.args.get("key")
    if provided_key != CRON_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    
    start_date = request.args.get("start")
    end_date = request.args.get("end")
    
    def run_pipeline():
        try:
            fetch_new_bse_announcements(start_date, end_date)
            process_pending_results()
        except Exception as e:
            print(f"Pipeline error: {e}")
            
    thread = threading.Thread(target=run_pipeline)
    thread.start()
    
    msg = f"Pipeline started. Fetching from {start_date or 'yesterday'} to {end_date or 'today'}."
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

# ==========================================
# RUN LOGIC
# ==========================================
if __name__ == "__main__":
    init_db()
    if len(sys.argv) == 3 and sys.argv[1] == '--delete':
        delete_scrip_from_db(sys.argv[2])
        sys.exit(0)
    elif len(sys.argv) == 4 and sys.argv[1] == '--test-dates':
        fetch_new_bse_announcements(sys.argv[2], sys.argv[3])
        process_pending_results()
    elif len(sys.argv) > 1 and sys.argv[1] == '--cron':
        fetch_new_bse_announcements()
        process_pending_results()
    else:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
