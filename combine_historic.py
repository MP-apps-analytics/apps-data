# combine_historic.py - Full history from 2025-01-01 to today (manual run, replace/append table)
import base64
import re
import zipfile
from io import BytesIO
from datetime import datetime, date, timedelta
import pandas as pd
import os
import requests
import json
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from pandas_gbq import to_gbq
from google.api_core.exceptions import NotFound, Forbidden, GoogleAPIError
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Import shared constants
from accounts_common import (
    PROJECT_ID, DATASET_ID, COMBINED_TABLE,
    financial_buckets_1, financial_buckets_2, play_id_to_name,
    admob_accounts, admob_id_to_name,
    customer_ids, customer_id_to_account_name
)

# ────────────────────────────────────────────────
# GCP Credentials from GitHub Secrets
# ────────────────────────────────────────────────
cred_path_1 = '/tmp/sa1.json'
cred_path_2 = '/tmp/sa2.json'
cred_path_bq = '/tmp/sa-bq.json'

with open(cred_path_1, 'wb') as f: f.write(base64.b64decode(os.environ['GCP_CREDENTIALS_BASE64']))
with open(cred_path_2, 'wb') as f: f.write(base64.b64decode(os.environ['GCP_CREDENTIALS_BASE64_2']))
with open(cred_path_bq, 'wb') as f: f.write(base64.b64decode(os.environ['GCP_CREDENTIALS_BASE64_BQ']))

credentials_1 = service_account.Credentials.from_service_account_file(cred_path_1)
credentials_2 = service_account.Credentials.from_service_account_file(cred_path_2)
credentials_bq = service_account.Credentials.from_service_account_file(cred_path_bq)

storage_client_1 = storage.Client(credentials=credentials_1)
storage_client_2 = storage.Client(credentials=credentials_2)
bq_client = bigquery.Client(credentials=credentials_bq)

# ────────────────────────────────────────────────
# Date range: your current range
# ────────────────────────────────────────────────
START_DATE = date(2026, 2, 1)
END_DATE = date(2026, 4, 30)
start_date_str = START_DATE.strftime("%Y-%m-%d")
end_date_str = END_DATE.strftime("%Y-%m-%d")

# Months needed for Play reports
months_needed = set()
current = START_DATE
while current <= END_DATE:
    months_needed.add(current.strftime('%Y%m'))
    if current.month == 12:
        current = date(current.year + 1, 1, 1)
    else:
        current = date(current.year, current.month + 1, 1)

# ────────────────────────────────────────────────
# Load currency rates (clean commas & force float)
# ────────────────────────────────────────────────
try:
    rates_df = pd.read_csv('currency_rates.csv')
    rates_df['rate'] = rates_df['rate'].astype(str).str.replace(',', '').astype(float)
    rates_df['date'] = pd.to_datetime(rates_df['date'], format='%Y-%m-%d', errors='coerce')
    rates_df = rates_df.dropna(subset=['date', 'rate']).sort_values(['currency', 'date'])
    print(f"Loaded {len(rates_df)} clean rows from currency_rates.csv")
except Exception as e:
    print(f"Currency rates error: {e}")
    rates_df = pd.DataFrame()

# ────────────────────────────────────────────────
# Load short country mapping
# ────────────────────────────────────────────────
country_map = {}
try:
    country_df = pd.read_csv('short_country_mapping.csv')
    country_df['short_code'] = country_df['short_code'].str.upper().str.strip()
    country_df['country_name'] = country_df['country_name'].str.strip()
    country_map = dict(zip(country_df['short_code'], country_df['country_name']))
    print(f"Loaded {len(country_map)} country mappings")
except Exception as e:
    print(f"Short country mapping error: {e}")

def map_country_code(code):
    if pd.isna(code):
        return "Unknown"
    upper_code = str(code).upper().strip()
    return country_map.get(upper_code, upper_code)

# ────────────────────────────────────────────────
# Play Console buckets
# ────────────────────────────────────────────────
buckets = {}
for acc, b in financial_buckets_1.items():
    buckets[acc] = {'bucket': b, 'client': storage_client_1}
for acc, b in financial_buckets_2.items():
    buckets[acc] = {'bucket': b, 'client': storage_client_2}

# ────────────────────────────────────────────────
# Helper functions (unchanged)
# ────────────────────────────────────────────────
def clean_column_names(df):
    df.columns = (
        df.columns.str.strip()
        .str.replace(r'\s+', '_', regex=True)
        .str.replace(r'[^\w_]', '', regex=True)
    )
    return df

def extract_publisher_id(bucket_name):
    parts = bucket_name.split('_')
    return parts[-1] if len(parts) > 1 else 'unknown'

def list_monthly_files(bucket_name, prefix, storage_client, report_type):
    try:
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        matching = []
        for blob in blobs:
            name = blob.name
            if report_type == 'sales':
                if re.search(r'salesreport_(\d{6})\.zip$', name):
                    month = re.search(r'(\d{6})', name).group(1)
                    if month in months_needed:
                        matching.append(name)
            else:
                if name.endswith('.csv') and re.search(r'_(\d{6})_', name):
                    month = re.search(r'_(\d{6})_', name).group(1)
                    if month in months_needed:
                        matching.append(name)
        return matching
    except Exception as e:
        print(f"Error listing files in {bucket_name}: {e}")
        return []

def read_report_file(bucket_name, file_path, storage_client, report_type='sales'):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    raw_bytes = blob.download_as_bytes()
    if file_path.endswith('.zip'):
        with zipfile.ZipFile(BytesIO(raw_bytes)) as z:
            csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
            if not csv_files:
                return None
            content = z.read(csv_files[0])
    else:
        content = raw_bytes
    encodings = (
        [('utf-16-sig', 'utf-16-sig'), ('utf-16', 'utf-16'), ('utf-8', 'utf-8')]
        if report_type == 'subscriptions' else
        [('utf-8', 'utf-8'), ('utf-16', 'utf-16')]
    )
    df = None
    for enc, _ in encodings:
        try:
            if report_type == 'subscriptions':
                temp_df = pd.read_csv(
                    BytesIO(content),
                    header=None,
                    encoding=enc,
                    on_bad_lines='skip',
                    low_memory=False,
                    encoding_errors='replace'
                )
                expected = [
                    'Date', 'Package_Name', 'Product_ID', 'Country',
                    'Base_Plan_ID', 'Offer_ID', 'New_Subscriptions',
                    'Cancelled_Subscriptions', 'Active_Subscriptions'
                ]
                if len(temp_df.columns) >= len(expected):
                    temp_df.columns = expected + [f'Unnamed_{i}' for i in range(len(temp_df.columns) - len(expected))]
                else:
                    temp_df.columns = expected[:len(temp_df.columns)]
                df = temp_df
            else:
                df = pd.read_csv(
                    BytesIO(content),
                    encoding=enc,
                    on_bad_lines='skip',
                    low_memory=False,
                    encoding_errors='replace'
                )
            break
        except:
            continue
    return df

def fetch_report_data(report_prefix, report_type='sales'):
    all_dfs = []
    print(f"{'📊' if report_type == 'sales' else '🧾'} Processing {report_type.capitalize()} Reports...")
    for account, info in buckets.items():
        bucket_name = info['bucket']
        client = info['client']
        prefix = f"{report_prefix}/"
        files = list_monthly_files(bucket_name, prefix, client, report_type)
        if not files:
            continue
        print(f" {account}: Found {len(files)} files")
        publisher = extract_publisher_id(bucket_name)
        for file_path in files:
            df = read_report_file(bucket_name, file_path, client, report_type)
            if df is not None and not df.empty:
                df['account_name'] = account
                df['publisher_id'] = publisher
                all_dfs.append(df)
    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        print(f"Collected {len(combined)} raw rows from {report_prefix}")
        return combined
    print(f"No data from {report_prefix}")
    return pd.DataFrame()

def get_access_token(client_id, client_secret, refresh_token):
    try:
        response = requests.post(
            'https://oauth2.googleapis.com/token',
            data={
                'client_id': client_id,
                'client_secret': client_secret,
                'refresh_token': refresh_token,
                'grant_type': 'refresh_token',
            }
        )
        response.raise_for_status()
        return response.json()['access_token']
    except requests.exceptions.HTTPError as e:
        print(f"❌ Failed to fetch access token: {e.response.status_code} - {e.response.text}")
        raise

def get_app_id_map(publisher_id, access_token):
    url = f"https://admob.googleapis.com/v1/accounts/{publisher_id}/apps"
    headers = {"Authorization": f"Bearer {access_token}"}
    app_id_map = {}
    while url:
        resp = requests.get(url, headers=headers).json()
        for app in resp.get("apps", []):
            app_id = app.get("appId")
            package = app.get("linkedAppInfo", {}).get("appStoreId", "unknown")
            app_id_map[app_id] = package
        next_page = resp.get("nextPageToken")
        url = f"https://admob.googleapis.com/v1/accounts/{publisher_id}/apps?pageToken={next_page}" if next_page else None
    return app_id_map

def fetch_admob_geo_report(publisher_id, access_token, app_id_map, account_name):
    headers = {"Authorization": f"Bearer {access_token}"}
    data = []
    start = START_DATE
    end = END_DATE
    while start <= end:
        chunk_end = min(start + timedelta(days=10), end)
        print(f"📆 Fetching AdMob {start} → {chunk_end} for {account_name}...")
        body = {
            "reportSpec": {
                "dateRange": {
                    "startDate": {"year": start.year, "month": start.month, "day": start.day},
                    "endDate": {"year": chunk_end.year, "month": chunk_end.month, "day": chunk_end.day}
                },
                "dimensions": ["DATE", "APP", "COUNTRY"],
                "metrics": [
                    "ESTIMATED_EARNINGS", "IMPRESSIONS", "CLICKS",
                    "MATCHED_REQUESTS", "AD_REQUESTS"
                ],
                "sortConditions": [{"dimension": "DATE", "order": "ASCENDING"}],
                "localizationSettings": {"currencyCode": "USD", "languageCode": "en-US"}
            }
        }
        try:
            response = requests.post(
                f"https://admob.googleapis.com/v1/accounts/{publisher_id}/mediationReport:generate",
                headers=headers,
                json=body
            )
            response.raise_for_status()
            result = response.json()
            rows = result.get('rows', []) if isinstance(result, dict) else result
            for record in rows:
                if 'row' not in record:
                    continue
                dims = record['row']['dimensionValues']
                metrics = record['row']['metricValues']
                app_id = dims["APP"]["value"]
                country_code = dims["COUNTRY"].get("value", "UNKNOWN")
                row = {
                    "Date": datetime.strptime(dims["DATE"]["value"], "%Y%m%d").date(),
                    "app_name": dims["APP"].get("displayLabel", "Unknown"),
                    "Package_Name": app_id_map.get(app_id, "unknown"),
                    "Country": country_code,
                    "ad_revenue": round(int(metrics["ESTIMATED_EARNINGS"]["microsValue"]) / 1_000_000, 4),
                    "Impressions_admob": int(metrics["IMPRESSIONS"]["integerValue"]),
                    "clicks_admob": int(metrics["CLICKS"]["integerValue"]),
                    "match_requests_admob": int(metrics.get("MATCHED_REQUESTS", {}).get("integerValue", 0)),
                    "ad_requests_admob": int(metrics.get("AD_REQUESTS", {}).get("integerValue", 0)),
                    "account_name": account_name,
                    "publisher_id": publisher_id
                }
                data.append(row)
        except Exception as e:
            print(f"❌ Error fetching AdMob {start} → {chunk_end}: {e}")
        start = chunk_end + timedelta(days=1)
    return data

# ────────────────────────────────────────────────
# Main execution
# ────────────────────────────────────────────────
print("Starting HISTORIC full fetch: Google Play + AdMob + Google Ads")
print(f"Range: {START_DATE} → {END_DATE}\n")

# ── Google Play: Sales ───────────────────────────
sales_raw = fetch_report_data('sales', 'sales')
sales = pd.DataFrame()
if not sales_raw.empty:
    sales = sales_raw.copy()
    sales = clean_column_names(sales)

    col_map = {col.lower(): col for col in sales.columns}
    rename_map = {}
    for p in ['orderchargeddate', 'order_charged_date', 'chargeddate', 'orderdate', 'date']:
        if p in col_map:
            rename_map[col_map[p]] = 'Date'
            break
    for p in ['packageid', 'package_id', 'productid', 'product_id', 'packagename']:
        if p in col_map:
            rename_map[col_map[p]] = 'Package_Name'
            break
    for p in ['countryofbuyer', 'country_of_buyer', 'country', 'buyercountry']:
        if p in col_map:
            rename_map[col_map[p]] = 'Country'
            break

    other = {
        'ordernumber': 'Order_Number',
        'financialstatus': 'Financial_Status',
        'currencyofsale': 'Currency_of_Sale',
        'taxescollected': 'Taxes_Collected',
        'chargedamount': 'Charged_Amount',
    }
    for low, orig in col_map.items():
        for k, v in other.items():
            if k in low:
                rename_map[orig] = v
                break

    sales.rename(columns=rename_map, inplace=True)

    # Strong cleaning & deduplication
    if 'Charged_Amount' in sales.columns:
        sales['Charged_Amount'] = pd.to_numeric(sales['Charged_Amount'].astype(str).str.replace(',', '').str.replace(r'[^\d.-]', '', regex=True), errors='coerce').fillna(0)
    if 'Taxes_Collected' in sales.columns:
        sales['Taxes_Collected'] = pd.to_numeric(sales['Taxes_Collected'].astype(str).str.replace(',', '').str.replace(r'[^\d.-]', '', regex=True), errors='coerce').fillna(0)

    key_cols = ['Order_Number', 'Date', 'Package_Name', 'Country', 'Financial_Status', 'Charged_Amount']
    sales = sales.drop_duplicates(subset=[c for c in key_cols if c in sales.columns], keep='last')
    print(f"After dedup: {len(sales)} rows (original: {len(sales_raw)})")

    if 'Date' in sales.columns:
        sales['Date'] = pd.to_datetime(sales['Date'], format='%Y-%m-%d', errors='coerce')
        sales = sales[sales['Date'].notna()]
        sales = sales[(sales['Date'].dt.date >= START_DATE) & (sales['Date'].dt.date <= END_DATE)]

    if 'Order_Number' in sales.columns and 'Financial_Status' in sales.columns:
        sales['Order_Number'] = sales['Order_Number'].astype(str).str.strip()
        sales['Subscriptions'] = 0
        sales.loc[sales['Financial_Status'] == 'Charged', 'Subscriptions'] = 1
        sales.loc[sales['Financial_Status'] == 'Refund', 'Subscriptions'] = -1
        sales['Without_Trial'] = 0
        sales.loc[(~sales['Order_Number'].str.contains(r'\.\.', na=False)) & (sales['Financial_Status'] == 'Charged'), 'Without_Trial'] = 1
        sales.loc[(~sales['Order_Number'].str.contains(r'\.\.', na=False)) & (sales['Financial_Status'] == 'Refund'), 'Without_Trial'] = -1
        sales['Convert'] = 0
        sales.loc[sales['Order_Number'].str.endswith('..0', na=False) & (sales['Financial_Status'] == 'Charged'), 'Convert'] = 1
        sales.loc[sales['Order_Number'].str.endswith('..0', na=False) & (sales['Financial_Status'] == 'Refund'), 'Convert'] = -1
        renew_pattern = r'\.\.(\d+)$'
        renew_matches = sales['Order_Number'].str.extract(renew_pattern, expand=False)
        renew_num = pd.to_numeric(renew_matches, errors='coerce').fillna(-1)
        sales['Renew'] = 0
        mask_renew = (renew_num >= 1) & (renew_num <= 100)
        sales.loc[mask_renew & (sales['Financial_Status'] == 'Charged'), 'Renew'] = 1
        sales.loc[mask_renew & (sales['Financial_Status'] == 'Refund'), 'Renew'] = -1

# ── Google Play: Subscriptions ───────────────────
subs_raw = fetch_report_data('financial-stats/subscriptions', 'subscriptions')
subs = pd.DataFrame()
if not subs_raw.empty:
    subs = subs_raw.copy()
    subs = clean_column_names(subs)
    
    # Rename including plan/offer columns (they exist in subscription reports)
    subs.rename(columns={
        'Date': 'Date',
        'Package_Name': 'Package_Name',
        'Country': 'Country',
        'Base_Plan_ID': 'Base_Plan_ID',
        'Offer_ID': 'Offer_ID',
        'New_Subscriptions': 'New_Subscriptions',
        'Cancelled_Subscriptions': 'Cancelled_Subscriptions',
        'Active_Subscriptions': 'Active_Subscriptions',
    }, inplace=True)

    # Step 1: Remove exact duplicate rows (very common in overlapping files)
    original_len = len(subs)
    subs = subs.drop_duplicates(keep='last')
    print(f"Subscriptions after removing exact duplicates: {len(subs)} rows (original: {original_len})")

    # Step 2: Deduplicate using plan/offer keys (removes most remaining overlaps)
    dedup_keys = ['Date', 'Package_Name', 'Country']
    if 'Base_Plan_ID' in subs.columns:
        dedup_keys.append('Base_Plan_ID')
    if 'Offer_ID' in subs.columns:
        dedup_keys.append('Offer_ID')
    
    subs = subs.drop_duplicates(subset=dedup_keys, keep='last')
    print(f"Subscriptions after plan/offer deduplication: {len(subs)} rows")

    if 'Date' in subs.columns:
        subs['Date'] = pd.to_datetime(subs['Date'], format='%Y-%m-%d', errors='coerce')
        subs = subs[subs['Date'].notna()]
        subs = subs[(subs['Date'].dt.date >= START_DATE) & (subs['Date'].dt.date <= END_DATE)]

# ── Combine Play sales + subs ────────────────────
play_df = pd.DataFrame()
sales_agg = pd.DataFrame(columns=['Date', 'Package_Name', 'Country'])
subs_agg = pd.DataFrame(columns=['Date', 'Package_Name', 'Country'])

# Sales aggregation (UNCHANGED - kept exactly as in your working code)
if not sales.empty:
    sales_for_agg = sales.copy()
    # Your original currency conversion logic
    if 'Currency_of_Sale' in sales_for_agg.columns:
        if not rates_df.empty:
            latest_rates = (
                rates_df.sort_values('date', ascending=False)
                .drop_duplicates(subset='currency', keep='first')
                .set_index('currency')['rate']
                .to_dict()
            )
            print(f"Using global latest rates for {len(latest_rates)} currencies")
        else:
            latest_rates = {}
            print("WARNING: No rates loaded → all conversions will use 1.0")
        def get_conversion_rate(currency):
            curr = str(currency).strip().upper()
            if curr == 'USD' or pd.isna(curr):
                return 1.0
            rate = latest_rates.get(curr, 1.0)
            if rate == 1.0 and curr != 'USD':
                print(f"WARNING: Currency '{curr}' not found → using 1.0")
            return float(rate) if rate != 0 else 1.0
        sales_for_agg['rate_local_per_usd'] = sales_for_agg['Currency_of_Sale'].apply(get_conversion_rate)
        sales_for_agg['rate_local_per_usd'] = pd.to_numeric(sales_for_agg['rate_local_per_usd'], errors='coerce').fillna(1.0)
        sales_for_agg['Charged_Amount_USD'] = sales_for_agg['Charged_Amount'].copy()
        sales_for_agg['Taxes_Collected_USD'] = sales_for_agg['Taxes_Collected'].copy()
        mask_convert = sales_for_agg['rate_local_per_usd'] != 1.0
        sales_for_agg.loc[mask_convert, 'Charged_Amount_USD'] = (
            sales_for_agg.loc[mask_convert, 'Charged_Amount'] /
            sales_for_agg.loc[mask_convert, 'rate_local_per_usd']
        ).round(2)
        sales_for_agg.loc[mask_convert, 'Taxes_Collected_USD'] = (
            sales_for_agg.loc[mask_convert, 'Taxes_Collected'] /
            sales_for_agg.loc[mask_convert, 'rate_local_per_usd']
        ).round(2)
        converted = mask_convert.sum()
        print(f"Converted {converted} rows using latest rates (out of {len(sales_for_agg)})")
        sales_for_agg = sales_for_agg.drop(columns=['rate_local_per_usd'], errors='ignore')
    else:
        sales_for_agg['Charged_Amount_USD'] = sales_for_agg['Charged_Amount'].fillna(0)
        sales_for_agg['Taxes_Collected_USD'] = sales_for_agg['Taxes_Collected'].fillna(0)
    
    agg_dict = {
        'Charged_Amount_USD': 'sum',
        'Taxes_Collected_USD': 'sum',
    }
    for col in ['Subscriptions', 'Without_Trial', 'Convert', 'Renew']:
        if col in sales_for_agg.columns:
            agg_dict[col] = 'sum'
    
    sales_agg = sales_for_agg.groupby(['Date', 'Package_Name', 'Country'], as_index=False).agg(agg_dict)

# ── FIXED Subscriptions aggregation ──
if not subs.empty:
    # Aggregate first at plan/offer level (prevents summing across different plans/offers)
    plan_keys = ['Date', 'Package_Name', 'Country']
    if 'Base_Plan_ID' in subs.columns:
        plan_keys.append('Base_Plan_ID')
    if 'Offer_ID' in subs.columns:
        plan_keys.append('Offer_ID')
    
    plan_agg = subs.groupby(plan_keys, as_index=False).agg({
        'New_Subscriptions': 'sum',
        'Cancelled_Subscriptions': 'sum',
        'Active_Subscriptions': 'max'  # max = correct for cumulative snapshot
    })
    print(f"Subscriptions aggregated at plan/offer level: {len(plan_agg)} rows")
    
    # Final roll-up to Date + Package + Country
    subs_agg = plan_agg.groupby(['Date', 'Package_Name', 'Country'], as_index=False).agg({
        'New_Subscriptions': 'sum',
        'Cancelled_Subscriptions': 'sum',
        'Active_Subscriptions': 'max'
    })
    print(f"Final subscriptions aggregated: {len(subs_agg)} rows")
    print("Active_Subscriptions stats after fix:", subs_agg['Active_Subscriptions'].describe())

# ── Merge sales + subs ────────────────────
play_df = pd.merge(sales_agg, subs_agg, how='outer', on=['Date', 'Package_Name', 'Country'])
play_df['Date'] = pd.to_datetime(play_df['Date']).dt.date
if 'Country' in play_df.columns:
    play_df['Country'] = play_df['Country'].apply(map_country_code)
if 'publisher_id' in play_df.columns:
    play_df['play_console_id'] = play_df['publisher_id']
    play_df['play_console_name'] = play_df['publisher_id'].map(play_id_to_name).fillna('Unknown')
else:
    play_df['play_console_id'] = 'Unknown'
    play_df['play_console_name'] = 'Unknown'
print(f"Play aggregated rows: {len(play_df)}")
if 'Charged_Amount_USD' in play_df.columns:
    print("Play USD revenue stats:", play_df['Charged_Amount_USD'].describe())

# ── AdMob data ───────────────────────────────────
admob_all = []
for acc in admob_accounts:
    print(f"\nProcessing AdMob account: {acc['account_name']}")
    try:
        token = get_access_token(acc["client_id"], acc["client_secret"], acc["refresh_token"])
        app_map = get_app_id_map(acc["publisher_id"], token)
        chunk = fetch_admob_geo_report(acc["publisher_id"], token, app_map, acc["account_name"])
        admob_all.extend(chunk)
        print(f" → {len(chunk)} rows")
    except Exception as e:
        print(f"Failed {acc['account_name']}: {e}")

admob_df = pd.DataFrame(admob_all)
if not admob_df.empty:
    if 'Country' in admob_df.columns:
        admob_df['Country'] = admob_df['Country'].apply(map_country_code)
    if 'publisher_id' in admob_df.columns:
        admob_df['ad_mob_id'] = admob_df['publisher_id']
        admob_df['ad_mob_name'] = admob_df['publisher_id'].map(admob_id_to_name).fillna('Unknown')
    else:
        admob_df['ad_mob_id'] = 'Unknown'
        admob_df['ad_mob_name'] = 'Unknown'
    print(f"Total AdMob rows: {len(admob_df)}")
else:
    print("No AdMob data retrieved")

# ── Google Ads data ──────────────────────────────
print(f"Fetching Google Ads data for period: {start_date_str} → {end_date_str}")
try:
    client = GoogleAdsClient.load_from_storage("google-ads.yaml")
except FileNotFoundError:
    raise FileNotFoundError("google-ads.yaml not found.")
except Exception as e:
    raise RuntimeError(f"Failed to initialize Google Ads client: {e}")

ga_service = client.get_service("GoogleAdsService")
query = f"""
SELECT
  campaign.id,
  campaign.name,
  segments.date,
  geographic_view.country_criterion_id,
  metrics.impressions,
  metrics.clicks,
  metrics.cost_micros,
  metrics.conversions,
  metrics.conversions_value,
  customer.currency_code,
  campaign.app_campaign_setting.app_id
FROM geographic_view
WHERE segments.date BETWEEN '{start_date_str}' AND '{end_date_str}'
ORDER BY segments.date, campaign.id, geographic_view.country_criterion_id
"""

gads_rows = []
print("Fetching data for all customer accounts...")
for customer_id in customer_ids:
    print(f" → Processing customer_id: {customer_id}")
    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        count = 0
        for row in response:
            count += 1
            account_name = customer_id_to_account_name.get(customer_id, "Unknown")
            gads_rows.append({
                "google_ads_customer_id": str(customer_id),
                "account_name": account_name,
                "date": row.segments.date,
                "campaign_id": int(row.campaign.id),
                "campaign_name": row.campaign.name,
                "country_criterion_id": int(row.geographic_view.country_criterion_id) if row.geographic_view.country_criterion_id else 0,
                "impressions": int(row.metrics.impressions) or 0,
                "clicks": int(row.metrics.clicks) or 0,
                "cost": float(row.metrics.cost_micros) / 1_000_000 if row.metrics.cost_micros else 0.0,
                "conversions": float(row.metrics.conversions) or 0.0,
                "conversion_value": float(row.metrics.conversions_value) or 0.0,
                "currency": row.customer.currency_code or "Unknown",
                "app_id": row.campaign.app_campaign_setting.app_id or None
            })
        print(f" Retrieved {count} rows")
    except GoogleAdsException as ex:
        print(f" ❌ Error for customer {customer_id}: {ex.request_id}")
        for error in ex.failure.errors:
            print(f" → {error.error_code}: {error.message}")
    except Exception as e:
        print(f" Unexpected error for {customer_id}: {e}")

gads_df = pd.DataFrame()
if gads_rows:
    gads_df = pd.DataFrame(gads_rows)
    csv_file = "gads_country_mapping.csv"
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"Country mapping CSV not found: {csv_file}")
    mapping_df = pd.read_csv(csv_file, dtype={'criterion_id': int})
    id_to_name = mapping_df.set_index('criterion_id')['country_name'].to_dict()
    gads_df['Country'] = gads_df['country_criterion_id'].map(id_to_name).fillna('Unknown')
    gads_df = gads_df.drop(columns=['country_criterion_id'])
    gads_df['Date'] = pd.to_datetime(gads_df['date']).dt.date
    gads_df = gads_df.drop(columns=['date'])
    gads_df = gads_df.rename(columns={'app_id': 'Package_Name'})
    agg_dict_gads = {
        'impressions': 'sum',
        'clicks': 'sum',
        'cost': 'sum',
        'conversions': 'sum',
        'conversion_value': 'sum'
    }
    gads_df = gads_df.groupby(['Date', 'Package_Name', 'Country'], as_index=False).agg(agg_dict_gads)
    gads_df = gads_df.rename(columns={
        'impressions': 'impressions_gads',
        'clicks': 'clicks_gads',
        'cost': 'cost_gads',
        'conversions': 'conversions_gads',
        'conversion_value': 'conversion_value_gads'
    })
    if 'google_ads_customer_id' in gads_df.columns:
        gads_df['g_ads_id'] = gads_df['google_ads_customer_id']
        gads_df['g_ads_name'] = gads_df['google_ads_customer_id'].map(customer_id_to_account_name).fillna('Unknown')
    else:
        gads_df['g_ads_id'] = 'Unknown'
        gads_df['g_ads_name'] = 'Unknown'
    print(f"Google Ads aggregated rows: {len(gads_df)}")
else:
    print("No Google Ads data retrieved")

# ── Final merge ──────────────────────────────────────
print("\nMerging Play + AdMob + Google Ads data...")
if not play_df.empty and not admob_df.empty:
    merged_play_admob = pd.merge(
        play_df,
        admob_df,
        on=['Date', 'Package_Name', 'Country'],
        how='outer',
        suffixes=('', '_admob')
    )
    for col in ['Charged_Amount_USD', 'Taxes_Collected_USD', 'Subscriptions',
                'Without_Trial', 'Convert', 'Renew', 'New_Subscriptions',
                'Cancelled_Subscriptions', 'Active_Subscriptions']:
        if col in merged_play_admob.columns:
            merged_play_admob[col] = merged_play_admob[col].fillna(0)
    for col in ['ad_revenue', 'Impressions_admob', 'clicks_admob',
                'match_requests_admob', 'ad_requests_admob']:
        if col in merged_play_admob.columns:
            merged_play_admob[col] = merged_play_admob[col].fillna(0)
    if 'app_name' in merged_play_admob.columns:
        merged_play_admob['app_name'] = merged_play_admob['app_name'].fillna("Unknown")
elif not play_df.empty:
    merged_play_admob = play_df.copy()
    for c in ['ad_revenue', 'Impressions_admob', 'clicks_admob',
              'match_requests_admob', 'ad_requests_admob']:
        merged_play_admob[c] = 0
    merged_play_admob['app_name'] = "Unknown"
elif not admob_df.empty:
    merged_play_admob = admob_df.copy()
    for c in ['Charged_Amount_USD', 'Taxes_Collected_USD', 'Subscriptions',
              'Without_Trial', 'Convert', 'Renew', 'New_Subscriptions',
              'Cancelled_Subscriptions', 'Active_Subscriptions']:
        merged_play_admob[c] = 0
    if 'app_name' in merged_play_admob.columns:
        merged_play_admob['app_name'] = merged_play_admob['app_name'].fillna("Unknown")
else:
    merged_play_admob = pd.DataFrame()

if not merged_play_admob.empty and not gads_df.empty:
    final_df = pd.merge(
        merged_play_admob,
        gads_df,
        on=['Date', 'Package_Name', 'Country'],
        how='outer'
    )
    for col in ['impressions_gads', 'clicks_gads', 'cost_gads',
                'conversions_gads', 'conversion_value_gads']:
        if col in final_df.columns:
            final_df[col] = final_df[col].fillna(0)
elif not merged_play_admob.empty:
    final_df = merged_play_admob.copy()
    for c in ['impressions_gads', 'clicks_gads', 'cost_gads',
              'conversions_gads', 'conversion_value_gads']:
        final_df[c] = 0
elif not gads_df.empty:
    final_df = gads_df.copy()
    for c in ['Charged_Amount_USD', 'Taxes_Collected_USD', 'Subscriptions',
              'Without_Trial', 'Convert', 'Renew', 'New_Subscriptions',
              'Cancelled_Subscriptions', 'Active_Subscriptions',
              'ad_revenue', 'Impressions_admob', 'clicks_admob',
              'match_requests_admob', 'ad_requests_admob']:
        final_df[c] = 0
    final_df['app_name'] = "Unknown"
else:
    final_df = pd.DataFrame()
    print("No data from any source → nothing to upload")

# ── Safer dtype conversion for subscription columns ──
for col in ['New_Subscriptions', 'Cancelled_Subscriptions', 'Active_Subscriptions']:
    if col in final_df.columns:
        try:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0).astype('Int64')
        except (TypeError, ValueError):
            print(f"Warning: Could not cast {col} to Int64 - using float64")
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0).astype('float64')

# ── Upload to BigQuery ───────────────────────────
if not final_df.empty:
    final_df = clean_column_names(final_df)

    # Debug: show dtypes and sample before upload
    print("\nDataFrame dtypes before upload:")
    print(final_df.dtypes)
    print("\nSample of problematic columns (first 5 rows):")
    problematic = ['New_Subscriptions', 'Cancelled_Subscriptions', 'Active_Subscriptions', 'Charged_Amount_USD']
    if all(col in final_df.columns for col in problematic):
        print(final_df[problematic].head())

    # Force ALL numeric columns to float64 (safest for append)
    numeric_types = ['int64', 'Int64', 'float64', 'int32', 'uint64']
    numeric_cols = final_df.select_dtypes(include=numeric_types).columns
    final_df[numeric_cols] = final_df[numeric_cols].astype('float64')

    # Replace NaN with 0 in numeric columns (BigQuery prefers no nulls when appending)
    final_df[numeric_cols] = final_df[numeric_cols].fillna(0)

    desired_columns = [
        'Date', 'Package_Name', 'Country', 'app_name',
        'play_console_name', 'play_console_id',
        'ad_mob_name', 'ad_mob_id',
        'g_ads_name', 'g_ads_id',
        'Charged_Amount_USD', 'Taxes_Collected_USD',
        'Subscriptions', 'Without_Trial', 'Convert', 'Renew',
        'New_Subscriptions', 'Cancelled_Subscriptions', 'Active_Subscriptions',
        'ad_revenue', 'Impressions_admob', 'clicks_admob',
        'match_requests_admob', 'ad_requests_admob',
        'impressions_gads', 'clicks_gads', 'cost_gads',
        'conversions_gads', 'conversion_value_gads',
        'account_name', 'publisher_id'
    ]
    existing = [c for c in desired_columns if c in final_df.columns]
    final_df = final_df[existing]

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{COMBINED_TABLE}"

    print(f"\nAttempting append to table: {table_id}")
    print(f"DataFrame shape: {final_df.shape}")
    print(f"Numeric columns forced to float64: {list(numeric_cols)}")

    try:
        to_gbq(
            final_df,
            f"{DATASET_ID}.{COMBINED_TABLE}",
            project_id=PROJECT_ID,
            if_exists='append',
            credentials=bq_client._credentials,
            # No table_schema → let pandas-gbq infer and append
        )
        print(f"\nSuccessfully appended {len(final_df)} rows to {DATASET_ID}.{COMBINED_TABLE}")
    except Exception as e:
        print(f"\nUpload failed: {str(e)}")
        print("\nNext steps to resolve:")
        print("1. In BigQuery console, run this to make numeric columns FLOAT64 (safer for append):")
        print("ALTER TABLE `{table_id}`")
        print("ALTER COLUMN New_Subscriptions SET DATA TYPE FLOAT64,")
        print("ALTER COLUMN Cancelled_Subscriptions SET DATA TYPE FLOAT64,")
        print("ALTER COLUMN Active_Subscriptions SET DATA TYPE FLOAT64,")
        print("ALTER COLUMN Charged_Amount_USD SET DATA TYPE FLOAT64,")
        print("ALTER COLUMN Taxes_Collected_USD SET DATA TYPE FLOAT64,")
        print("ALTER COLUMN ad_revenue SET DATA TYPE FLOAT64,")
        print("ALTER COLUMN cost_gads SET DATA TYPE FLOAT64,")
        print("ALTER COLUMN conversion_value_gads SET DATA TYPE FLOAT64;")
        print("2. Or, as one-time fix: change if_exists='replace' below and run once")
        print("   (then change back to 'append' for future runs)")
        print("3. Re-run the script after altering schema")
else:
    print("\nNo combined data to upload.")

print("\nHistoric full fetch finished.")
