# accounts_common.py - Shared constants for all scripts
import os

# BigQuery target (same for both historic and daily)
PROJECT_ID = os.environ['BIGQUERY_PROJECT_ID']
DATASET_ID = os.environ['BIGQUERY_DATASET_ID']
COMBINED_TABLE = os.environ['BIGQUERY_TABLE_ID']

# ────────────────────────────────────────────────
# Play Console buckets → name & ID mapping
# ────────────────────────────────────────────────
financial_buckets_1 = {
    'Rosy Apps Studio': 'pubsite_prod_8988929219322388461',
    'Hifi Studio': 'pubsite_prod_4883153693061392761'
}

financial_buckets_2 = {
}

financial_buckets_3 = {
}

play_id_to_name = {v: k for k, v in financial_buckets_1.items()}
play_id_to_name.update({v: k for k, v in financial_buckets_2.items()})
play_id_to_name.update({v: k for k, v in financial_buckets_3.items()})

# ────────────────────────────────────────────────
# AdMob accounts → name & ID mapping
# ────────────────────────────────────────────────
admob_accounts = [
    {
        "account_name": "Mobi-pixel-Admob",
        "client_id": os.getenv("CLIENT_ID_MOBI_PIXEL"),
        "client_secret": os.getenv("CLIENT_SECRET_MOBI_PIXEL"),
        "refresh_token": os.getenv("REFRESH_TOKEN_MOBI_PIXEL"),
        "publisher_id": "pub-3612093051527159"
    }
]

admob_id_to_name = {acc["publisher_id"]: acc["account_name"] for acc in admob_accounts}

# ────────────────────────────────────────────────
# Google Ads customer IDs → name mapping
# ────────────────────────────────────────────────
customer_ids_account1 = [ "8193128547", "1753030144" ]

customer_ids_account2 = []

# Combined list (for backward compatibility if needed)
customer_ids = customer_ids_account1 + customer_ids_account2

customer_id_to_account_name = {
    "8193128547": "Rosy Apps-USD", 
    "1753030144": "WA-USD",
    "2597822203": "Airnet Information Technology LLC"
    
}
