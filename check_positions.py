"""
obtain features from RL database
"""
import os
import sys
sys.path.insert(0, '.')
import clickhouse_connect
import pandas as pd
import tqdm
from icecream import ic
from pathlib import Path
# from dotenv import load_dotenv
# from utility.utils import timer
# load_dotenv()
# PROJECT_DROPBOX_PATH = os.getenv("PROJECT_DROPBOX_PATH")
PROJECT_DROPBOX_PATH = "D:/fenghe/dropbox/Dropbox/LMSW (Diversity Washing Through the Boardroom)/1. Data"
print(PROJECT_DROPBOX_PATH)



# functions
def connect_to_clickhouse():
    client = clickhouse_connect.get_client(host='192.168.204.128', 
                                           port=8123, 
                                           username='default', 
                                           password='pm19951014',
                                           connect_timeout=600,
                                           send_receive_timeout=600)
    client.command("USE revelio071625")
    return client

#################### establish Clickhouse connection ####################

client = connect_to_clickhouse()

# Functions to execute commands by always connecting to Clickhouse first
def run_query_df(query):
    client = connect_to_clickhouse()
    return client.query_df(query)



def distinct_role(k_value):
    """
    Get distinct role values for a specific k value

    Args:
        k_value: The k value (e.g., 50, 150, 500, etc.)

    Returns:
        DataFrame with distinct role values
    """
    col_name = f"role_k{k_value}_v3"
    query = f"""
    SELECT DISTINCT {col_name}
    FROM temp_processed_global_position
    WHERE {col_name} IS NOT NULL
    ORDER BY {col_name}
    """
    return run_query_df(query)


def get_all_distinct_roles():
    """
    Get distinct roles for all role categories

    Returns:
        Dictionary with k values as keys and DataFrames as values
    """
    k_values = [50, 150, 500, 1000, 1500, 5000, 10000, 15000]
    results = {}

    for k in k_values:
        print(f"Fetching distinct roles for k={k}...")
        results[f"k{k}"] = distinct_role(k)
        print(f"  Found {len(results[f'k{k}'])} distinct values")

    return results


# Get all distinct roles
print("\n" + "="*60)
print("Fetching distinct roles for all categories...")
print("="*60)
all_roles = get_all_distinct_roles()

# Individual DataFrames for convenience
df_k50 = all_roles['k50']
df_k150 = all_roles['k150']
df_k500 = all_roles['k500']
df_k1000 = all_roles['k1000']
df_k1500 = all_roles['k1500']
df_k5000 = all_roles['k5000']
df_k10000 = all_roles['k10000']
df_k15000 = all_roles['k15000']

print("\n" + "="*60)
print("Summary:")
print("="*60)
for k, df in all_roles.items():
    print(f"{k}: {len(df)} unique roles")