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
                                           password='YOUR_PASSWORD_HERE',
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


##################### create a temp table that documents RCIDs #####################
def create_temp_rcid_table(rcid_list, batch_size=100_000):
    """
    Create a temporary ClickHouse table with required RCIDs only
    """
    # drop & create temp table
    run_query_df("DROP TABLE IF EXISTS revelio071625.tmp_global_rcids")
    run_query_df("""
        CREATE TABLE revelio071625.tmp_global_rcids (
            rcid Int32
        ) ENGINE = Memory
    """)
    # insert RCIDs in batches
    for i in range(0, len(rcid_list), batch_size):
        batch = rcid_list[i:i + batch_size]
        values = ",".join(f"({int(rcid)})" for rcid in batch)
        insert_sql = f"INSERT INTO revelio071625.tmp_global_rcids VALUES {values}"
        run_query_df(insert_sql)


def create_temp_table():
    run_query_df("""
        CREATE OR REPLACE TABLE revelio071625.temp_processed_global_position
            ENGINE MergeTree()
            ORDER BY user_id AS
            SELECT
                  rowNumberInAllBlocks() + 1 AS position_id,
                  user_id,
                    postion_id,
                    weight,
                    salary,
                    startdate,
                    enddate,
                    rcid,
                    title_raw,
                    role_k50_v3,
                    role_k150_v3,
                    role_k500_v3,
                    role_k1000_v3,
                    role_k1500_v3,
                    role_k5000_v3, 
                    role_k10000_v3, 
                    role_k15000_v3,
                    onet_title,
                    description
            FROM revelio071625.academic_individual_position a
            INNER JOIN revelio071625.tmp_global_rcids r
            ON a.rcid = r.rcid
        """)

####################### main function #####################

def create_tmp_keywords(job_title_list):
    run_query_df("DROP TABLE IF EXISTS tmp_keywords")
    run_query_df("""
        CREATE TABLE tmp_keywords
        (
            keyword String
        ) ENGINE = Memory
    """)

    values = ",".join(f"('{kw}')" for kw in job_title_list)
    run_query_df(f"INSERT INTO tmp_keywords VALUES {values}")

# for long words such as "carbon emission", 
# we do concat('%', k.keyword, '%')
def query_year_long_words(year):
    query = f"""
    SELECT
        a.user_id,
        a.position_id,
        EXTRACT(YEAR FROM parseDateTimeBestEffortOrNull(startdate)) AS start_year,
        EXTRACT(YEAR FROM parseDateTimeBestEffortOrNull(enddate)) AS end_year,
        startdate,
        enddate,
        a.weight,
        a.salary,
        a.rcid,
        k.keyword AS keyword,
        a.title_raw,
        a.role_k50_v3,
        a.role_k150_v3,
        a.role_k500_v3,
        a.role_k1000_v3,
        a.role_k1500_v3,
        a.role_k5000_v3,
        a.role_k10000_v3,
        a.role_k15000_v3,
        a.onet_title,
        CASE WHEN a.description != '' THEN 1 ELSE 0 END AS description
    FROM temp_processed_global_position a
    CROSS JOIN tmp_keywords k
    WHERE toYear(parseDateTimeBestEffortOrNull(a.startdate)) = {year}
        AND (
            a.title_raw LIKE concat('%', k.keyword, '%')
            OR a.role_k50_v3 LIKE concat('%', k.keyword, '%')
            OR a.role_k150_v3 LIKE concat('%', k.keyword, '%')
            OR a.role_k500_v3 LIKE concat('%', k.keyword, '%')
            OR a.role_k1000_v3 LIKE concat('%', k.keyword, '%')
            OR a.role_k1500_v3 LIKE concat('%', k.keyword, '%')
            OR a.role_k5000_v3 LIKE concat('%', k.keyword, '%')
            OR a.role_k10000_v3 LIKE concat('%', k.keyword, '%')
            OR a.role_k15000_v3 LIKE concat('%', k.keyword, '%')
            OR a.onet_title LIKE concat('%', k.keyword, '%')
            OR a.description LIKE concat('%', k.keyword, '%')
        )
    """
    return run_query_df(query)

# for short words like "gramm" or "nist",
# we do match(a.title_raw, concat('(^|[^a-zA-Z])', k.keyword, '([^a-zA-Z]|$)'))
def query_year_short_words(year):
    query = f"""
    SELECT
        a.user_id,
        a.position_id,
        EXTRACT(YEAR FROM parseDateTimeBestEffortOrNull(startdate)) AS start_year,
        EXTRACT(YEAR FROM parseDateTimeBestEffortOrNull(enddate)) AS end_year,
        startdate,
        enddate,
        a.weight,
        a.salary,
        a.rcid,
        k.keyword AS keyword,
        a.title_raw,
        a.role_k50_v3,
        a.role_k150_v3,
        a.role_k500_v3,
        a.role_k1000_v3,
        a.role_k1500_v3,
        a.role_k5000_v3,
        a.role_k10000_v3,
        a.role_k15000_v3,
        a.onet_title,
        CASE WHEN a.description != '' THEN 1 ELSE 0 END AS description
    FROM temp_processed_global_position a
    CROSS JOIN tmp_keywords k
    WHERE toYear(parseDateTimeBestEffortOrNull(a.startdate)) = {year}
        AND (
            match(a.title_raw, concat('(^|[^a-zA-Z])', k.keyword, '([^a-zA-Z]|$)'))
            OR match(a.role_k50_v3, concat('(^|[^a-zA-Z])', k.keyword, '([^a-zA-Z]|$)'))
            OR match(a.role_k150_v3, concat('(^|[^a-zA-Z])', k.keyword, '([^a-zA-Z]|$)'))
            OR match(a.role_k500_v3, concat('(^|[^a-zA-Z])', k.keyword, '([^a-zA-Z]|$)'))
            OR match(a.role_k1000_v3, concat('(^|[^a-zA-Z])', k.keyword, '([^a-zA-Z]|$)'))
            OR match(a.role_k1500_v3, concat('(^|[^a-zA-Z])', k.keyword, '([^a-zA-Z]|$)'))
            OR match(a.role_k5000_v3, concat('(^|[^a-zA-Z])', k.keyword, '([^a-zA-Z]|$)'))
            OR match(a.role_k10000_v3, concat('(^|[^a-zA-Z])', k.keyword, '([^a-zA-Z]|$)'))
            OR match(a.role_k15000_v3, concat('(^|[^a-zA-Z])', k.keyword, '([^a-zA-Z]|$)'))
            OR match(a.onet_title, concat('(^|[^a-zA-Z])', k.keyword, '([^a-zA-Z]|$)'))
            OR match(a.description, concat('(^|[^a-zA-Z])', k.keyword, '([^a-zA-Z]|$)'))
        )
    """
    return run_query_df(query)

def main_long_words(job_title_list, category):
    """
    Compute the occurrence of specific job titles in the Revelio Labs database
    """
    YEARS = list(range(2007, 2026))
    create_tmp_keywords(job_title_list)
    for year in YEARS:
        print("processing:", year)
        df = query_year_long_words(year)
        df.to_csv(
            f"D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/position_table_simple/rl_sasb_raw_{category}_{year}.csv",
            index=False
        )

def main_short_words(job_title_list, category):
    """
    Compute the occurrence of specific job titles in the Revelio Labs database
    """
    YEARS = list(range(2007, 2026))
    create_tmp_keywords(job_title_list)
    for year in YEARS:
        print("processing:", year)
        df = query_year_short_words(year)
        df.to_csv(
            f"D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/position_table_regex/rl_sasb_raw_{category}_{year}.csv",
            index=False
        )




####################  load Revelio Labs linking table #################### 
def load_linking_table():
    #df_linking = pd.read_csv(f"{PROJECT_DROPBOX_PATH}/links/RL_linking_202508_full.csv")
    df_linking = pd.read_parquet(f"{PROJECT_DROPBOX_PATH}/links/RL_linking_202508_global_public.parquet")
    return df_linking

# load the linking table
# df_linking = load_linking_table_wfdyn()
df_linking = load_linking_table()
print(df_linking.info())

RCIDs = df_linking['rcid'].tolist()
# drop duplicates
RCIDs = list(set(RCIDs))
print(len(RCIDs))

### do some cleaning
# df_linking[df_linking["gvkey"]==0].head(1)
# # fill 0 gvkey with null
# print(df_linking['gvkey'].dtype) # Int32
# df_linking['gvkey'] = df_linking['gvkey'].replace(0, pd.NA).astype('Int32')
# # rename isin
# df_linking = df_linking.rename(columns={'isnin': 'isin'})
# # save the cleaned version
# df_linking.to_parquet(f"{PROJECT_DROPBOX_PATH}/links/RL_linking_202508_global_public.parquet")


# df_linking = load_linking_table()

create_temp_rcid_table(RCIDs, batch_size=100_000)

create_temp_table() # ONLY NEED TO RUN ONCE

#################### create the keywords dictonary ####################

df = pd.read_csv(f"D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/generated_keywords_o3.csv")

category_dict = {}
for _, row in df.iterrows():
    key = row["Category"].strip().replace(" ", "_")
    keywords = []
    if pd.notna(row["Keywords"]):
        keywords += row["Keywords"].split(",")
    if pd.notna(row["Generated_Keywords"]):
        keywords += row["Generated_Keywords"].split(",")
    # clean keywords: replace "_" with " ", strip whitespace, drop empties
    keywords = [
        kw.strip().replace("_", " ")
        for kw in keywords
        if kw.strip()
    ]
    keywords = list(set(keywords)) # deuplication
    category_dict[key] = keywords


### split the dictionary because some short words are risky
category_dict_regex = {'GHG_Emissions': ['egrid', 'n20', 'ghg', 'co2', 'co2e', 'nf3', 'ch4', 'nox', 'cf6', 'ghgs', 'n2o'],
                       'Air_Quality': ['pm25', 'c29', 'aqi', 'voc', 'cfc', 'vocs', 'so2', 'sox', 'smog', 'nox', 'hap', 'pm10'], 
                       'Energy_Management': ['kwh', 'mwh', 'joule', 'pue', 'gwh'], 
                       'Water_&_Wastewater_Management': ['potw'], 
                       'Waste_&_Hazardous_Materials_Management': ['galsf'], 
                       'Ecological_Impacts': ['eia'], 
                       'Human_Rights_&_Community_Relations': ['ej', 'fpic'], 
                       'Customer_Privacy': ['gdpr', 'pii', 'ccpa', 'gramm'], 
                       'Data_Security': ['isms', 'nist', 'siem', 'cirp', 'c2m2', 'mfa'], 
                       'Access_&_Affordability': [], 
                       'Product_Quality_&_Safety': ['harpc', 'airp', 'haccp', 'is0', 'qms', 'gmp'], 
                       'Customer_Welfare': [], 
                       'Selling_Practices_&_Product_Labeling': [], 
                       'Labor_Practices': ['foa'], 
                       'Employee_Health_&_Safety': ['ltir', 'lwir', 'ppe', 'ohs', 'ldir', 'sms', 'osh', 'trir', 'hira'], 
                       'Employee_Engagement,_Diversity_&_Inclusion': ['ueg', 'tmn', 'erg', 'brg', 'dei', 'ergs', 'ebrg'], 
                       'Product_Design_&_Lifecycle_Management': ['epds', 'lca', 'dfr', 'pcfs', 'c2c', 'lcas', 'vert'], 
                       'Business_Model_Resilience': ['bcp', 'tcfd'], 
                       'Supply_Chain_Management': ['3tg'], 
                       'Materials_Sourcing_&_Efficiency': [], 
                       'Physical_Impacts_of_Climate_Change': ['storm'], 
                       'Business_Ethics': ['coi', 'ethic', 'bribe'], 
                       'Competitive_Behavior': [],
                       'Critical_Incident_Risk_Management': ['bcem', 'bcm', 'bcdr', 'bcms','drps'],
                       'Systemic_Risk_Management': ['basel', 'lcr', 'gsib', 'cet1'],
                       'Management_of_the_Legal_&_Regulatory_Environment': []}

# everything else is category_dict_simple
category_dict_simple = {}
for cat, all_kws in category_dict.items():
    regex_kws = set(category_dict_regex.get(cat, []))   # manual regex list (may be empty / missing)
    category_dict_simple[cat] = [kw for kw in all_kws if kw not in regex_kws]

# check
print(len(category_dict_simple))
print(len(category_dict_regex)) #25
print(len(category_dict))
#################### run the function ####################

for k, v in category_dict_simple.items():
    print("processing dictionary:", k)
    main_long_words(v, k)
    # main_count(RCIDs, job_title_list)


for k, v in category_dict_regex.items():
    print("processing dictionary:", k)
    main_short_words(v, k)
    # main_count(RCIDs, job_title_list)


# ################# remove the general words ###############

# in_dir = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/position_table")
# out_dir = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/sasb_position_cleaned")

# general_dict = {
#     "GHG_Emissions": ["intensity", "calculate", "scope 1", "scope 3"],
#     "Air_Quality": ["pm", "pollutant", "heavy metal", "heavy metals"],
#     "Energy_Management": ["saving", "efficiency", "management", "renewable"],
#     "Human_Rights_&_Community_Relations": ["stakeholder"],
#     "Data_Security": ["cyber", "vulnerability", "patching", "breach", "incident response"],
#     "Product_Quality_&_Safety":["qms"],
#     "Customer_Welfare": ["animal care"],
#     "Labor_Practices": ["wage", "overtime"],
#     "Employee_Health_&_Safety": ["safety", "ehs", "assessment", "inspection", "accidentincident", "rate", "injury", "recordable", "total recordable", "mask", "glove","face mask", "recordable incident", "hand sanitizer"],
#     "Employee_Engagement,_Diversity_&_Inclusion": ["di", "diversity", "turnover", "belonging"],
#     "Business_Model_Resilience": ["scenario"],
#     "Business_Ethics": ["dutie"]
# }


# for f in in_dir.glob("rl_sasb_raw_*.csv"):
#     # infer category from filename
#     category = "_".join(f.stem.split("_")[3:-1])  # after rl_sasb_raw_, before year
#     print(category)
#     bad_keywords = set(general_dict.get(category, []))

#     df = pd.read_csv(f)
#     print("total number of rows:", len(df))
#     df = df.drop_duplicates() # one keyword appear in multiple places
#     print("number of rows after deduplication:", len(df))

#     # drop rows triggered by general keywords
#     df_clean = df[~df["keyword"].isin(bad_keywords)]
#     print("number of rows after dropping bad words:", len(df_clean))

#     df_clean.to_csv(out_dir / f.name, index=False)


#################### found some neglected keywords, remove them ###################
simple_dir = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/position_table_regex")

for f in simple_dir.glob("rl_sasb_raw_Air_Quality_*.csv"):
    df = pd.read_csv(f)
    print(len(df))
    df = df[df["keyword"].str.lower() != "pm"]
    print(len(df))
    df.to_csv(f, index=False)

regex_dir = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/position_table_regex")
for f in regex_dir.glob("rl_sasb_raw_Employee_Engagement,_Diversity_&_Inclusion_*.csv"):
    df = pd.read_csv(f)
    print(len(df))
    df = df[df["keyword"].str.lower() != "di"]
    print(len(df))
    df.to_csv(f, index=False)


#################### firms that are in reveliolabs universe ###################

def query_rcid_universe():
    query = """
    SELECT DISTINCT r.*
    FROM revelio071625.tmp_global_rcids r
    INNER JOIN revelio071625.temp_processed_global_position p 
        ON r.rcid = p.rcid
    """
    return run_query_df(query)

rcid_universe = query_rcid_universe()

len(rcid_universe)
rcid_universe.head()
rcid_universe = pd.merge(rcid_universe, df_linking, on='rcid', how='left')
len(rcid_universe)
rcid_universe.head()
rcid_universe.to_csv("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/cleaned_data/reveliolab_universe_identifiers.csv", index=False)


#################### pull monthly total new jobs ######################

def query_all_new_jobs_monthly():
    query = """
    SELECT
        rcid,
        formatDateTime(parseDateTimeBestEffortOrNull(startdate), '%Y-%m') AS start_month,
        sum(weight) AS all_new_jobs_weighted
    FROM revelio071625.temp_processed_global_position
    WHERE parseDateTimeBestEffortOrNull(startdate) IS NOT NULL
    GROUP BY rcid, start_month
    """
    return run_query_df(query)

all_new_jobs_monthly = query_all_new_jobs_monthly()
all_new_jobs_monthly = all_new_jobs_monthly.sort_values(
    by=["rcid", "start_month"]
)
all_new_jobs_monthly.head()

all_new_jobs_monthly.to_csv("../all_new_jobs_monthly.csv", index = False)

# merge onto monthly data 
