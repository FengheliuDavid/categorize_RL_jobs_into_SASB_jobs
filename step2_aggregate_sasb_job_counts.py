"""
obtain features from RL database
"""
import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from pandas.errors import EmptyDataError
# from dotenv import load_dotenv
# from utility.utils import timer
# load_dotenv()
# PROJECT_DROPBOX_PATH = os.getenv("PROJECT_DROPBOX_PATH")
PROJECT_DROPBOX_PATH = "D:/fenghe/dropbox/Dropbox/LMSW (Diversity Washing Through the Boardroom)/1. Data"
print(PROJECT_DROPBOX_PATH)

sasb_cols = [
    'Access_&_Affordability', 'Air_Quality',
    'Business_Ethics', 'Business_Model_Resilience', 'Competitive_Behavior',
    'Critical_Incident_Risk_Management', 'Customer_Privacy',
    'Customer_Welfare', 'Data_Security', 'Ecological_Impacts',
    'Employee_Engagement,_Diversity_&_Inclusion',
    'Employee_Health_&_Safety', 'Energy_Management', 'GHG_Emissions',
    'Human_Rights_&_Community_Relations', 'Labor_Practices',
    'Management_of_the_Legal_&_Regulatory_Environment',
    'Materials_Sourcing_&_Efficiency', 'Physical_Impacts_of_Climate_Change',
    'Product_Design_&_Lifecycle_Management', 'Product_Quality_&_Safety',
    'Selling_Practices_&_Product_Labeling', 'Supply_Chain_Management',
    'Systemic_Risk_Management', 'Waste_&_Hazardous_Materials_Management',
    'Water_&_Wastewater_Management'
]

####################################################################
################ aggregate to compute yearly occurence #############
####################################################################

simple_dir = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/position_table_simple")
regex_dir = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/position_table_regex")

# read in linking table
df_link = pd.read_parquet(f"{PROJECT_DROPBOX_PATH}/links/RL_linking_202508_global_public.parquet").drop_duplicates(subset=["rcid"])

parts = []

for f_simple in simple_dir.glob("rl_sasb_raw_*.csv"):
    year = int(f_simple.stem[-4:])
    category = f_simple.stem[len("rl_sasb_raw_"):-5]

    f_regex = regex_dir / f_simple.name  # same filename expected

    print("-------------")
    print("processing:", category, year)

    # read simple
    df_simple = pd.read_csv(f_simple)

    # read regex if exists, else empty df with same columns
    try:
        if f_regex.exists() & f_regex.stat().st_size != 0: # could be an empty file
            df_regex = pd.read_csv(f_regex)
        else:
            df_regex = df_simple.iloc[0:0].copy()
    except EmptyDataError:
        df_regex = df_simple.iloc[0:0].copy()

    # combine
    df = pd.concat([df_simple, df_regex], ignore_index=True)

    # dedup ignoring keyword (same position hit by multiple keywords)
    print("before dedup:", len(df))
    df = df.drop(columns=["keyword"], errors="ignore").drop_duplicates()
    print("after dedup:", len(df))

    # weighted count
    agg = df.groupby("rcid", as_index=False)["weight"].sum()
    agg["year"] = year
    agg["category"] = category
    agg = agg.rename(columns={"weight": "job_weighted_count"})

    parts.append(agg)

long_df = pd.concat(parts, ignore_index=True)
len(long_df['rcid'].unique()) # 98904

# wide pivot: one column per SASB category
wide = (
    long_df.pivot_table(index=["rcid", "year"],
                        columns="category",
                        values="job_weighted_count",
                        aggfunc="sum",
                        fill_value=0)
          .reset_index()
)

# flatten column names (pivot puts category names in a columns index)
wide.columns.name = None
wide.head()



# BALANCE PANEL: all rcid x years 2007-2025 
all_years = pd.DataFrame({"year": list(range(2007, 2026))})
rcids = pd.DataFrame({"rcid": wide["rcid"].unique()})
panel = rcids.merge(all_years, how="cross")  # pandas >= 1.2
len(panel)
len(rcids)

# merge wide onto full panel, fill missing category counts with 0
wide_balanced = panel.merge(wide, on=["rcid", "year"], how="left")
len(wide_balanced)

wide_balanced = wide_balanced.fillna(0)



# merge identifiers after aggregation
len(wide_balanced)
wide_balanced = wide_balanced.merge(df_link, on="rcid", how="left")
len(wide_balanced)
len(wide_balanced['rcid'].unique())

wide_balanced[sasb_cols] = wide_balanced[sasb_cols].fillna(0)
wide_balanced.tail(10)

# ---- write the wide CSV ----
wide_balanced.to_csv("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/cleaned_data/revelio_sasb_yearly_new_jobs.csv", index=False)




####################################################################
############### aggregate to compute monthly occurence #############
####################################################################

simple_dir = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/position_table_simple")
regex_dir = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/position_table_regex")

# read in linking table
df_link = pd.read_parquet(f"{PROJECT_DROPBOX_PATH}/links/RL_linking_202508_global_public.parquet").drop_duplicates(subset=["rcid"])

parts = []

for f_simple in simple_dir.glob("rl_sasb_raw_*.csv"):
    year = int(f_simple.stem[-4:])
    category = f_simple.stem[len("rl_sasb_raw_"):-5]

    f_regex = regex_dir / f_simple.name  # same filename expected

    print("-------------")
    print("processing:", category, year)

    # read simple
    df_simple = pd.read_csv(f_simple)

    # read regex if exists, else empty df with same columns
    try:
        if f_regex.exists() & f_regex.stat().st_size != 0: # could be an empty file
            df_regex = pd.read_csv(f_regex)
        else:
            df_regex = df_simple.iloc[0:0].copy()
    except EmptyDataError:
        df_regex = df_simple.iloc[0:0].copy()

    # combine
    df = pd.concat([df_simple, df_regex], ignore_index=True)

    # create a month column
    df["startdate"] = pd.to_datetime(df["startdate"], errors="coerce")
    len_before = len(df)
    print("before dropping empty month:", len_before)
    df = df.dropna(subset=["startdate"])  # drop rows where startdate couldn't be parsed
    len_after = len(df)
    print("after dropping empty month:", len_after)
    
    # Raise error if any rows were dropped
    if len_before != len_after:
        raise ValueError(f"MISMATCH FOUND! {len_before - len_after} rows with missing/invalid startdate in {category} {year}")
   
    df["start_month"] = df["startdate"].dt.to_period("M").astype(str)  # "2007-05"


    # dedup ignoring keyword (same position hit by multiple keywords)
    print("before dedup:", len(df))
    df = df.drop(columns=["keyword"], errors="ignore").drop_duplicates()
    print("after dedup:", len(df))

    # weighted count
    agg = df.groupby(["rcid", "start_month"], as_index=False)["weight"].sum()
    agg["year"] = year
    agg["category"] = category
    agg = agg.rename(columns={"weight": "job_weighted_count"})

    parts.append(agg)

long_df = pd.concat(parts, ignore_index=True)
long_df = long_df.drop(columns=["year"])
long_df.head()
len(long_df['rcid'].unique()) # 98904

# wide pivot: one column per SASB category
wide = (
    long_df.pivot_table(index=["rcid", "start_month"],
                        columns="category",
                        values="job_weighted_count",
                        aggfunc="sum",
                        fill_value=0)
          .reset_index()
)

# flatten column names (pivot puts category names in a columns index)
wide.columns.name = None
wide.head()



# BALANCE PANEL: all rcid x years 2007-2025 
all_months = pd.DataFrame({
    "start_month": pd.period_range("2007-01", "2025-12", freq="M").astype(str)
})
rcids = pd.DataFrame({"rcid": wide["rcid"].unique()})
panel = rcids.merge(all_months, how="cross")  # pandas >= 1.2
len(panel)
len(rcids) # 98904
# also check the unique number of rcids in linking table and in yearly data
len(df_link["rcid"].unique()) # 194716
yearly_data = pd.read_csv("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/cleaned_data/revelio_sasb_yearly_new_jobs.csv")
len(yearly_data["rcid"].unique()) # 98904
# yearly_data[yearly_data['isnin'].notna()]

# merge wide onto full panel, fill missing category counts with 0
wide_balanced = panel.merge(wide, on=["rcid", "start_month"], how="left")
len(wide_balanced)

wide_balanced = wide_balanced.fillna(0)



# merge identifiers after aggregation
len(wide_balanced)
wide_balanced = wide_balanced.merge(df_link, on="rcid", how="left")
len(wide_balanced)
len(wide_balanced['rcid'].unique())

wide_balanced[sasb_cols] = wide_balanced[sasb_cols].fillna(0)
wide_balanced.tail(10)

# ---- write the wide CSV ----
wide_balanced.to_csv("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/cleaned_data/revelio_sasb_monthly_new_jobs.csv", index=False)
wide_balanced.to_parquet("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/cleaned_data/revelio_sasb_monthly_new_jobs.parquet", index=False)


###############################################################################
############################# compute sasb shares ############################# 
###############################################################################

df_month = pd.read_parquet("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/cleaned_data/revelio_sasb_monthly_new_jobs.parquet")
print(len(df_month)) #22550112

all_new_jobs_monthly = pd.read_csv("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/all_new_jobs_monthly.csv")
all_new_jobs_monthly= all_new_jobs_monthly.rename(columns={"month": "start_month"})
len(all_new_jobs_monthly) #13616765
# len(all_new_jobs_monthly['rcid'].unique())
df_merged = df_month.merge(
    all_new_jobs_monthly,
    on=["rcid", "start_month"],
    how="right"
)
print(len(df_merged)) #13616765

# compute the share
for col in sasb_cols:
    df_merged[f"pct_{col}"] = np.where(
        df_merged["all_new_jobs_weighted"] > 0,
        df_merged[col] / df_merged["all_new_jobs_weighted"],
        np.nan
    )

# delete all sasb_columns
df_merged = df_merged.drop(columns=sasb_cols)
df_merged = df_merged.drop(columns=['company', 'primary_name', 'ticker',
       'exchange_name', 'sedol', 'isin', 'cusip', 'cik', 'gvkey', 'hq_country'])

# cases where some firm never hire any sasb but hire other jobs, so nan in df_merged
# therefore, merge in df_link again and then fillna pct_cols
df_merged_linked = df_merged.merge(
    df_link,
    on="rcid",
    how="left"
)
len(df_merged_linked)
# fillna the pct cols
# check
pct_cols = [
        'pct_Access_&_Affordability',
       'pct_Air_Quality', 'pct_Business_Ethics',
       'pct_Business_Model_Resilience', 'pct_Competitive_Behavior',
       'pct_Critical_Incident_Risk_Management', 'pct_Customer_Privacy',
       'pct_Customer_Welfare', 'pct_Data_Security', 'pct_Ecological_Impacts',
       'pct_Employee_Engagement,_Diversity_&_Inclusion',
       'pct_Employee_Health_&_Safety', 'pct_Energy_Management',
       'pct_GHG_Emissions', 'pct_Human_Rights_&_Community_Relations',
       'pct_Labor_Practices',
       'pct_Management_of_the_Legal_&_Regulatory_Environment',
       'pct_Materials_Sourcing_&_Efficiency',
       'pct_Physical_Impacts_of_Climate_Change',
       'pct_Product_Design_&_Lifecycle_Management',
       'pct_Product_Quality_&_Safety',
       'pct_Selling_Practices_&_Product_Labeling',
       'pct_Supply_Chain_Management', 'pct_Systemic_Risk_Management',
       'pct_Waste_&_Hazardous_Materials_Management',
       'pct_Water_&_Wastewater_Management'
]
df_merged_linked[pct_cols] = df_merged_linked[pct_cols].fillna(0)

### check
# rows where at least one pct column is non-zero and non-null
mask = (df_merged_linked[pct_cols].fillna(0) != 0).any(axis=1)

# number of unique rcids satisfying that condition
num_rcids = df_merged_linked.loc[mask, "rcid"].nunique()
num_rcids #98904

len(df_merged_linked)
df_merged_linked.columns

df_merged_linked.to_csv("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/cleaned_data/revelio_sasb_monthly_share_new_jobs.csv", index=False)
df_merged_linked.to_parquet("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/cleaned_data/revelio_sasb_monthly_share_new_jobs.parquet", index=False)





################### BELOW IS DATA QUALITY CHECK ###############

#######################################################
############### aggregate at year level ###############
#######################################################

year_level = (
    wide_balanced
        .groupby("year", as_index=False)
        .sum(numeric_only=True)
)
year_level.to_csv("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/year_level_new_job_count.csv", index=False)


#######################################################
################## at least one job  ##################
#######################################################

exclude_cols = [
    "rcid", "year",
    "company", "primary_name", "ticker", "exchange_name",
    "sedol", "isnin", "cusip", "cik", "gvkey", "hq_country"
]

# SASB category columns only
cat_cols = [c for c in wide_balanced.columns if c not in exclude_cols]

# for each rcid-year: does firm have any ESG job?
wide_balanced["has_esg_job"] = wide_balanced[cat_cols].sum(axis=1) > 0

# year-level count of firms with ESG jobs
esg_firms_by_year = (
    wide_balanced
        .groupby("year")["has_esg_job"]
        .sum()
        .reset_index(name="num_firms_with_esg_jobs")
)

esg_firms_by_year.to_csv("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/at_least_one_new_job.csv", index=False)



#######################################################
############ which keywords occur the most ############
#######################################################
kw_parts = []

for f_simple in simple_dir.glob("rl_sasb_raw_*.csv"):
    year = int(f_simple.stem[-4:])
    category = f_simple.stem[len("rl_sasb_raw_"):-5]

    f_regex = regex_dir / f_simple.name  # same filename expected

    print("-------------")
    print("processing:", category, year)

    # read simple
    df_simple = pd.read_csv(f_simple)

    # read regex if exists, else empty df with same columns
    try:
        if f_regex.exists() & f_regex.stat().st_size != 0: # could be an empty file
            df_regex = pd.read_csv(f_regex)
        else:
            df_regex = df_simple.iloc[0:0].copy()
    except EmptyDataError:
        df_regex = df_simple.iloc[0:0].copy()

    # combine
    df = pd.concat([df_simple, df_regex], ignore_index=True)
    df['category'] = category
    df['year'] = year

    kw_year = (
        df.groupby(["category", "year", "keyword"], as_index=False)
        .size()
        .rename(columns={"size": "keyword_count"})
    )
    kw_parts.append(kw_year)


kw_long2 = pd.concat(kw_parts, ignore_index=True)
kw_long2.head()
# average occurrence across years for each category-keyword
kw_avg = (
    kw_long2.groupby(["category", "keyword"], as_index=False)["keyword_count"]
          .mean()
          .rename(columns={"keyword_count": "avg_keyword_count"})
)


kw_avg.head()
kw_avg = kw_avg.sort_values(["category", "avg_keyword_count"], ascending=[True, False])

kw_avg.to_csv("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs/sasb_keywords_occurence_v2.csv", index=False)