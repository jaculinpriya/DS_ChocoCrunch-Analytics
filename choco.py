import os
import time
import math
from urllib.parse import urlencode

import streamlit as st
import requests
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import plotly.express as px

st.set_page_config(page_title="ChocoCrunch Analytics", layout="wide")

# ----------------------------
# Backend DB config (hidden from UI)
# ----------------------------
# Recommended: set these as environment variables in your system or Streamlit secrets.
MYSQL_HOST = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com"
MYSQL_PORT = 4000
MYSQL_USER = "3ZYfgsUpxp9T6Ln.root"
MYSQL_PASSWORD = "zYhtCi4H0xxyq3F5"
MYSQL_DB = "chococrunch"

# ----------------------------
# ETL Configuration
# ----------------------------
API_BASE = "https://world.openfoodfacts.org/api/v2/search"
CATEGORY = "chocolates"
PAGE_SIZE = 100
MAX_RECORDS = 12000
MAX_PAGES = math.ceil(MAX_RECORDS / PAGE_SIZE)
CSV_CACHE = "choco_products_cache.csv"

# ----------------------------
# UI (main page only)
# ----------------------------
st.title("🍫✨ ChocoCrunch Analytics — Sweet Stats & Sour Truths")
st.markdown(
    "Extract → Clean → Feature-engineer → Store → Explore. "
    "Use the buttons below to fetch data, preview, and load into the database."
)

# Controls
col_a, col_b, col_c = st.columns([1,1,1])
with col_a:
    fetch_button = st.button("📥 Fetch & Preview (API)")
with col_b:
    load_button = st.button("🗄️ Create Tables & Load into DB")
with col_c:
    run_queries_button = st.button("▶️ Run Selected Queries")

use_cache = st.checkbox("Use cached CSV if present", value=True)
show_raw = st.checkbox("Show raw preview (first 200 rows)", value=False)

st.markdown("---")

# ----------------------------
# Helper functions: fetch & clean
# ----------------------------
def fetch_page(page:int, page_size:int=100, retries:int=3, sleep:float=0.5):
    params = {
        "categories": CATEGORY,
        "fields": "code,product_name,brands,nutriments",
        "page_size": page_size,
        "page": page
    }
    url = API_BASE + "?" + urlencode(params)
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            time.sleep(sleep * (attempt + 1))
    return None

def fetch_all_records(max_pages=MAX_PAGES, page_size=PAGE_SIZE, max_records=MAX_RECORDS):
    rows, total = [], 0
    with st.spinner("Fetching data from OpenFoodFacts (this may take a few minutes)..."):
        for page in range(1, max_pages + 1):
            data = fetch_page(page, page_size=page_size)
            if data is None:
                break
            products = data.get("products", [])
            if not products:
                break
            for p in products:
                rows.append({
                    "product_code": p.get("code"),
                    "product_name": p.get("product_name"),
                    "brand": p.get("brands"),
                    "nutriments": p.get("nutriments", {}) or {}
                })
                total += 1
                if total >= max_records:
                    break
            if total >= max_records:
                break
            time.sleep(0.15)
    return rows

def normalize_nutriments(rows):
    target_fields = [
        "energy-kcal_value","energy-kj_value","carbohydrates_value","sugars_value",
        "fat_value","saturated-fat_value","proteins_value","fiber_value",
        "salt_value","sodium_value","nova-group","nutrition-score-fr",
        "fruits-vegetables-nuts-estimate-from-ingredients_100g"
    ]
    recs = []
    for r in rows:
        nutr = r.get("nutriments", {}) or {}
        rec = {
            "product_code": r.get("product_code"),
            "product_name": r.get("product_name"),
            "brand": r.get("brand")
        }
        for f in target_fields:
            # try direct and variants
            val = nutr.get(f)
            if val is None:
                possible = [f, f.replace("_100g",""), f.replace("-value",""), f + "_100g", f.replace("-value","_100g")]
                for k in possible:
                    if k in nutr:
                        val = nutr.get(k)
                        break
            rec[f] = val
        recs.append(rec)
    return pd.DataFrame(recs)

def clean_and_engineer(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # basic cleaning
    df['brand'] = df['brand'].fillna("").astype(str)
    df['product_name'] = df['product_name'].fillna("").astype(str)
    df = df.fillna(0)
    df = df.replace("", 0)
    numeric_cols = [c for c in df.columns if c not in ['product_code','product_name','brand']]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    # derived columns
    df['sugar_to_carb_ratio'] = np.where(
        (df['carbohydrates_value'].notna()) & (df['carbohydrates_value'] != 0),
        df['sugars_value'] / df['carbohydrates_value'],
        np.nan
    )

    def calorie_cat(k):
        if pd.isna(k): return "Unknown"
        if k <= 250: return "Low"
        if k <= 400: return "Moderate"
        return "High"
    df['calorie_category'] = df['energy-kcal_value'].apply(calorie_cat)

    def sugar_cat(s):
        if pd.isna(s): return "Unknown"
        if s <= 10: return "Low Sugar"
        if s <= 30: return "Moderate Sugar"
        return "High Sugar"
    df['sugar_category'] = df['sugars_value'].apply(sugar_cat)

    df['is_ultra_processed'] = np.where(df['nova-group'] == 4, "Yes", "No")
    df['product_code'] = df['product_code'].astype(str)
    df = df.drop_duplicates(subset=['product_code'])
    return df

# ----------------------------
# DB helpers (credentials used from backend constants)
# ----------------------------
def get_mysql_connection(host, port, user, password, database=None, create_if_missing=True):
    port_int = int(port)
    try:
        conn = mysql.connector.connect(host=host, port=port_int, user=user, password=password, database=database)
        return conn
    except mysql.connector.Error as err:
        # If DB missing and allowed, create then reconnect
        if getattr(err, "errno", None) == errorcode.ER_BAD_DB_ERROR and create_if_missing and database:
            conn = mysql.connector.connect(host=host, port=port_int, user=user, password=password)
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
            cursor.close()
            conn.close()
            return mysql.connector.connect(host=host, port=port_int, user=user, password=password, database=database)
        else:
            raise

def create_tables(conn):
    cursor = conn.cursor()
    queries = [
        """
        CREATE TABLE IF NOT EXISTS product_info (
            product_code TEXT PRIMARY KEY,
            product_name TEXT,
            brand TEXT
        ) CHARACTER SET utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS nutrient_info (
            product_code TEXT,
            `energy-kcal_value` FLOAT,
            `energy-kj_value` FLOAT,
            `carbohydrates_value` FLOAT,
            `sugars_value` FLOAT,
            `fat_value` FLOAT,
            `saturated-fat_value` FLOAT,
            `proteins_value` FLOAT,
            `fiber_value` FLOAT,
            `salt_value` FLOAT,
            `sodium_value` FLOAT,
            `fruits-vegetables-nuts-estimate-from-ingredients_100g` FLOAT,
            `nutrition-score-fr` INT,
            `nova-group` INT,
            FOREIGN KEY (product_code) REFERENCES product_info(product_code) ON DELETE CASCADE
        ) CHARACTER SET utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS derived_metrics (
            product_code TEXT,
            sugar_to_carb_ratio FLOAT,
            calorie_category TEXT,
            sugar_category TEXT,
            is_ultra_processed TEXT,
            FOREIGN KEY (product_code) REFERENCES product_info(product_code) ON DELETE CASCADE
        ) CHARACTER SET utf8mb4;
        """
    ]
    for q in queries:
        cursor.execute(q)
    conn.commit()
    cursor.close()

def insert_dataframes(conn, df: pd.DataFrame):
    cursor = conn.cursor()
    # product_info
    p_rows = df[['product_code','product_name','brand']].values.tolist()
    cursor.executemany(
        "INSERT INTO product_info (product_code, product_name, brand) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE product_name=VALUES(product_name), brand=VALUES(brand);",
        p_rows
    )
    # nutrient_info
    nut_cols = [
        "energy-kcal_value","energy-kj_value","carbohydrates_value","sugars_value",
        "fat_value","saturated-fat_value","proteins_value","fiber_value",
        "salt_value","sodium_value","fruits-vegetables-nuts-estimate-from-ingredients_100g",
        "nutrition-score-fr","nova-group"
    ]
    n_rows = []
    for _, r in df.iterrows():
        row = [r.get('product_code')] + [None if pd.isna(r.get(c)) else float(r.get(c)) for c in nut_cols]
        n_rows.append(row)
    nut_placeholders = ", ".join(["%s"] * (1 + len(nut_cols)))
    nut_cols_sql = ", ".join([f"`{c}`" for c in nut_cols])
    cursor.executemany(
        f"INSERT INTO nutrient_info (product_code, {nut_cols_sql}) VALUES ({nut_placeholders}) ON DUPLICATE KEY UPDATE {', '.join([f'`{c}`=VALUES(`{c}`)' for c in nut_cols])};",
        n_rows
    )
    # derived_metrics
    d_rows = []
    for _, r in df.iterrows():
        d_rows.append((
            r.get('product_code'),
            None if pd.isna(r.get('sugar_to_carb_ratio')) else float(r.get('sugar_to_carb_ratio')),
            r.get('calorie_category'),
            r.get('sugar_category'),
            r.get('is_ultra_processed')
        ))
    cursor.executemany(
        "INSERT INTO derived_metrics (product_code, sugar_to_carb_ratio, calorie_category, sugar_category, is_ultra_processed) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE sugar_to_carb_ratio=VALUES(sugar_to_carb_ratio), calorie_category=VALUES(calorie_category), sugar_category=VALUES(sugar_category), is_ultra_processed=VALUES(is_ultra_processed);",
        d_rows
    )
    conn.commit()
    cursor.close()

# ----------------------------
# SQL Queries: 24 selectable queries
# ----------------------------
QUERIES = {
    # product_info (6)
    "1 - Count products per brand": "SELECT brand, COUNT(*) AS product_count FROM product_info GROUP BY brand ORDER BY product_count DESC;",
    "2 - Count unique products per brand": "SELECT brand, COUNT(DISTINCT product_code) AS unique_products FROM product_info GROUP BY brand ORDER BY unique_products DESC;",
    "3 - Top 5 brands by product count": "SELECT brand, COUNT(*) AS product_count FROM product_info GROUP BY brand ORDER BY product_count DESC LIMIT 5;",
    "4 - Products with missing product name": "SELECT product_code, brand FROM product_info WHERE product_name IS NULL OR TRIM(product_name) = '';",
    "5 - Number of unique brands": "SELECT COUNT(DISTINCT brand) AS unique_brands FROM product_info;",
    "6 - Products with code starting with '3'": "SELECT product_code, product_name FROM product_info WHERE product_code LIKE '3%';",

    # nutrient_info (7)
    "7 - Top 10 products with highest energy-kcal_value": "SELECT n.product_code, p.product_name, n.`energy-kcal_value` FROM nutrient_info n JOIN product_info p USING(product_code) WHERE n.`energy-kcal_value` IS NOT NULL ORDER BY n.`energy-kcal_value` DESC LIMIT 10;",
    "8 - Average sugars_value per nova-group": "SELECT `nova-group`, AVG(`sugars_value`) AS avg_sugars FROM nutrient_info WHERE `nova-group` IS NOT NULL GROUP BY `nova-group`;",
    "9 - Count products with fat_value > 20g": "SELECT COUNT(*) AS cnt FROM nutrient_info WHERE `fat_value` > 20;",
    "10 - Average carbohydrates_value per product": "SELECT AVG(`carbohydrates_value`) AS avg_carbs FROM nutrient_info;",
    "11 - Products with sodium_value > 1g": "SELECT n.product_code, p.product_name, `sodium_value` FROM nutrient_info n JOIN product_info p USING(product_code) WHERE `sodium_value` > 1;",
    "12 - Count products with non-zero fruits/vegetables/nuts content": "SELECT COUNT(*) AS cnt FROM nutrient_info WHERE `fruits-vegetables-nuts-estimate-from-ingredients_100g` IS NOT NULL AND `fruits-vegetables-nuts-estimate-from-ingredients_100g` > 0;",
    "13 - Products with energy-kcal_value > 500": "SELECT n.product_code, p.product_name, `energy-kcal_value` FROM nutrient_info n JOIN product_info p USING(product_code) WHERE `energy-kcal_value` > 500;",

    # derived_metrics (6)
    "14 - Count products per calorie_category": "SELECT calorie_category, COUNT(*) AS cnt FROM derived_metrics GROUP BY calorie_category;",
    "15 - Count of High Sugar products": "SELECT COUNT(*) AS cnt FROM derived_metrics WHERE sugar_category = 'High Sugar';",
    "16 - Products both High Calorie & High Sugar": "SELECT d.product_code, p.product_name FROM derived_metrics d JOIN nutrient_info n USING(product_code) JOIN product_info p USING(product_code) WHERE d.calorie_category='High' AND d.sugar_category='High Sugar';",
    "17 - Number of products marked as ultra-processed": "SELECT COUNT(*) AS cnt FROM derived_metrics WHERE is_ultra_processed = 'Yes';",
    "18 - Products with sugar_to_carb_ratio > 0.7": "SELECT d.product_code, p.product_name, d.sugar_to_carb_ratio FROM derived_metrics d JOIN product_info p USING(product_code) WHERE d.sugar_to_carb_ratio > 0.7;",
    "19 - Average sugar_to_carb_ratio per calorie_category": "SELECT calorie_category, AVG(sugar_to_carb_ratio) AS avg_ratio FROM derived_metrics GROUP BY calorie_category;",

    # join queries (5)
    "20 - Top 5 brands with most High Calorie products": "SELECT p.brand, COUNT(*) AS cnt FROM product_info p JOIN derived_metrics d USING(product_code) WHERE d.calorie_category = 'High' GROUP BY p.brand ORDER BY cnt DESC LIMIT 5;",
    "21 - Average energy-kcal_value for each calorie_category": "SELECT d.calorie_category, AVG(n.`energy-kcal_value`) AS avg_kcal FROM derived_metrics d JOIN nutrient_info n USING(product_code) GROUP BY d.calorie_category;",
    "22 - Count of ultra-processed products per brand": "SELECT p.brand, COUNT(*) AS cnt FROM product_info p JOIN derived_metrics d USING(product_code) WHERE d.is_ultra_processed = 'Yes' GROUP BY p.brand ORDER BY cnt DESC;",
    "23 - Products High Sugar & High Calorie along with brand": "SELECT p.brand, p.product_name, d.calorie_category, d.sugar_category FROM product_info p JOIN derived_metrics d USING(product_code) WHERE d.calorie_category='High' AND d.sugar_category='High Sugar';",
    "24 - Top 5 products by sugar_to_carb_ratio with categories": "SELECT d.product_code, p.product_name, d.sugar_to_carb_ratio, d.calorie_category, d.sugar_category FROM derived_metrics d JOIN product_info p USING(product_code) ORDER BY d.sugar_to_carb_ratio DESC LIMIT 5;"
}

# Show query list (collapsible) and allow selection
st.subheader("Available SQL Queries (select any to run)")
with st.expander("Click to view the 24 queries"):
    for k, q in QUERIES.items():
        st.code(f"{k}\n{q}", language='sql')

selected_queries = st.multiselect("Choose queries to run (you can pick many)", options=list(QUERIES.keys()), default=["1 - Count products per brand", "14 - Count products per calorie_category"])

# ----------------------------
# Actions: Fetch, Load, Run
# ----------------------------
# Load dataset (either cached or from API)
df = None
if use_cache and os.path.exists(CSV_CACHE):
    try:
        df = pd.read_csv(CSV_CACHE)
        st.info(f"Loaded cached dataset ({CSV_CACHE}) — {len(df)} rows.")
    except Exception:
        df = None

if fetch_button:
    rows = fetch_all_records(max_pages=MAX_PAGES, page_size=PAGE_SIZE, max_records=MAX_RECORDS)
    st.success(f"Fetched {len(rows)} raw records from OpenFoodFacts.")
    df = normalize_nutriments(rows)
    df = clean_and_engineer(df)
    df.to_csv(CSV_CACHE, index=False)
    st.success(f"Cleaned dataset saved to {CSV_CACHE} ({len(df)} rows).")

# Show preview + minimal EDA
if df is not None:
    st.markdown("### Dataset preview & EDA")
    st.write(f"Rows: {len(df)} — Columns: {df.shape[1]}")
    if show_raw:
        st.dataframe(df.head(200))
    # Quick distribution charts
    col1, col2 = st.columns(2)
    with col1:
        fig = px.histogram(df, x='energy-kcal_value', nbins=40, title='Energy (kcal) distribution', marginal='box')
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig2 = px.histogram(df, x='sugars_value', nbins=40, title='Sugars (g/100g) distribution', marginal='box')
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("Calories vs Sugar (scatter)")
    fig3 = px.scatter(df, x='sugars_value', y='energy-kcal_value', color='calorie_category', hover_data=['product_name'], height=450)
    st.plotly_chart(fig3, use_container_width=True)

    # CSV download
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    st.download_button("Download cleaned dataset (CSV)", csv_bytes, file_name="choco_cleaned.csv", mime="text/csv")

# Create tables and load into DB
if load_button:
    if df is None:
        st.error("No dataset to load — first fetch data.")
    else:
        try:
            conn = get_mysql_connection(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, create_if_missing=True)
            st.info("Connected to database. Creating tables...")
            create_tables(conn)
            st.info("Inserting data (this may take a few minutes)...")
            insert_dataframes(conn, df)
            conn.close()
            st.success("Data loaded into database successfully.")
        except Exception as e:
            st.error(f"Database load error: {e}")

# Run selected queries
if run_queries_button:
    if len(selected_queries) == 0:
        st.warning("No queries selected — pick at least one query.")
    else:
        try:
            # create SQLAlchemy engine (for pandas.read_sql)
            engine = create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}", pool_recycle=3600)
            st.info("Running selected queries...")
            results = {}
            for key in selected_queries:
                sql = QUERIES[key]
                try:
                    df_q = pd.read_sql(sql, engine)
                    results[key] = df_q
                except Exception as e_q:
                    results[key] = pd.DataFrame({"error":[str(e_q)]})
            engine.dispose()

            # display results in accordions
            for k, df_r in results.items():
                with st.expander(k):
                    if df_r.empty:
                        st.write("No results.")
                    else:
                        st.dataframe(df_r)
            st.success("Finished running selected queries.")
        except Exception as e:
            st.error(f"Error running queries: {e}")

st.markdown("---")
st.markdown("### Notes")
st.markdown(
    "- Database credentials are stored in the backend constants at the top of this file. Use environment variables for security.\n"
    "- The app fetches cleaned product data and loads into 3 tables: product_info, nutrient_info, derived_metrics.\n"
    "- The 24 SQL queries above are selectable — run the ones you need and inspect results.\n"
    "- To create a Power BI dashboard: connect Power BI to your MySQL/TiDB using the same DB details and import the 3 tables."
)
