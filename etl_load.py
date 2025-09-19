# etl_load.py
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# --- CONFIG ---
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

USER = "root"
PASSWORD = "Shobamohan@1"
HOST = "127.0.0.1"    # ✅ no 1@, just this
PORT = 3306          # or 3306, depending on your docker-compose
DB = "retail_dw"

password_enc = quote_plus(PASSWORD)
conn_str = f"mysql+pymysql://{USER}:{password_enc}@{HOST}:{PORT}/{DB}?charset=utf8mb4"

print("DEBUG conn_str:", conn_str)  # will show exactly what sqlalchemy sees

engine = create_engine(conn_str, echo=False, pool_pre_ping=True)

# test
with engine.connect() as conn:
    print("Connected to DB:", conn.execute(text("SELECT DATABASE();")).scalar())


# --- LOAD CSVs ---
customers = pd.read_csv("data/customers.csv")
products = pd.read_csv("data/products.csv")
stores = pd.read_csv("data/stores.csv")
sales = pd.read_csv("data/sales.csv")


sales['DateValue'] = pd.to_datetime(sales['DateValue'], dayfirst=True).dt.strftime("%Y-%m-%d")



# 1) Load staging (replace or append as desired)
sales.to_sql("stgsales", con=engine, if_exists="append", index=False)

with engine.begin() as conn:
    # 2) Upsert dims: Customer
    for _, r in customers.iterrows():
        conn.execute(text("""
            INSERT INTO DimCustomer (CustomerID, CustomerName, Region)
            VALUES (:cid, :cname, :region)
            ON DUPLICATE KEY UPDATE CustomerName = VALUES(CustomerName), Region = VALUES(Region)
        """), {"cid": r.CustomerID, "cname": r.CustomerName, "region": r.Region})
    # Products
    for _, r in products.iterrows():
        conn.execute(text("""
            INSERT INTO DimProduct (ProductID, ProductName, Category, Price)
            VALUES (:pid, :pname, :cat, :price)
            ON DUPLICATE KEY UPDATE ProductName = VALUES(ProductName), Category = VALUES(Category), Price = VALUES(Price)
        """), {"pid": r.ProductID, "pname": r.ProductName, "cat": r.Category, "price": float(r.Price)})
    # Stores
    for _, r in stores.iterrows():
        conn.execute(text("""
            INSERT INTO DimStore (StoreID, StoreName, Location)
            VALUES (:sid, :sname, :loc)
            ON DUPLICATE KEY UPDATE StoreName = VALUES(StoreName), Location = VALUES(Location)
        """), {"sid": r.StoreID, "sname": r.StoreName, "loc": r.Location})

    # 3) Populate DimDate for all dates present in staging (idempotent)
    res = conn.execute(text("SELECT DISTINCT DateValue FROM StgSales"))
    dates = [row[0] for row in res.fetchall()]
    for d in dates:
        # parse date string to date
        dt = pd.to_datetime(d).date()
        date_key = int(dt.strftime("%Y%m%d"))
        conn.execute(text("""
            INSERT INTO DimDate (DateKey, DateValue, Year, Month, Day)
            VALUES (:dk, :dv, :yr, :mo, :da)
            ON DUPLICATE KEY UPDATE DateValue = VALUES(DateValue)
        """), {"dk": date_key, "dv": dt, "yr": dt.year, "mo": dt.month, "da": dt.day})

    # 4) Upsert FactSales from StgSales: aggregate per natural key and add to fact (incremental)
    # This query groups staging rows to one row per natural key, computes totals, then uses ON DUPLICATE KEY UPDATE to increment
    upsert_sql = """
    INSERT INTO FactSales (DateKey, CustomerKey, ProductKey, StoreKey, Quantity, TotalAmount)
    SELECT
        d.DateKey,
        c.CustomerKey,
        p.ProductKey,
        s.StoreKey,
        SUM(t.Quantity) AS Quantity,
        SUM(t.Quantity * p.Price) AS TotalAmount
    FROM StgSales t
    JOIN DimCustomer c ON t.CustomerID = c.CustomerID
    JOIN DimProduct p ON t.ProductID = p.ProductID
    JOIN DimStore s ON t.StoreID = s.StoreID
    JOIN DimDate d ON DATE(t.DateValue) = d.DateValue
    GROUP BY d.DateKey, c.CustomerKey, p.ProductKey, s.StoreKey
    ON DUPLICATE KEY UPDATE
        Quantity = Quantity + VALUES(Quantity),
        TotalAmount = TotalAmount + VALUES(TotalAmount);
    """
    conn.execute(text(upsert_sql))

    # 5) Optionally clear staging if you want (or keep for audit)
    # conn.execute(text("TRUNCATE TABLE StgSales"))

print("ETL complete.")
