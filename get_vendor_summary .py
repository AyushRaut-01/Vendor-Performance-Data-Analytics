import pandas as pd
import sqlite3
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s- %(levelname)s- %(message)s",
    filemode="a"
)
def create_vendor_summary(conn):
    """
    Create a vendor sales summary combining purchases, sales, and freight data.
    """
    vendor_sales_summary = pd.read_sql_query("""
    -- Freight cost summary by Vendor
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    -- Purchase summary with product & pricing info
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price,
            pp.Volume
    ),
    -- Sales summary by Vendor & Brand
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    -- Final summary joining Purchases, Sales, and Freight
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC;
    """, conn)

    return vendor_sales_summary


   def clean_data(df):
    # convert Volume column to float
    df['Volume'] = df['Volume'].astype('float64')

    # fill missing values with 0
    df.fillna(0, inplace=True)

    # remove leading/trailing spaces from VendorName
    df['VendorName'] = df['VendorName'].str.strip()

    # calculate Gross Profit = Sales - Purchases
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']

    # calculate Profit Margin (%)
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100

    # calculate Stock Turnover = SalesQty / PurchaseQty
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']

    # calculate Sales to Purchase Ratio
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    return df


if __name__ == "__main__":
    # creating database connection
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating Vendor Summary Table.....')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning Data.....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data.....')
    ingest_db(clean_df, 'vendor_sales_summary', conn)

    logging.info('Completed')

