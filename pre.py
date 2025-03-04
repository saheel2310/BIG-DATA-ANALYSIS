#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
Big Data Analysis with PySpark - Iowa Liquor Sales Data Example

This script analyzes the Iowa Liquor Sales dataset using PySpark to demonstrate
scalability. It performs data cleaning, transformation, aggregation, and
visualization, and includes a scalability test using synthetic data.

Dataset: Iowa Liquor Sales
Source: Publicly available dataset (e.g., from the Iowa Department of Commerce
        or Kaggle).  You'll need to find and download the data.
        A good start: https://data.iowa.gov/Economy/Iowa-Alcoholic-Beverage-Sales/vr4w-2v6h

Attributes (from OCR):
    Invoice/Ite Date
    Store Num
    Store Nam Address
    City
    Zip Code
    Store Loca County Nu
    County
    Category
    Category Vendor Nu
    Vendor Na
    Item Numl
    Item Desci
    Pack
    Bottle Voli State Bott
    State Bott Bottles So
    Sale (Dolla
    Volume So
    Volume Sold (Gallons

Instructions:
1.  Download the Iowa Liquor Sales dataset (e.g., as a CSV file).
2.  Place the data file in the same directory as this script, or update
    the `data_path` variable accordingly.  Make sure to download a large amount of files.
3.  Install the required libraries: `pip install pyspark pandas matplotlib numpy`
4.  Run the script: `python your_script_name.py`
"""

import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    mean,
    hour,
    count,
    to_timestamp,
    unix_timestamp,
    col,
    year,
    month,
    dayofmonth,
    concat_ws
)

# --- Helper Functions ---

def generate_synthetic_data(num_rows):
    """Generates a synthetic dataset with random data."""
    categories = ["Vodka", "Whiskey", "Rum", "Gin", "Tequila", "Liqueur"]
    vendors = ["Diageo", "Beam Suntory", "Pernod Ricard", "Brown-Forman"]
    data = []
    for _ in range(num_rows):
        data.append({
            "Invoice/Ite Date": pd.to_datetime('2023-01-01') + pd.Timedelta(days=np.random.randint(0, 365)),
            "Store Num": np.random.randint(2000, 6000),
            "Store Nam Address": "Synthetic Store Address",
            "City": "Synthetic City",
            "Zip Code": np.random.randint(50000, 60000),
            "Store Loca County Nu": np.random.randint(1, 100),
            "County": "Synthetic County",
            "Category": np.random.choice(categories),
            "Category Vendor Nu": np.random.randint(100, 500),
            "Vendor Na": np.random.choice(vendors),
            "Item Numl": np.random.randint(10000, 20000),
            "Item Desci": "Synthetic Item Description",
            "Pack": np.random.randint(6, 24),
            "Bottle Voli State Bott": np.random.randint(750, 1750),
            "State Bott Bottles So": np.random.randint(1, 12),
            "Sale (Dolla": np.random.uniform(10, 100),
            "Volume So": np.random.uniform(0.5, 5),
            "Volume Sold (Gallons": np.random.uniform(0.1, 1)
        })
    return data


def analyze_data(df):
    """Performs a simple analysis on the DataFrame."""
    # Example:  Calculate average sale amount
    avg_sale = df.select(mean("Sale (Dolla")).first()[0]
    return avg_sale


def perform_liquor_sales_analysis(spark, data_path):
    """Performs the Iowa Liquor Sales data analysis."""

    print("Loading data...")
    df = spark.read.csv(data_path, header=True, inferSchema=True)

    print("Data schema:")
    df.printSchema()

    # --- Data Cleaning and Transformation ---
    print("Cleaning and transforming data...")

    # Correctly handle column names with spaces
    df = df.withColumnRenamed("Invoice/Ite Date", "Invoice_Ite_Date")
    df = df.withColumnRenamed("Store Num", "Store_Num")
    df = df.withColumnRenamed("Store Nam Address", "Store_Nam_Address")
    df = df.withColumnRenamed("Zip Code", "Zip_Code")
    df = df.withColumnRenamed("Store Loca County Nu", "Store_Loca_County_Nu")
    df = df.withColumnRenamed("Category Vendor Nu", "Category_Vendor_Nu")
    df = df.withColumnRenamed("Vendor Na", "Vendor_Na")
    df = df.withColumnRenamed("Item Numl", "Item_Numl")
    df = df.withColumnRenamed("Item Desci", "Item_Desci")
    df = df.withColumnRenamed("Bottle Voli State Bott", "Bottle_Voli_State_Bott")
    df = df.withColumnRenamed("State Bott Bottles So", "State_Bott_Bottles_So")
    df = df.withColumnRenamed("Sale (Dolla", "Sale_Dolla")
    df = df.withColumnRenamed("Volume So", "Volume_So")
    df = df.withColumnRenamed("Volume Sold (Gallons", "Volume_Sold_Gallons")

    # Convert date column
    df = df.withColumn("Invoice_Date", to_timestamp("Invoice_Ite_Date"))

    # Drop rows with missing values in critical columns (example)
    df = df.dropna(subset=["Invoice_Date", "Store_Num", "Category"])

    # --- Aggregation and Analysis ---

    print("Performing analysis...")
    average_sale_amount = df.select(mean("Sale_Dolla")).first()[0]
    print(f"Average Sale Amount: {average_sale_amount:.2f}")

    top_categories = df.groupBy("Category").agg(count("*").alias("count"))
    top_categories = top_categories.orderBy(col("count").desc()).limit(10)
    print("Top 10 Categories:")
    top_categories.show()

    df = df.repartition(12)  # Adjust based on your cluster/cores
    df.cache()

    # Additional example: Monthly sales trends
    monthly_sales = df.groupBy(year("Invoice_Date").alias("Year"), month("Invoice_Date").alias("Month")) \
                      .agg(mean("Sale_Dolla").alias("Average_Sale")) \
                      .orderBy("Year", "Month")
    print("Average Monthly Sales:")
    monthly_sales.show()

    #Visualization
    print("Generating visualization...")
    pandas_df = monthly_sales.toPandas()
    pandas_df['Date'] = pandas_df['Year'].astype(str) + '-' + pandas_df['Month'].astype(str)
    plt.figure(figsize=(12, 6))
    plt.plot(pandas_df['Date'], pandas_df['Average_Sale'], marker='o')
    plt.xlabel("Month-Year")
    plt.ylabel("Average Sale (Dollars)")
    plt.title("Average Monthly Sales of Liquor")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.grid(True)
    plt.savefig("Average_Monthly_Sales.png")

def perform_scalability_test(spark):
    """Performs a scalability test using synthetic data."""

    print("\nStarting scalability test...")
    data_sizes = [100000, 500000, 1000000, 2000000]  # Number of rows
    execution_times = []

    for num_rows in data_sizes:
        print(f"Generating data with {num_rows} rows...")
        synthetic_data = generate_synthetic_data(num_rows)
        df_pandas = pd.DataFrame(synthetic_data)  # Convert to Pandas for Spark
        df = spark.createDataFrame(df_pandas)

        start_time = time.time()
        analyze_data(df)  # Or a more complex analysis
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)
        print(f"Execution time for {num_rows} rows: {execution_time:.2f} seconds")

    # Plot the results
    print("Plotting scalability results...")
    plt.figure()  # Create a new figure for this plot
    plt.plot(data_sizes, execution_times)
    plt.xlabel("Number of Rows")
    plt.ylabel("Execution Time (seconds)")
    plt.title("Scalability Test - Synthetic Data")
    plt.grid(True)
    plt.savefig("scalability_test.png")
    print("Scalability plot saved to scalability_test.png")


def main():
    """Main function to orchestrate the analysis."""
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Iowa Liquor Sales Analysis") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    # Path to the Iowa Liquor Sales data file (Update this!)
    data_path = "Iowa_Liquor_Sales.csv"  # Replace with your actual path

    # Perform Iowa Liquor Sales data analysis
    try:
        perform_liquor_sales_analysis(spark, data_path)
    except Exception as e:
        print(f"Error during Iowa Liquor Sales analysis: {e}")

    # Perform the scalability test
    try:
        perform_scalability_test(spark)
    except Exception as e:
        print(f"Error during scalability test: {e}")

    # Stop the SparkSession
    spark.stop()
    print("SparkSession stopped.")


if __name__ == "__main__":
    main()
    print("Analysis complete.")
    














































