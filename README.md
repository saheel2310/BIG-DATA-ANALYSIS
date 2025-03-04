# BIG-DATA-ANALYSIS
# Iowa Liquor Sales Analysis with PySpark

This project analyzes the Iowa Liquor Sales dataset using PySpark to demonstrate scalability and big data processing techniques.  It includes data cleaning, transformation, aggregation, visualization, and a scalability test using synthetic data.

## Overview

The script performs the following tasks:

*   **Data Loading:** Reads the Iowa Liquor Sales dataset from a CSV file into a Spark DataFrame.
*   **Data Cleaning and Transformation:** Renames columns for easier access, converts data types, handles missing values, and creates new columns for date and time analysis.
*   **Aggregation and Analysis:** Calculates average sale amounts, identifies top-selling categories, and analyzes monthly sales trends.
*   **Visualization:** Generates a plot of average monthly sales trends and saves it as `Average_Monthly_Sales.png`.
*   **Scalability Testing:** Generates synthetic data of varying sizes and measures the execution time of a simple analysis function to demonstrate Spark's scalability.  The results are plotted and saved as `scalability_test.png`.

## Dataset

The Iowa Liquor Sales dataset is a publicly available dataset from the Iowa Department of Commerce (or Kaggle).  It contains information about individual liquor sales in Iowa, including details such as date, store, category, vendor, item, quantity, and sale amount.

**Data Source:** [Iowa Alcoholic Beverage Sales](https://data.iowa.gov/Economy/Iowa-Alcoholic-Beverage-Sales/vr4w-2v6h) on data.iowa.gov.

**Note:** You will need to download the dataset and place it in the same directory as the script, or update the `data_path` variable accordingly.

**Attributes (from OCR, potentially requiring cleanup):**

*   Invoice/Ite Date
*   Store Num
*   Store Nam Address
*   City
*   Zip Code
*   Store Loca County Nu
*   County
*   Category
*   Category Vendor Nu
*   Vendor Na
*   Item Numl
*   Item Desci
*   Pack
*   Bottle Voli State Bott
*   State Bott Bottles So
*   Sale (Dolla
*   Volume So
*   Volume Sold (Gallons

## Requirements

*   Python 3.6+
*   PySpark
*   Pandas
*   Matplotlib
*   NumPy

Install the required libraries using pip:

```bash
pip install pyspark pandas matplotlib numpy
