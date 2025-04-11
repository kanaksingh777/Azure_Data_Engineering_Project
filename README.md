# Azure-DE-Project
#Data source - "https://tableauserverguru.wordpress.com/sample-data-sets/"
#ENTIRE PROJECT END TO END
#Project-   https://medium.com/@kanaksingh2785/azure-end-to-end-data-engineering-project-259b0e0cc38d



![image](https://github.com/user-attachments/assets/ff3aa4a9-0b18-47b0-8bcd-a51d5be0e5a5)


In this project, I implemented an end-to-end data pipeline using Azure services and Databricks, following a Medallion Architecture. Data is ingested incrementally from an Azure SQL Server to Azure Data Lake Storage (Bronze Layer), transformed with PySpark in Databricks for analytics (Silver Layer), and optimized for business intelligence through dimensional modeling and Slowly Changing Dimensions (SCD) in the Gold Layer. The pipeline uses Delta Lake for efficient storage and querying, with key features such as incremental data loads, data governance via Unity Catalog, and Change Data Capture (CDC).

Project Overview
Data Ingestion (Bronze Layer):
Incremental data loads from an Azure SQL Server to Azure Data Lake Storage, utilizing Azure Data Factory (ADF)
Data Transformation (Silver Layer):
Raw data is transformed using the PySpark API, with schema inference from Parquet files. Key transformations are applied, and the processed data is stored in the Silver container. This layer also supports ad-hoc analysis and visualization in Databricks, enabling further insights and reporting.
Data Storage (Gold Layer):
Data is prepared for business intelligence and reporting through dimensional modeling and Slowly Changing Dimensions (SCD). Historical changes are captured, and data is optimized for analysis. Incremental and initial loads are handled using surrogate keys and Delta Lake’s merge() function. The final data is stored in Delta format for efficient querying and reporting.
Key Features:
Medallion Architecture (Bronze → Silver → Gold).
Incremental data ingestion using stored procedures.
Data governance with Unity Catalog for access control and lineage tracking.
Slowly changing dimensions handling & Change Data Capture (CDC)
Why ADLS GEN 2 ?


In ADLS, we can store data in the form of hierarchies. While creating the storage account, make sure to enable “Hierarchical Namespace” to create ADLS, else a blob storage will be created by default.

About the Dataset
For this project, I used the Car Sales dataset from this source. The Car Sales dataset contains transactional data related to vehicle sales, including details like Branch ID, Dealer ID, Model ID, Revenue, Units Sold, and Sale Date. It also includes metadata such as Branch Name, Dealer Name, and Product Name (car brand). This dataset is structured and well-suited for demonstrating data ingestion, transformation, and governance within an Azure data pipeline.

GitHub (Source) to Azure SQL Server
In this step, a source pipeline is created in which raw data is ingested from GitHub into Azure SQL Server using Azure Data Factory (ADF). Here’s how it works:

Copy Activity in ADF: The copy activity in ADF retrieves data from GitHub (HTTP source) and loads it into Azure SQL Server.
Parameterized File Name: A parameter is created in the copy activity to dynamically handle the file name coming from GitHub, this parameters gets dynamically added to the relative url. This allows for flexible and automated data loading each time a new file is available.
Linked Services: Linked services are used to establish connections with both GitHub and Azure SQL Server for seamless data transfer.



Bronze Layer
The Bronze Layer pipeline facilitates incremental data loads from SQL Server to Azure Data Lake using Azure Data Factory (ADF). Below is the detailed implementation:

Activities:
1️⃣ First Lookup Activity (Last Load Date)

Initially retrieves the earliest date from the dataset.
For subsequent runs, it fetches the last load date from the watermark table.
This ensures the pipeline only processes new data beyond the previous maximum date in the incremental runs.

2️⃣ Second Lookup Activity (Current Max Date)

Computes the maximum current date from the newly copied data.
Utilizes parameters in the query for enhanced flexibility.
This is the date that gets used in the Stored Procedure ahead.

3️⃣ Copy Activity

Implements incremental data loading using a date-based filter to fetch only new records.


4️⃣ Stored Procedure — Watermark Management

It is used to Update the watermark table to track the latest processed date.
Retrieves the current max date value and stores it for the next iteration.

Stored Procedure Definition:

Accepts a @last_load_value parameter to record the latest processed date. This @last_load_value is being updated through the lookup activity 2.
Updates the watermark table with the new load date.
Commits the transaction to persist changes.
Setting Up Unity Catalog in Azure Databricks
1. Create a Unity Metastore

To create Compute, we must attach the Databricks workspace to the Unity Catalog. But to be able to create a Unity Metastore, we need to do that from the admin console.

All you need to do is navigate to Azure > Microsoft Intra ID > users, copy the User principal name, and log in to the console https://accounts.azuredatabricks.net/ (by resetting the password).


Then all you need to do is assign the admin role to your email address which you used in your Azure account.


Then go back to the Databricks workspace & refresh the page and you should see the ‘Manage account’ button.

Notes to keep in mind:

It is only possible to create one Metastore per region.
Databricks creates default Metastores (to be deleted)
Now, in the Databricks admin console in the Catalog tab, click on Create metastore.


Add a name, select the region and provide the ADLS Gen 2 path (Azure Datalake Storage) following this convention:

<container_name>@<storage_account_name>.dfs.core.windows.net/<path>

Example: unity-metastore@datalakecarsale.dfs.core.windows.net/

This storage account will be used to store the default data e.g. metadata. To create this ADLS storage, navigate to the Azure portal > our project resource group > account storage > containers


About the Access Connector ID which is required to create the metastore, we need to create a Databricks Access Connector which will connect the Databricks workspace and the ADLS Gen 2 storage.


Then in the resource group, add the access connector for Databricks

Step 2: Create Compute

Step 3: Create External locations
At the current state, we have the raw data on Azure datalake in the bronze container, now, we need to create 3 External Locations (bronze, silver, and gold) because we need to read & write data between these containers, so we should have an external location for it.

To create an external location, we should have “storage credentials”.


After creating the credentials, click again on Catalog > External location And provide a URL following this structure: abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>


After creating the external location for the bronze layer, do the same work for the silver and gold layers.


Step 4: Create Catalog and Schema
Within the notebook, we create one catalog and two schemas.


To better understand the concept of Unity Catalog hierarchical architecture, check the following graph:


Source: https://docs.databricks.com/en/data-governance/unity-catalog/best-practices.html
SILVER LAYER - Data Transformation (one big table)
We will use PySpark API to read the data and one thing to note here is the ‘inferSchema’ option which helps to derive the schema from the raw data in parquet file format.


Then, after reading the dataset, we will do column transformation, to split the Model_ID and make the part before the ‘-’ as model_category.


Then we created an additional column to calculate the revenue per unit this can be useful for the analytics.



Adhoc Analysis — Visualization in Databricks



Then we write the transformed data to the silver storage container


GOLD LAYER — Dimension Model
The main goal of transitioning data from the silver to the gold layer is to prepare data for high-level business intelligence and reporting. This involves modeling the data e.g. following the start schema, to ensure it is ready for consumption by end-users, analysts, and decision-makers.

Silver layer: doesn’t maintain historical changes — it’s more about reflecting the current, cleaned state of incoming data.

Gold layer — Moving to the Gold layer with a focus on dimensional modeling and implementing SCD, the strategy needs to capture and store historical changes for analysis.

First we create a flag parameter which tells our notebook, if it is an initial run or an incremental run,


Creating Dimensional Model:
Source Data Extraction:
This first query extracts distinct car models and their categories from a Parquet file stored in Azure Blob Storage. The data is deduplicated using DISTINCT to ensure we have unique model entries.


The code then implements a common data warehousing pattern known as slowly changing dimensions (SCD):

Key Features:
Surrogate Key Creation: The code introduces a dim_model_key as a surrogate key for the dimension table.
Initial Schema Setup: For the first run (if not tableExists), it creates the schema without loading any data (using where 1=0).
Incremental Loading: For subsequent runs, it loads the actual data with the dimension key.
The code produces two DataFrames:

df_src: Contains Model_ID (string) and model_category (string)
df_sink: Contains dim_model_key (integer), Model_ID (string), and additional fields
Incremental Loading Implementation
The next step in the process shows how to handle incremental loads and identify new records:


Left Join Operation:

A LEFT JOIN is performed between df_src (new data) and df_sink (existing dimension table).
The join condition is based on the Model_ID field.
This operation helps differentiate between existing and new records.
Column Selection:

Model_ID and model_category are extracted from the source dataset (df_src).
dim_model_key is taken from the existing dimension table (df_sink).
Analysis of Results:

The df_filter.display() output reveals multiple car models (e.g., Lin-M28, Hon-M69, Toy-M202, etc.).
Their corresponding categories (e.g., Lin, Hon, Toy, etc.) are also visible.
If the dim_model_key column contains null values, it indicates new records that do not exist in the current dimension table.
Handling Existing vs. New Records:

Explanation:
This code separates records into two distinct groups:

df_filter_old: Contains records that already exist in the dimension table.
df_filter_new: Identifies new records that need to be added.
Surrogate Key Generation:

Key Aspects of Surrogate Key Generation:
For the initial load (incremental_flag == '0'), the key starts at 1.
For incremental loads, the highest existing key value is retrieved, monotonically_increasing_id() ensures each new record gets a unique key.
SCD Type 1 Implementation

Incremental Update Logic
When the dimension table already exists (cars_catalog.gold.dim_model):

Use Delta Lake’s merge() operation
Match records by dim_model_key
Update existing records
Insert new records not present in target table
Initial Load Logic
When the dimension table doesn’t exist:

Write initial data in Delta format
Create table in Azure Data Lake Storage
Use ‘overwrite’ mode for first-time data population
Final Result for Gold Layer:
The final result look like this in the dimension table:


Then to create the rest of the dimensions, we can simply clone the same notebook and just rename it with the new dimension name, and make the necessary changes, like the relative columns and table name.

Databricks Workflows (End-to-end Pipeline)
We can automate this whole pipeline with Azure Data Factory, but we will opt for using Databricks.

To do that, navigate to Workflows on Databricks workspace and click on ‘create job’ and then fill in the needed info as shown below attach the silver_notebook and the cluster, and finally click on create task.


Adding more tasks:


Now that we have all the tasks organized, click on ‘Run now’ to test the pipeline.


After running this ,we navigate to the data factory, choose the incremental pipeline, run it again, and verify the count of the rows to verify the results (via the query editor in databricks)

At this stage we finished the whole end to end pipeline using Azure and Databricks.

Thank you for reading :)

