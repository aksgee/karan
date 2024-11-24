-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.cp('/FileStore/tables/customers_100.csv','/mnt/databases/landing/tables/customer/20241120/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp('/FileStore/tables/customers_1000.csv','/mnt/databases/landing/tables/customer/20241121/')

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS landing LOCATION "/mnt/databases/landing"

-- COMMAND ----------

-- when specifying default schema + additional columns 
CREATE TABLE IF NOT EXISTS landing.customer
(
`Index` BIGINT,
`Customer Id` STRING,
`First Name` STRING,
`Last Name` STRING,
Company STRING,
City STRING,
Country STRING,
`Phone 1` STRING,
`Phone 2` STRING,
Email STRING,
`Subscription Date` DATE,
Website STRING
)
USING CSV
LOCATION "/mnt/databases/landing/tables/customer/*/"
OPTIONS (
  sep = ',',
  header = True
)


-- COMMAND ----------

SELECT _metadata.file_modification_time, * FROM landing.customer

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver LOCATION '/mnt/databases/silver'

-- COMMAND ----------

DROP TABLE IF EXISTS silver.customer_scd1;
CREATE TABLE silver.customer_scd1 ( 
surrogate_key STRING,
CustomerId STRING,
FirstName STRING,
LastName STRING,
Company STRING,
City STRING,
Country STRING,
Phone STRING,
Email STRING,
SubscriptionDate DATE,
Website STRING,
record_hash STRING,
insert_timestamp TIMESTAMP,
update_timestamp TIMESTAMP,
active_flag BOOLEAN,
is_deleted BOOLEAN,
file_metadata STRING
)

-- COMMAND ----------


-- DEDUPLICATE
CREATE OR REPLACE TEMP VIEW default_timestamp AS 
SELECT current_timestamp() AS default_timestamp
;

CREATE OR REPLACE TEMP VIEW landing_customer AS 
SELECT 
MD5(CustomerId) AS surrogate_key,
CustomerId,
FirstName,
LastName,
Company,
City,
Country,
Phone,
Email,
SubscriptionDate,
Website,
MD5(
  CONCAT_WS('|!',
            FirstName,
            LastName,
            Company,
            City,
            Country,
            Phone,
            Email,
            SubscriptionDate,
            Website
            )
) AS record_hash,
(SELECT default_timestamp FROM default_timestamp) AS insert_timestamp,
(SELECT default_timestamp FROM default_timestamp) AS update_timestamp,
1 as active_flag,
0 as is_deleted,
file_metadata
FROM 
(
SELECT 
`Customer Id` AS CustomerId,
`First Name` AS FirstName,
`Last Name` AS LastName,
Company,
City,
Country AS Country,
`Phone 1` AS Phone,
Email AS Email,
`Subscription Date` AS SubscriptionDate,
Website,
_metadata AS file_metadata,
ROW_NUMBER() OVER(PARTITION BY `Customer Id` ORDER BY _metadata.file_modification_time) AS RN 
FROM landing.customer
)
WHERE RN = 1;

-- COMMAND ----------

-- UPSERT TO silver 
MERGE INTO silver.customer_scd1 tgt 
USING landing_customer src
ON src.surrogate_key = tgt.surrogate_key
WHEN MATCHED THEN UPDATE SET tgt.FirstName = src.FirstName, tgt.LastName = src.LastName, tgt.Company = src.Company, tgt.City = src.City, tgt.Country = src.Country, tgt.Phone = src.Phone, tgt.Email = src.Email, tgt.SubscriptionDate = src.SubscriptionDate, tgt.Website = src.Website, tgt.update_timestamp = src.update_timestamp, tgt.file_metadata = src.file_metadata
WHEN NOT MATCHED THEN INSERT *
