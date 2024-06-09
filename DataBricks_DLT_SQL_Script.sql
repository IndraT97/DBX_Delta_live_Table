-- Databricks notebook source
--Ingesting data from landing layer to bronze layer

-- COMMAND ----------

--Ingesting data from landing layer dim_date csv file into bronze layer table dim_date_table

CREATE OR REFRESH STREAMING TABLE Dim_Date_Table_Bronze
LOCATION "gs://dlt-gcp-project-storage-account/Bronze/Dim_Date_Table"
AS SELECT *, current_timestamp() as Load_Time
FROM cloud_files("gs://dlt-gcp-project-storage-account/Landing/dim_date",
                "csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

--Ingesting data from landing layer dim_hotels csv file into bronze layer table dim_hotels_table

CREATE OR REFRESH STREAMING TABLE Dim_hotels_Table_Bronze
LOCATION "gs://dlt-gcp-project-storage-account/Bronze/Dim_hotels_Table"
AS SELECT *, current_timestamp() as Load_Time
FROM cloud_files("gs://dlt-gcp-project-storage-account/Landing/dim_hotels","csv",
                map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

--Ingesting data from landing layer dim_rooms csv file into bronze layer table dim_rooms_table

CREATE OR REFRESH STREAMING TABLE Dim_rooms_Table_Bronze
LOCATION "gs://dlt-gcp-project-storage-account/Bronze/Dim_rooms_Table"
AS SELECT *, current_timestamp() as Load_Time
FROM cloud_files("gs://dlt-gcp-project-storage-account/Landing/dim_rooms","csv",
                map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

--Ingesting data from landing layer fact_aggregated_bookings csv file into bronze layer table fact_aggregated_bookings_table

CREATE OR REFRESH STREAMING TABLE fact_aggregated_bookings_Table_Bronze
LOCATION "gs://dlt-gcp-project-storage-account/Bronze/fact_aggregated_bookings_Table"
AS SELECT *, current_timestamp() as Load_Time
FROM cloud_files("gs://dlt-gcp-project-storage-account/Landing/fact_aggregated_bookings","csv",
                map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

--Ingesting data from landing layer fact_bookings csv file into bronze layer table fact_bookings-table

CREATE OR REFRESH STREAMING TABLE fact_bookings_Table_Bronze
LOCATION "gs://dlt-gcp-project-storage-account/Bronze/fact_bookings_Table"
AS SELECT *, current_timestamp() as Load_Time
FROM cloud_files("gs://dlt-gcp-project-storage-account/Landing/fact_bookings","csv",
                map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

--ingesting data from bronze layer to Silver layer (Basic Data Checks an Transformation layer)

-- COMMAND ----------

-- Moving data from Bronze layer table Dim_Date_Table_Bronze into silver layer table Dim_Date_silver_Table

CREATE OR REFRESH STREAMING TABLE Dim_Date_silver_Table
(CONSTRAINT Key_exists EXPECT(key IS NOT NULL) ON VIOLATION DROP ROW)
LOCATION "gs://dlt-gcp-project-storage-account/Silver/Dim_Date_silver_Table"
AS SELECT to_date(date, 'dd-MMM-yy') AS Date,
    mmm_yy AS Month_year,
    week_no AS Week_number,
    day_type,
    key,
    Load_Time
FROM STREAM(LIVE.Dim_Date_Table_Bronze);

-- COMMAND ----------

--Moving data from Bronze layer table Dim_hotels_Table_Bronze into silver layer table Dim_hotels_silver_Table

CREATE OR REFRESH STREAMING TABLE Dim_hotels_silver_Table
(CONSTRAINT Key_exists EXPECT(key IS NOT NULL) ON VIOLATION DROP ROW)
LOCATION "gs://dlt-gcp-project-storage-account/Silver/Dim_hotels_silver_Table"
AS SELECT property_id,
property_name,
category,
city,
key,
Load_Time
FROM STREAM(LIVE.Dim_hotels_Table_Bronze)

-- COMMAND ----------

--Moving data from Bronze layer table Dim_rooms_Table_Bronze into silver layer table Dim_rooms_silver_Table

CREATE OR REFRESH STREAMING TABLE Dim_rooms_silver_Table
(CONSTRAINT Key_exists EXPECT(key IS NOT NULL) ON VIOLATION DROP ROW)
LOCATION "gs://dlt-gcp-project-storage-account/Silver/Dim_rooms_silver_Table"
AS SELECT room_id,
room_class,
key,
Load_Time
FROM STREAM(LIVE.Dim_rooms_Table_Bronze)

-- COMMAND ----------

--Moving data from Bronze layer table fact_aggregated_bookings_Table_Bronze into silver layer table fact_aggregated_bookings_silver_Table

CREATE OR REFRESH STREAMING TABLE fact_aggregated_bookings_silver_Table
(CONSTRAINT Key_exists EXPECT(key IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT property_id_exists EXPECT(property_id IS NOT NULL) ON VIOLATION DROP ROW)
LOCATION "gs://dlt-gcp-project-storage-account/Silver/fact_aggregated_bookings_silver_Table"
AS SELECT property_id,
to_date(check_in_date,"dd-MMM-yy") as check_in_date,
room_category,
successful_bookings,
capacity,
key,
Load_Time
FROM STREAM(LIVE.fact_aggregated_bookings_Table_Bronze)

-- COMMAND ----------

--Moving data from Bronze layer fact_bookings_Table_Bronze into silver layer table fact_bookings_silver_Table

CREATE OR REFRESH STREAMING TABLE fact_bookings_silver_Table
(CONSTRAINT Key_exists EXPECT(Key IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT property_id_exists EXPECT(property_id IS NOT NULL) ON VIOLATION DROP ROW)
LOCATION "gs://dlt-gcp-project-storage-account/Silver/fact_bookings_silver_Table"
AS SELECT booking_id,
property_id,
booking_date,
check_in_date,
checkout_date,
no_guests,
room_category,
booking_platform,
ratings_given,
booking_status,
revenue_generated,
revenue_realized,
key,
Load_Time
FROM STREAM(LIVE.fact_bookings_Table_Bronze)

-- COMMAND ----------

-- Moving data from intermediate silver layer tables to final silver layer tables 

-- COMMAND ----------

--Moving data from Intermediate silver layer table Dim_Date_silver_Table to final silver layer table Dim_Date_silver_final_Table

CREATE OR REFRESH STREAMING TABLE Dim_Date_silver_final_Table
LOCATION "gs://dlt-gcp-project-storage-account/Silver/Dim_Date_silver_final_Table";

APPLY CHANGES INTO LIVE.Dim_Date_silver_final_Table
FROM STREAM(LIVE.Dim_Date_silver_Table)
KEYS (key)
SEQUENCE BY Load_Time
STORED AS SCD TYPE 1;

-- COMMAND ----------

--Moving data from Intermediate silver layer table Dim_hotels_silver_Table to final silver layer table silver layer table Dim_hotels_silver_final_Table

CREATE OR REFRESH STREAMING TABLE Dim_hotels_silver_final_Table
LOCATION "gs://dlt-gcp-project-storage-account/Silver/Dim_hotels_silver_final_Table";

APPLY CHANGES INTO LIVE.Dim_hotels_silver_final_Table
FROM STREAM(LIVE.Dim_hotels_silver_Table)
KEYS (key)
SEQUENCE BY Load_Time
STORED AS SCD TYPE 1;

-- COMMAND ----------

--Moving data from Intermediate silver layer table Dim_rooms_silver_Table to final silver layer table silver layer table Dim_rooms_silver_final_Table

CREATE OR REFRESH STREAMING TABLE Dim_rooms_silver_final_Table
LOCATION "gs://dlt-gcp-project-storage-account/Silver/Dim_rooms_silver_final_Table";

APPLY CHANGES INTO LIVE.Dim_rooms_silver_final_Table
FROM STREAM(LIVE.Dim_rooms_silver_Table)
KEYS (key)
SEQUENCE BY Load_Time
STORED AS SCD TYPE 1;

-- COMMAND ----------

--Moving data from Intermediate silver layer table fact_aggregated_bookings_silver_Table to final silver layer table fact_aggregated_bookings_silver_final_Table

CREATE OR REFRESH STREAMING TABLE fact_aggregated_bookings_silver_final_Table
LOCATION "gs://dlt-gcp-project-storage-account/Silver/fact_aggregated_bookings_silver_final_Table";

APPLY CHANGES INTO LIVE.fact_aggregated_bookings_silver_final_Table
FROM STREAM(LIVE.fact_aggregated_bookings_silver_Table)
KEYS (key)
SEQUENCE BY Load_Time
STORED AS SCD TYPE 2;

-- COMMAND ----------

--Moving data from Intermediate silver layer table fact_bookings_silver_Table to final silver layer table fact_bookings_silver_final_Table

CREATE OR REFRESH STREAMING TABLE fact_bookings_silver_final_Table
LOCATION "gs://dlt-gcp-project-storage-account/Silver/fact_bookings_silver_final_Table";

APPLY CHANGES INTO LIVE.fact_bookings_silver_final_Table
FROM STREAM(LIVE.fact_bookings_silver_Table)
KEYS (key)
SEQUENCE BY Load_Time
STORED AS SCD TYPE 2;

-- COMMAND ----------

--Creating Materialized Views using final silver layer table


-- COMMAND ----------

--Creating Materialized Views Booking_Details in Gold layer (Table : Booking_Details_Gold)

CREATE LIVE TABLE Booking_Details_Gold
LOCATION "gs://dlt-gcp-project-storage-account/Gold/Booking_Details_Gold"
AS SELECT A.booking_id,
  A.property_id,
  A.booking_date,
  A.check_in_date,
  A.checkout_date,
  A.no_guests,
  A.room_category,
  A.booking_platform,
  A.ratings_given,
  A.booking_status,
  A.revenue_generated,
  A.revenue_realized,
  B.category AS Hotel_Category,
  B.city
FROM
  LIVE.fact_bookings_silver_final_Table AS A
LEFT JOIN 
  LIVE.Dim_hotels_silver_final_Table AS B
ON 
  A.property_id = B.property_id


-- COMMAND ----------

--Creating Materialized Views Aggregated Booking days in Gold layer (Table : Aggregated_Booking_Days_Gold)

CREATE LIVE TABLE Aggregated_Booking_Days_Gold
LOCATION "gs://dlt-gcp-project-storage-account/Gold/Aggregated_Booking_Days_Gold"
AS SELECT A.property_id,
  A.check_in_date,
  A.room_category,
  A.successful_bookings,
  A.capacity,
  B.Month_year,
  B.Week_number,
  B.day_type
FROM 
  LIVE.fact_aggregated_bookings_silver_final_Table AS A
LEFT JOIN 
  LIVE.Dim_Date_silver_final_Table AS B
ON  
  A.check_in_date = B.date

-- COMMAND ----------

--Creating Materialized Views Hotel Booking room detail in Gold layer (Table : Booking_Hotel_Rooms_Details_Gold)

CREATE LIVE TABLE Booking_Hotel_Rooms_Details_Gold
LOCATION "gs://dlt-gcp-project-storage-account/Gold/Booking_Hotel_Rooms_Details_Gold"
AS SELECT A.booking_id,
  A.property_id,
  B.property_name,
  B.category,
  B.city,
  C.room_class
FROM 
  LIVE.fact_bookings_silver_final_Table AS A
LEFT JOIN 
  LIVE.Dim_hotels_silver_final_Table AS B
ON 
  A.property_id = B.property_id
LEFT JOIN 
  LIVE.Dim_rooms_silver_final_Table AS C
ON 
  A.room_category = C.room_id
