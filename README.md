# workshop1-AI-Assisted-Data-Ingestion-with-dlt
From APIs to Warehouses: AI-Assisted Data Ingestion with dlt

## Homework 

### What is the start date and end date of the dataset?

    dlt pipeline taxi_pipeline show

sorting the drop off time to decided the range of the date 

### What proportion of trips are paid with credit card?

    select count (*)
    from "yellow_tripdata"
    where payment_type = 'Credit'

then divid 2666 to 10000

###  What is the total amount of money generated in tips? 
    select sum(tip_amt)
    from "yellow_tripdata"
