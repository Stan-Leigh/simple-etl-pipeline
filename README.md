# # Creating a simple ETL data pipeline using Python script from source (MYSQL) to sink (MongoDB)
We will try to create a ETL pipeline using easy python script and take the data from mysql, do some operations on it and then push the data to mongodb. Let’s look at different steps involved in it.

### Step 1. Extracting the data from data source MYSQL.
![Alt text](https://github.com/Stan-Leigh/simple-etl-pipeline/blob/main/Images/MySQL%20Data.png)

### Step 2. Transform the data using Python Pandas
![Alt text](https://github.com/Stan-Leigh/simple-etl-pipeline/blob/main/Images/Pandas%20Data.png)

### Step 3. Load the data to sink MongoDB
![Alt text](https://github.com/Stan-Leigh/simple-etl-pipeline/blob/main/Images/MongoDB%20Data.png)

## CRON SCHEDULE
*/15 * * * * job.sh
