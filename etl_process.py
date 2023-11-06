# # Install required packages
# import subprocess
# import sys

# subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

# Import relevant libraries
import os
import pymongo
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector as msql
import pandas as pd

load_dotenv()

mysql_password = os.getenv("MYSQL_PASSWORD")
mongodb_password = os.getenv("MONGODB_PASSWORD")


def get_data():
    # Extracting data from mysql database
    try:
        connection = msql.connect(host='localhost', database='diamond_data', user='root', password=mysql_password) 

        # creating a connection to mysql database
        if connection.is_connected():
            db_Info = connection.get_server_info() 
            print("Connected to MySQL Server version ", db_Info) # getting the server info
            cursor = connection.cursor() 
            cursor.execute("select database();") # selecting the database diamond
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            mycursor = connection.cursor()

        # executing the query to fetch all record from diamond record
            mycursor.execute("SELECT * FROM diamonds") 
            table_rows = mycursor.fetchall()
            
    except Exception as error:
        print("Error while connecting to MySQL", error)
        
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print('mysql connection is closed')

    # Creating dataframe
    df = pd.DataFrame(table_rows, columns=["carat", "cut", "color","clarity","depth", "table", "price", "x", "y", "z"])

    return df


def clean_data(df):
    df.dropna(inplace=True)

    return df


def append_time_column(df):
    # create new column to keep track of inserted data
    df['inserted_at'] = datetime.now()

    return df


def etl_pipeline(df):
    # Making connection to Mongocloud
    cluster = pymongo.MongoClient(f"mongodb://Stanislaus:{mongodb_password}@ac-kddib4z-shard-00-00.wfg4du8.mongodb.net:27017,ac-kddib4z-shard-00-01.wfg4du8.mongodb.net:27017,ac-kddib4z-shard-00-02.wfg4du8.mongodb.net:27017/?ssl=true&replicaSet=atlas-dr51du-shard-0&authSource=admin&retryWrites=true&w=majority")

    # creating collection testdb 
    db = cluster["diamond_db"]
    collection = db["diamonds"]

    # Inserting values to table test 
    x = collection.insert_many(df.to_dict('records')) # my result comes from mysql cursor
    print(len(x.inserted_ids)) 

    return len(x.inserted_ids)


step1 = get_data()
step2 = clean_data(step1)
step3 = append_time_column(step2)
step4 = etl_pipeline(step3)
