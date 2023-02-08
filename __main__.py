from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as F
import collections
import pymongo
import pandas as pd
import numpy as np
import psycopg2
from pymongo import MongoClient
from sqlalchemy import create_engine
import apache_beam as beam
import os
from google.cloud import storage
from apache_beam.options.pipeline_options import PipelineOptions

# Postgres CONNECTION in GCP

db_connection = "postgresql+psycopg2://gessicauser:y2d$6FsmT|ij/%Aj@35.222.243.30/postgresdb"

conn = create_engine(db_connection)

# SECURITY KEY SETUP

serviceAccount = 'cobalt-ripsaw-376516-237a71b98795.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = serviceAccount

# GETTING tables from database POSTGRES in GCP

#table_categories = pd.read_sql("""SELECT * FROM categories""", conn)
#print(table_categories)

# table_customer_customer_demo = pd.read_sql("""SELECT * FROM customer_customer_demo""", conn)

# table_customer_demographics = pd.read_sql("""SELECT * FROM customer_demographics""", conn)

# table_customers = pd.read_sql("""SELECT * FROM customers""", conn)

# table_employees = pd.read_sql("""SELECT * FROM employees""", conn)

# table_employee_territories = pd.read_sql("""SELECT * FROM employee_territories""", conn)

# table_orders = pd.read_sql("""SELECT * FROM orders""", conn)

# table_products = pd.read_sql("""SELECT * FROM products""", conn)

# table_region = pd.read_sql("""SELECT * FROM region""", conn)

# table_shippers = pd.read_sql("""SELECT * FROM shippers""", conn)

# table_suppliers = pd.read_sql("""SELECT * FROM suppliers""", conn)

# table_territories = pd.read_sql("""SELECT * FROM territories""", conn)

# table_us_states = pd.read_sql("""SELECT * FROM us_states""", conn)

# READING FILE CSV with Pandas

# df = pd.read_csv('order_details.csv')
# print(df)

# CREATING a pipeline for file order_details

# pipe1 = beam.Pipeline()

# order_details = (
#     pipe1
#     |'Leitura do dataset' >> beam.io.ReadFromText('order_details.csv', skip_header_lines=1)
#     |'Separar por vírgula' >> beam.Map(lambda record: record.split(','))
#     |'Exibir resultado' >> beam.Map(print)
# )
# pipe1.run()

# TURNING tables into csv file with Pandas

# table_categories.to_csv('categories.csv', index=False)

# table_customer_customer_demo.to_csv('customer_demo.csv', index=False)

# table_customer_demographics.to_csv('customer_demographics.csv', index=False)

# table_customers.to_csv('customers.csv', index=False)

# table_employees.to_csv('employees.csv', index=False)

# table_employee_territories.to_csv('employee_territories.csv', index=False)

# table_orders.to_csv('orders.csv', index=False)

# table_products.to_csv('products.csv', index=False)

# table_region.to_csv('region.csv', index=False)

# table_shippers.to_csv('shippers.csv', index=False)

# table_suppliers.to_csv('suppliers.csv', index=False)

# table_territories.to_csv('territories.csv', index=False)

# table_us_states.to_csv('us_states.csv', index=False)

# SENDING TABLES TO DATABASE MONGOdb

client = pymongo.MongoClient('mongodb+srv://gessica:XuAnJtTP6CLjbH9f@cluster0.wesgtc6.mongodb.net/?retryWrites=true&w=majority')

#df1 = pd.read_csv('categories.csv', delimiter=',')

#df2 = pd.read_csv('customer_demo.csv', delimiter=',')

#df3 = pd.read_csv('customer_demographics.csv', delimiter=',')

#df4 = pd.read_csv('customers.csv', delimiter=',')

#df5 = pd.read_csv('employees.csv', delimiter=',')

#df6 = pd.read_csv('employee_territories.csv', delimiter=',')

#df7 = pd.read_csv('orders.csv', delimiter=',')

#df8 = pd.read_csv('products.csv', delimiter=',')

#df9 = pd.read_csv('shippers.csv', delimiter=',')

#df10 = pd.read_csv('suppliers.csv', delimiter=',')

#df11 = pd.read_csv('territories.csv', delimiter=',')

#df12 = pd.read_csv('region.csv', delimiter=',')

#df13 = pd.read_csv('us_states.csv', delimiter=',')

#df14 = pd.read_csv('order_details.csv', delimiter=',')

# UPLOADING files to MONGOdb

# dicio = df14.to_dict(orient='records')

database = client.challenge_pipeline

# collection = database['table_orders_details']

# collection.insert_many(dicio)

# print("arquivo enviado!")

# PULLING data from MONGOdb

#Listing collections
# for collection in database.list_collection_names():
#     print(collection)

# Collection Orders - MongoDB
collection_orders = database['table_orders']

# cursor = collection_orders.find({})
# for orders in cursor:
#         print(orders)

# Collection Orders Details - MongoDB

# collection_orders_details = database['table_orders_details']

# cursor2 = collection_orders_details.find({})   
# for orders_details in cursor2:
#         print(orders_details)

# USING PYSPARK FOR DATA ANALYSIS

spark = ( SparkSession.builder
              .master("local")
              .appName("challenge-dataengineer")
              .config('spark.ui.port', '4050')
              .getOrCreate()
        )

orders = ( spark.read.format("csv")
                .option("header", "true")
                .option("inferschema", "true")
                .option("delimiter", ",")
                .load('orders.csv')
      )

#print(orders.show())

order_details = ( spark.read.format("csv")
                .option("header", "true")
                .option("inferschema", "true")
                .option("delimiter", ",")
                .load('order_details.csv')
      )

# USING JOIN TO CREATE A NEW DATAFRAME WITH THE ORDERS AND THEIR DETAILS IN THE SAME TABLE

orders.join(order_details,orders["order_id"] == order_details["order_id"]).show()


