#!/usr/bin/env python
# coding:=utf-8

import sys
import readline

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date, timedelta, datetime
import time

# Getting Spark context
spark = SparkSession.builder.appName("SparkREPL").getOrCreate()

# Reading CSV into the context
# ============================
# df = spark.read.csv("_data_/t1.csv")
# df.printSchema()

# Registering a table and running SQL on it
# =========================================
# df.registerTempTable("df")
# spark.sql("select * from df").show(3)

# # Path to data set
# csv_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
# # Read and create a temporary view
# # Infer schema (note that for larger files you 
# # may want to specify the schema)
# df = (spark.read.format("csv")
#   .option("inferSchema", "true")
#   .option("header", "true")
#   .load(csv_file))
# df.createOrReplaceTempView("us_delay_flights_tbl")

# https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/ch04.html

last_exception = None
sql_multiline_mode = False
prompt = ">>> "
multiline_command = ""

while True:
    cmd = input(prompt);

    if cmd.startswith(".") and not sql_multiline_mode:
        if cmd == ".r":
            with open("_queries_/t.sql") as f:
                query = f.read()
                try:
                    res_df = spark.sql(query)
                    res_df.registerTempTable("res")
                    res_df.show()
                    continue
                except:
                    print("Error:", sys.exc_info()[0])
                    print(sys.exc_info()[1])
                    last_exception = sys.exc_info()

        if cmd == ".s":
            print(last_exception)
            continue

        if cmd == ".q":
            break

    if ";" in cmd:
        try:
            res_df = spark.sql(multiline_command + cmd)
            res_df.registerTempTable("res")
            res_df.show()
        except:
            print("Error:", sys.exc_info()[0])
            print(sys.exc_info()[1])
            last_exception = sys.exc_info()
        finally:
            # return back to normal prompt
            prompt = ">>> "
            sql_multiline_mode = False
            multiline_command = ""
    else:
        prompt = "... "
        sql_multiline_mode = True
        multiline_command += " " + cmd + " "
