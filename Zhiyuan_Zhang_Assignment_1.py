from __future__ import print_function

import os
import sys
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False


#Function - Cleaning
#For example, remove lines if they don’t have 16 values and
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles,
# fare amount and total amount are more than 0.1 dollars
def correctRows(p):
    if len(p) == 17:
        if isfloat(p[5]) and isfloat(p[11]):
            if float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0:
                return p


#Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output1> <output2>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Assignment-1")
    rdd = sc.textFile(sys.argv[1]).map(lambda line: line.split(','))
    clean_rdd = rdd.filter(correctRows)

    #Task 1

    taxi_drivers = clean_rdd.map(lambda p: (p[0], p[1])).distinct()
    taxi_driver_counts = taxi_drivers.map(lambda x: (x[0], 1)).reduceByKey(add)
    results_1 = taxi_driver_counts.takeOrdered(10, key=lambda x: -x[1])
    # Save output
    sc.parallelize(results_1).coalesce(1).saveAsTextFile(sys.argv[2])

    #Task 2

    # Total income and time of each driver
    driver_earnings = clean_rdd.map(lambda p: (p[1], (float(p[16]), float(p[4]))))

    # Average income per minute of each driver
    driver_avg_earnings = driver_earnings \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .mapValues(lambda x: x[0] / (x[1] / 60))  # Convert time to minute

    results2 = driver_avg_earnings.takeOrdered(10, key=lambda x: -x[1])
    sc.parallelize(results2).coalesce(1).saveAsTextFile(sys.argv[3])

    # Task 3 - Optional
    # Your code goes here

    # Task 4 - Optional
    # Your code goes here

    sc.stop()
