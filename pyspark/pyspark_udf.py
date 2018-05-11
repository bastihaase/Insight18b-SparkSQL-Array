"""PySpark Array Intersection job
This module tests the performance of using UDfs to compute Array Intersections
with PySpark
"""


import json
from datetime import datetime
from StringIO import StringIO
from math import radians, sin, cos, sqrt, asin

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, DataFrameReader, Row
from pyspark.sql.types import (StructType, StructField, FloatType, DateType,
                               TimestampType, IntegerType, StringType)
from pyspark.sql.functions import udf



def main():

    # File with Amazon metadata
    raw = 'path_to_input_file'

    # Setup Spark session
    sc = SparkContext("spark://xxxxxxxxxxxxxxxx:7077", "PySpark-test")
    spark = SparkSession(sc)

    #Define user-defined-function for intersection
    def my_intersection(arr1, arr2):
        if arr1 == None or arr2 == None:
            return None
        return list(set(arr1) & set(arr2))

    spark.udf.register("MY_INTERSECTION", my_intersection)

    #load raw data
    metadata = spark.read.json(raw)

    metadata.createOrReplaceTempView("meta")

    # compute intersections
    df = spark.sql("SELECT MY_INTERSECTION(related.buy_after_viewing, related.also_viewed) FROM meta")
    df.rdd.count()


if __name__ == '__main__':
    main()
