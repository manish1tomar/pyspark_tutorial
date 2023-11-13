# Pyspark Basic Introduction

import pyspark
from pyspark.sql import SparkSession
import pandas as pd

if __name__ == '__main__':
    print('PySpark Tutorial')
    df = pd.read_csv('test1.csv')
    print(df)

    spark = SparkSession.builder.appName('Practice').getOrCreate()
    print(spark.version, spark.sparkContext)

    df_pyspark = spark.read.csv('test1.csv', header=True)
    print(df_pyspark)
    print("======================================================================")
    df_pyspark.show()
    print("======================================================================")
    df_pyspark = spark.read.option('header','true').csv('test1.csv')
    print(df_pyspark)
    print(type(df_pyspark))
    print(df_pyspark.head(), df_pyspark.printSchema())
    print("======================================================================")
