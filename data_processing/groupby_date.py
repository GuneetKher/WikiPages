import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types

# defining schema
data_schema = types.StructType([
    types.StructField('Date', types.StringType()),
    types.StructField('Name', types.StringType()),
    types.StructField('Visits', types.IntegerType())
])


def main(input,output):
    # Reading the data and adding filepath column
    pages = spark.read.csv(inputs,schema=data_schema)
    
    kv_pages = pages.withColumn("Key",pages['Date']+pages['Name'])
    pages_with_counts = kv_pages.groupBy('Key').agg(functions.first('Date').alias('Date'),functions.first('Name').alias('Name'),functions.sum('Visits').alias("Total_Visits"))
    pages_with_counts = pages_with_counts.select("Date","Name","Total_Visits")
    pages_with_counts.write.mode('overwrite').csv(output,header=True,compression="gzip")
    
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wikicounts').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,output)
