import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types

# defining schema
data_schema = types.StructType([
    types.StructField('Project', types.StringType()),
    types.StructField('Name', types.StringType()),
    types.StructField('Visits', types.IntegerType()),
    types.StructField('Bytes', types.IntegerType())
])

@functions.udf(returnType=types.StringType())
def path_to_date(path):
    filename = path.split("/")[-1]
    filename = filename.split('.')[0]   # Incase there is an extention
    _,date,time = filename.split("-")
    return date

def main(input,output):
    # Reading the data and adding filepath column
    pages = spark.read.csv(inputs,sep=" ",schema=data_schema).withColumn('filename',functions.input_file_name())
    pages = pages.repartition(24)
    # Filtering operations
    en_pages = pages.filter(pages['Project']=="en")
    useful_pages = en_pages.filter(en_pages['Name']!="Main_Page")
    non_special = useful_pages.filter(useful_pages['Name'].startswith("Special:")==False)
    withDate = non_special.withColumn("Date",path_to_date(non_special['filename']))
    withDate_cleaned = withDate.select("Date","Name","Visits")
    
    withDate_cleaned.write.mode('overwrite').csv(output,header=True,compression="gzip")
    
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wikicounts').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,output)