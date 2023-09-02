from pyspark.sql.session import *
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import current_date

# Creating SparkSession and SparkContext
spark = (SparkSession.builder
         .appName("Usecase-5")
         .enableHiveSupport()
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Default configuration for Hadoop/GCS connectivity

conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# Custom schema creation, reading data from GCS bucket using Pyspark and creating a dataframe

custs_schema = StructType([StructField("custid",IntegerType(),True),
                          StructField("firstname",StringType(),True),
                          StructField("lastname",StringType(),True),
                          StructField("age",IntegerType(),True),
                          StructField("profession",StringType(),True)])

custs_gcs = spark.read.csv("gs://inceptez-usecase4/custs",sep=",",schema=custs_schema)
custs_gcs.cache()

#Transformations:

coladded_custs_gcs = custs_gcs.withColumn("loaddt",current_date()) # Added date column for load date
coladded_custs_gcs.write.saveAsTable("default.custdata",mode="append") # Storing it in Hive for Data Analyst team for further analysis
coladded_custs_gcs.write.json("gs://inceptez-usecase4/custdata") # Storing the data in JSON format for datscience team

# Stopping the program
spark.stop()