from pyspark.sql import SparkSession

object = SparkSession.Builder().master("local").appName("ObjectPySparkApp").getOrCreate()

print(object)