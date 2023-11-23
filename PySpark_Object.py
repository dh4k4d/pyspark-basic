from pyspark.sql import SparkSession

def SparkObject() :
    object = SparkSession.Builder().master("local").appName("ObjectPySparkApp").getOrCreate()
    return object

if __name__ == '__main__':
    print(SparkObject())