from PySpark_Object import SparkObject

def DFCreation():
    sparkObject = SparkObject()
    df = sparkObject.read.csv(path="/home/anandngr04/123Dir/Source/olap/USA_Medicare_Data_2021.csv", header=True)
    return df

