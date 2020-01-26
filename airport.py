import pyspark
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)


df=spark.read.csv('s3a://finalgroup6/Raw_Data/airports.csv',inferSchema=True,header=True)

newRow = spark.createDataFrame([('ECP','Northwest Florida Beaches International Airport','Panama City','FL','USA',30.358250,-85.795610)])

newRow1 = spark.createDataFrame([('PBG','Plattsburgh International Airport','Plattsburgh','NY','USA',344.6597091,-73.46722069999998)])

newRow2 = spark.createDataFrame([('UST','Northeast Florida Regional Airport (St. Augustine Airport)','St. Augustine','FL','USA',29.954705,-81.343314)])

df=df.na.drop()

df = df.union(newRow)
df = df.union(newRow1)
df = df.union(newRow2)



df = df.withColumnRenamed("IATA_CODE","iata_code").withColumnRenamed("AIRPORT","airport").withColumnRenamed("CITY","city").withColumnRenamed("COUNTRY","country").withColumnRenamed("STATE","state").withColumnRenamed("LATITUDE","latitude").withColumnRenamed("LONGITUDE","longitude")

df.write.format("orc").save("s3a://finalgroup6/clean_data/Airports1/")


