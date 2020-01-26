import pyspark
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)


# Read csv from S3
df=spark.read.csv("s3a://finalgroup6/Raw_Data/RDS/2017.csv",inferSchema=True,header=True)
df=df.drop('Unnamed: 27')

df1=df.limit(500000)

# Write csv to RDS
df1.write.format('jdbc').options(
          url='jdbc:mysql://database-1.cio4qal4q6yu.us-east-1.rds.amazonaws.com/project',
          driver='com.mysql.jdbc.Driver',
          dbtable='test2',
          user='admin',
          password='password').mode('append').save()



