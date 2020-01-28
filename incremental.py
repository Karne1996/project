import pyspark
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext

sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)



df= sqlContext.read.format('jdbc').options(
          url='jdbc:mysql://database-1.cio4qal4q6yu.us-east-1.rds.amazonaws.com/project?autoReconnect=true&useSSL=false',
          driver='com.mysql.jdbc.Driver',
          query = "select * from test where substr(FL_DATE,1,4)='2018'",
          user='admin',
          password='password').load()    


#converting FL_DATE to unixtime
from pyspark.sql import functions as f
df=df.withColumn("FL_DATE", f.from_unixtime(f.unix_timestamp(df.FL_DATE), "yyyy-MM-dd"))

from pyspark.sql.functions import *
df = df.withColumn('CANCELLATION_CODE', regexp_replace('CANCELLATION_CODE', 'A', 'Airline/Carrier'))
df = df.withColumn('CANCELLATION_CODE', regexp_replace('CANCELLATION_CODE', 'B', 'Weather'))
df = df.withColumn('CANCELLATION_CODE', regexp_replace('CANCELLATION_CODE', 'C', 'National Air System(NAS)'))
df = df.withColumn('CANCELLATION_CODE', regexp_replace('CANCELLATION_CODE', 'D', 'Security'))


#spliting FL_DATE to get year,month day columns
import pyspark.sql.functions as f
df_split = f.split(df['FL_DATE'], '-')

df = df.withColumn('year', df_split.getItem(0))
df = df.withColumn('month', df_split.getItem(1))
df = df.withColumn('day', df_split.getItem(2))

#filling NA values
df1=df.na.fill({'DEP_TIME': 0,'DEP_DELAY': 0,'TAXI_OUT': 0,'WHEELS_OFF': 0,'WHEELS_ON': 0,'CANCELLATION_CODE': 'F','TAXI_IN': 0,'ARR_TIME': 0,'ARR_DELAY': 0,'ACTUAL_ELAPSED_TIME': 0,'AIR_TIME': 0,'CARRIER_DELAY': 0, 'WEATHER_DELAY': 0,'NAS_DELAY': 0,'SECURITY_DELAY': 0,'LATE_AIRCRAFT_DELAY': 0})

#adding status and delay column to dataframe
from pyspark.sql.functions import col, when
df1=df1.withColumn('delay', when(df1.ARR_DELAY > 0, df1.ARR_DELAY).otherwise(0))
df1=df1.withColumn('status', when(df1.ARR_DELAY > 0, 1).otherwise(0))


#converting datatype of columns
from pyspark.sql.types import IntegerType,DoubleType,DateType

df1 = df1.withColumn("FL_DATE", df1["FL_DATE"].cast(DateType()))
df1 = df1.withColumn("year", df1["year"].cast(IntegerType()))
df1 = df1.withColumn("month", df1["month"].cast(IntegerType()))
df1 = df1.withColumn("day", df1["day"].cast(IntegerType()))

#changinge columns name
df2 = df1.withColumnRenamed("FL_DATE","fl_date").withColumnRenamed("OP_CARRIER","op_carrier").withColumnRenamed("OP_CARRIER_FL_NUM","op_carrier_fl_num").withColumnRenamed("OP_CARRIER_FL_NUM","op_carrier_fl_num").withColumnRenamed("ORIGIN","origin").withColumnRenamed("DEST","dest").withColumnRenamed("CRS_DEP_TIME","crs_dep_time").withColumnRenamed("DEP_TIME","dep_time").withColumnRenamed("DEP_DELAY","dep_delay").withColumnRenamed("TAXI_OUT","taxi_out").withColumnRenamed("WHEELS_OFF","wheels_off").withColumnRenamed("WHEELS_ON","wheels_on").withColumnRenamed("TAXI_IN","taxi_in").withColumnRenamed("CRS_ARR_TIME","crs_arr_time").withColumnRenamed("ARR_TIME","arr_time").withColumnRenamed("ARR_DELAY","arr_delay").withColumnRenamed("CANCELLED","cancelled").withColumnRenamed("CANCELLATION_CODE","cancellation_code").withColumnRenamed("DIVERTED","diverted").withColumnRenamed("CRS_ELAPSED_TIME","crs_elapsed_time").withColumnRenamed("ACTUAL_ELAPSED_TIME","actual_elapsed_time").withColumnRenamed("AIR_TIME","air_time").withColumnRenamed("DISTANCE","distance").withColumnRenamed("CARRIER_DELAY","carrier_delay").withColumnRenamed("WEATHER_DELAY","whather_delay").withColumnRenamed("NAS_DELAY","nas_delay").withColumnRenamed("SECURITY_DELAY","security_delay").withColumnRenamed("LATE_AIRCRAFT_DELAY","late_aircraft_delay")

#writing clean file to s3 bucket
df2.write.format("orc").mode("append").save("s3a://finalgroup6/clean_data/final26/")




