import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)



try:        
    dyf = glueContext.create_dynamic_frame_from_options(connection_type="s3",connection_options={"paths": ["s3://gen-garage-poc/smart-home-data/HomeC.csv"]}, format="csv",format_options={"withHeader": True})       
    dyf.printSchema()
    dyf.show()
except Exception as e:
    print("An error occurred:", e)


df = dyf.toDF()
df.show(2)

from pyspark.sql.functions import col,lit,current_date
from datetime import datetime

etlbatch_id = datetime.now().strftime("%Y%m%d%H%M%S%f")[:17]
df = df.withColumn("ETL_batch_id",lit(etlbatch_id))
df = df.withColumn("ETL_batch_date", lit(current_date()))
df.show(2)


from pyspark.sql.functions import concat
import pyspark.sql.functions as f


df = df.withColumn("uuid", f.expr("uuid()"))

from pyspark.sql.functions import from_unixtime
#df = df.withColumn("time", from_unixtime(df["time"].cast("bigint"), 'yyyy-MM-dd HH:mm:ss'))
df = df.withColumn("time", from_unixtime(col("time")))
df.show()
'''time_counts = df.groupBy('time').count()
sorted_times = time_counts.orderBy(col('count').desc())
sorted_times.limit(10).show()'''
# from pyspark.sql.functions import from_unixtime
# df = df.withColumn("time", from_unixtime(df["time"].cast("bigint"), 'yyyy-MM-dd HH:mm:ss'))

'''df=df.withColumn("time",from_unixtime("time"))
df.select(df["time"]).show()'''


from pyspark.sql.types import TimestampType
from datetime import datetime, timedelta

from pyspark.sql import functions as F

df = df.withColumn("time", F.to_timestamp(F.col("time"), "yyyy-MM-dd HH:mm:ss"))

filtered_df = df.where(F.col("time").between(lit("2016-01-01 5:00:00"), lit("2016-01-01 6:00:00")))
filtered_df.count()
#filtered_df.count()


filtered_df = filtered_df.na.replace("", None)\
       .na.replace("NA",None)\
       .na.replace("Null",None)\
       .na.replace("None",None)
filtered_df.show(55)


df_no_duplicates = filtered_df.dropDuplicates()

df_no_duplicates.show(55) 




df_cleaned = df_no_duplicates.dropna(how='all',subset = None)

df_cleaned.show(5)
print(df_cleaned.count())


def cast_column_values(column_name, data_type):
    return col(column_name).cast(data_type)
 
column_data_types = {
    "use [kW]": "double",
    "gen [kW]": "double",
    "House overall [kW]": "double",
    "Dishwasher [kW]": "double",
    "Furnace 1 [kW]": "double",
    "Furnace 2 [kW]": "double",
    "Home office [kW]": "double",
    "Fridge [kW]": "double",
    "Wine cellar [kW]": "double",
    "Garage door [kW]": "double",
    "Kitchen 12 [kW]": "double",
    "Kitchen 14 [kW]": "double",
    "Kitchen 38 [kW]": "double",
    "Barn [kW]": "double",
    "Well [kW]": "double",
    "Microwave [kW]": "double",
    "Living room [kW]": "double",
    "Solar [kW]": "double",
    "temperature": "double",
    "icon": "string",
    "humidity": "double",
    "visibility": "double",
    "summary": "string",
    "apparentTemperature": "double",
    "pressure": "double",
    "windSpeed": "double",
    "cloudCover": "double",
    "windBearing": "double",
    "precipIntensity": "double",
    "dewPoint": "double",
    "precipProbability": "double"

}


for column, data_type in column_data_types.items():
    df_cleaned = df_cleaned.withColumn(column, cast_column_values(column, data_type))

df_cleaned.printSchema()


df_cleaned.show(1)

df_cleaned = df_cleaned.withColumn("Furnace Total [kW]",(col("Furnace 1 [kW]")+col("Furnace 2 [kW]")))
df_cleaned = df_cleaned.withColumn("Kitchen Total [kW]",(col("Kitchen 12 [kW]")+col("Kitchen 14 [kW]")+col("Kitchen 38 [kW]")))
df_cleaned = df_cleaned.withColumn("Kitchen Appliances",(col("Fridge [kW]")+col("Dishwasher [kW]")+col("Microwave [kW]")))
df_cleaned.show(1)


df_cleaned = df_cleaned.drop('Furnace 1 [kW]','Furnace 2 [kW]',
'Kitchen 12 [kW]','Kitchen 14 [kW]','Kitchen 38 [kW]','Fridge [kW]',
'Dishwasher [kW]','Microwave [kW]')

df_cleaned = df_cleaned.withColumn("House overall [kW]", (col("Kitchen Total [kW]")+ 
                                   col("Home office [kW]")+col("Garage door [kW]")+
                                   col("Wine cellar [kW]")+col("Barn [kW]")+
                                   col("Well [kW]")+col("Living room [kW]" )+
                                   col("Kitchen Appliances")+col("Furnace Total [kW]")))


from pyspark.sql.functions import *
import uuid
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


from pyspark.sql import functions as F, Window


window_spec = Window.partitionBy()


df_with_stats = df_cleaned.withColumn("max_power", F.max("House overall [kW]").over(window_spec)) \
                           .withColumn("min_power", F.min("House overall [kW]").over(window_spec)) \
                           .withColumn("avg_power", F.avg("House overall [kW]").over(window_spec))


df_with_stats.show()


from awsglue.dynamicframe import DynamicFrame


desired_columns = ["ETL_batch_id", "ETL_batch_date","uuid",
                   "time","use [kW]","gen [kW]","House overall [kW]",
                  "Home office [kW]","Wine cellar [kW]","Garage door [kW]",
                  "Barn [kW]","Well [kW]","Living room [kW]","Solar [kW]","temperature",
                  "icon" ,"humidity","visibility","summary","apparentTemperature","pressure",
                  "windSpeed","cloudCover","windBearing","precipIntensity","dewPoint",
                   "precipProbability","Furnace Total [kW]","Kitchen Total [kW]","Kitchen Appliances",
                   "max_power","min_power","avg_power"]


# Reorder columns
df_final = df_with_stats.select(desired_columns + [col for col in df_with_stats.columns if col not in desired_columns])

df_final.show(55)


dyf2 = DynamicFrame.fromDF(df_final, glueContext)


dyf2 = dyf2.coalesce(2)

df = df.repartition(1)
 
output_path = "s3://aws-dym-bucket/aws-db-smarthome/output"
df.write.csv(output_path, header=True)



try:
    '''data1 = glueContext.write_dynamic_frame_from_options(frame=dyf2,
                                                    connection_type="s3",
                                                    connection_options = {"path": "s3://gen-garage-poc/usac-home/"},
                                                    format='csv',
                                                    format_options={"quoteChar": -1,})'''
    df_final.write.option("header", "true").csv("s3://gen-garage-poc/usac-home")
except Exception as e:
    print("An error occured in loading into S3:", e)



# AWS Glue Studio Notebook
##### You are now running a AWS Glue Studio notebook; To start using your notebook you need to start an AWS Glue Interactive Session.

%idle_timeout 2880
%glue_version 4.0
%worker_type G.1X
%number_of_workers 5

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

try:        
    dyf = glueContext.create_dynamic_frame_from_options(connection_type="s3",connection_options={"paths": ["s3://gen-garage-poc/smart-home-data/HomeC.csv"]}, format="csv",format_options={"withHeader": True})       
    dyf.printSchema()
    dyf.show()

except Exception as e:
    print("An error occurred:", e)

df = dyf.toDF()
df.show(2)
from pyspark.sql.functions import col,lit,current_date
from datetime import datetime

etlbatch_id = datetime.now().strftime("%Y%m%d%H%M%S%f")[:17]
df = df.withColumn("ETL_batch_id",lit(etlbatch_id))
df = df.withColumn("ETL_batch_date", lit(current_date()))
df.show(2)
from pyspark.sql.functions import concat
import pyspark.sql.functions as f
df = df.withColumn("uuid", f.expr("uuid()"))
from pyspark.sql.functions import from_unixtime
#df = df.withColumn("time", from_unixtime(df["time"].cast("bigint"), 'yyyy-MM-dd HH:mm:ss'))
df = df.withColumn("time", from_unixtime(col("time")))
df.show()
'''time_counts = df.groupBy('time').count()
sorted_times = time_counts.orderBy(col('count').desc())
sorted_times.limit(10).show()'''
# from pyspark.sql.functions import from_unixtime
# df = df.withColumn("time", from_unixtime(df["time"].cast("bigint"), 'yyyy-MM-dd HH:mm:ss'))

'''df=df.withColumn("time",from_unixtime("time"))
df.select(df["time"]).show()'''
from pyspark.sql.types import TimestampType
from datetime import datetime, timedelta
from pyspark.sql import functions as F

df = df.withColumn("time", F.to_timestamp(F.col("time"), "yyyy-MM-dd HH:mm:ss"))

filtered_df = df.where(F.col("time").between(lit("2016-01-01 5:00:00"), lit("2016-01-01 6:00:00")))
filtered_df.count()
#filtered_df.count()
'''from pyspark.sql.functions import sum
firstday = df.filter(df["time"] == "2016-01-01")
sum1 = firstday.agg(sum("House overall [kW]")).collect()[0][0]
print(sum1)
secondday= df.filter(df["time"] == "2016-01-02")
sum2 =secondday.agg(sum("House overall [kW]")).collect()[0][0]
print(sum2)
difference = sum1-sum2
print(difference)'''
Removal of rows with all values of null , none, NA and empty spaces ; if only few convert to None
filtered_df = filtered_df.na.replace("", None)\
       .na.replace("NA",None)\
       .na.replace("Null",None)\
       .na.replace("None",None)
filtered_df.show(55)
Dropping Duplicate rows
df_no_duplicates = filtered_df.dropDuplicates()

df_no_duplicates.show(55) 
print(df_no_duplicates.count())

#print(df.count())

df_cleaned = df_no_duplicates.dropna(how='all',subset = None)

df_cleaned.show(5)
print(df_cleaned.count())
def cast_column_values(column_name, data_type):
    return col(column_name).cast(data_type)
 
column_data_types = {
    "use [kW]": "double",
    "gen [kW]": "double",
    "House overall [kW]": "double",
    "Dishwasher [kW]": "double",
    "Furnace 1 [kW]": "double",
    "Furnace 2 [kW]": "double",
    "Home office [kW]": "double",
    "Fridge [kW]": "double",
    "Wine cellar [kW]": "double",
    "Garage door [kW]": "double",
    "Kitchen 12 [kW]": "double",
    "Kitchen 14 [kW]": "double",
    "Kitchen 38 [kW]": "double",
    "Barn [kW]": "double",
    "Well [kW]": "double",
    "Microwave [kW]": "double",
    "Living room [kW]": "double",
    "Solar [kW]": "double",
    "temperature": "double",
    "icon": "string",
    "humidity": "double",
    "visibility": "double",
    "summary": "string",
    "apparentTemperature": "double",
    "pressure": "double",
    "windSpeed": "double",
    "cloudCover": "double",
    "windBearing": "double",
    "precipIntensity": "double",
    "dewPoint": "double",
    "precipProbability": "double"

}


for column, data_type in column_data_types.items():
    df_cleaned = df_cleaned.withColumn(column, cast_column_values(column, data_type))

df_cleaned.printSchema()


df_cleaned.show(1)

df_cleaned = df_cleaned.withColumn("Furnace Total [kW]",(col("Furnace 1 [kW]")+col("Furnace 2 [kW]")))
df_cleaned = df_cleaned.withColumn("Kitchen Total [kW]",(col("Kitchen 12 [kW]")+col("Kitchen 14 [kW]")+col("Kitchen 38 [kW]")))
df_cleaned = df_cleaned.withColumn("Kitchen Appliances",(col("Fridge [kW]")+col("Dishwasher [kW]")+col("Microwave [kW]")))
df_cleaned.show(1)
df_cleaned = df_cleaned.drop('Furnace 1 [kW]','Furnace 2 [kW]','Kitchen 12 [kW]','Kitchen 14 [kW]','Kitchen 38 [kW]','Fridge [kW]','Dishwasher [kW]','Microwave [kW]')
Replacing the values of field 'House Overall" by the actual sum from the power used in the house
df_cleaned = df_cleaned.withColumn("House overall [kW]", (col("Kitchen Total [kW]")+ 
                                   col("Home office [kW]")+col("Garage door [kW]")+
                                   col("Wine cellar [kW]")+col("Barn [kW]")+
                                   col("Well [kW]")+col("Living room [kW]" )+
                                   col("Kitchen Appliances")+col("Furnace Total [kW]")))
df_cleaned.printSchema()
'''from pyspark.sql.functions import col, max  
power_fields = ['Home office [kW]','Wine cellar [kW]',
 'Garage door [kW]','Barn [kW]',
 'Well [kW]', 'Living room [kW]' ,'Furnace Total [kW]',
 'Kitchen Total [kW]','Kitchen Appliances']
df_cleaned = df_cleaned.select(*power_fields).filter(col(power_fields[0]).isNotNull())
max_value = float('-inf')
max_field = None

for field in power_fields:
    if not df_cleaned.filter(col(field).isNull()).isEmpty():  
        current_max = df_cleaned.select(col(field)).rdd.first()[0]  
        if current_max > max_value:
            max_value = current_max
            max_field = field

print(f"Field with maximum power usage: {max_field}")
print(f"Maximum power usage value: {max_value}")'''
'''from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
day="2016-01-01"
day_df=df.filter(col("time")==day)
max_day=day_df.agg(max("House overall [kW]").alias("max_total_overall")).collect()[0]["max_total_overall"]
min_day=day_df.agg(min("House overall [kW]").alias("min_total_overall")).collect()[0]["min_total_overall"]
df=df.withColumn("max_total_overall_day1",lit(max_day))
df=df.withColumn("min_total_overall_day1",lit(min_day))
df.show()'''
from pyspark.sql.functions import *
import uuid
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Add max and min columns
'''max_day=df_cleaned.agg(max("House overall [kW]").alias("max_power")).collect()[0]#["max_total_overall"]
min_day=df_cleaned.agg(min("House overall [kW]").alias("min_power")).collect()[0]#["min_total_overall"]
avg=df_cleaned.agg(sum("House overall [kW]").alias("Avg power")).collect()[0]#["avg"]
df_cleaned=df_cleaned.withColumn("max_power",lit(max_day))
df_cleaned=df_cleaned.withColumn("min_power",lit(min_day))
df_cleaned=df_cleaned.withColumn("avg power",lit(avg))
df_cleaned.show()'''


from pyspark.sql import functions as F, Window


window_spec = Window.partitionBy()


df_with_stats = df_cleaned.withColumn("max_power", F.max("House overall [kW]").over(window_spec)) \
                           .withColumn("min_power", F.min("House overall [kW]").over(window_spec)) \
                           .withColumn("avg_power", F.avg("House overall [kW]").over(window_spec))


df_with_stats.show()
df_cleaned.count()
df_with_stats.printSchema()
from awsglue.dynamicframe import DynamicFrame
df_with_stats.describe()
desired_columns = ["ETL_batch_id", "ETL_batch_date","uuid",
                   "time","use [kW]","gen [kW]","House overall [kW]",
                  "Home office [kW]","Wine cellar [kW]","Garage door [kW]",
                  "Barn [kW]","Well [kW]","Living room [kW]","Solar [kW]","temperature",
                  "icon" ,"humidity","visibility","summary","apparentTemperature","pressure",
                  "windSpeed","cloudCover","windBearing","precipIntensity","dewPoint",
                   "precipProbability","Furnace Total [kW]","Kitchen Total [kW]","Kitchen Appliances",
                   "max_power","min_power","avg_power"]


# Reorder columns
df_final = df_with_stats.select(desired_columns + [col for col in df_with_stats.columns if col not in desired_columns])

df_final.show(55)
dyf2 = DynamicFrame.fromDF(df_final, glueContext)
dyf2 = dyf2.coalesce(2)
df = df.repartition(1)
 
output_path = "s3://aws-dym-bucket/aws-db-smarthome/output"
df.write.csv(output_path, header=True)
try:
    '''data1 = glueContext.write_dynamic_frame_from_options(frame=dyf2,
                                                    connection_type="s3",
                                                    connection_options = {"path": "s3://gen-garage-poc/usac-home/"},
                                                    format='csv',
                                                    format_options={"quoteChar": -1,})'''
    df_final.write.option("header", "true").csv("s3://gen-garage-poc/usac-home")
except Exception as e:
    print("An error occured in loading into S3:", e)
    

import boto3

s3_client = boto3.client('s3')# Create an S3 client

bucket_name = 'gen-garage-poc'
directory_path = '/usac-home/'

response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory_path)

output_files = [obj['Key'] for obj in response.get('Contents', [])]

for old_file_key in output_files:
    new_file_key = directory_path + 'clean_usac.csv' # Rename the file in the S3 bucket
    
    s3_client.copy_object(
        Bucket=bucket_name,
        CopySource={'Bucket': bucket_name, 'Key':old_file_key},
        Key=new_file_key
    )
    # Delete the old file
    s3_client.delete_object(Bucket=bucket_name, Key=old_file_key)
    
try:
    glueContext.write_dynamic_frame_from_options(frame =dyf2, 
                                             connection_type = "dynamodb", 
                                             connection_options = {"tableName": "usac-iot"})
except Exception as e:
    print("An error occurred while loading into dynamodb:", e)


    
