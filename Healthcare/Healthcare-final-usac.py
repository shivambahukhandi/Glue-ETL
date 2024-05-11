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

import boto3
glue_client=boto3.client('glue')
glue_args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])

workflow_args = glue_client.get_workflow_run_properties(Name=glue_args['WORKFLOW_NAME'], RunId=glue_args['WORKFLOW_RUN_ID'])["RunProperties"]
input_path = workflow_args['input_path']
output_path = workflow_args['output_path']

dyf = glueContext.create_dynamic_frame_from_options(connection_type="s3",connection_options={"paths": [input_path]}, format="csv",format_options={"withHeader": True})


df = dyf.toDF()


from pyspark.sql.functions import col,lit,current_date
from datetime import datetime

etlbatch_id = datetime.now().strftime("%Y%m%d%H%M%S%f")[:17]
df = df.withColumn("ETL_batch_id",lit(etlbatch_id))
df = df.withColumn("ETL_batch_date", lit(current_date()))


#dyf = glueContext.create_dynamic_frame_from_options(connection_type="s3",connection_options={"paths": ["s3://s3-bkt-us/sample.csv"]}, format="csv",format_options={"withHeader": True})

#df = dyf.toDF()
#df.show(55)

df = df.na.replace("", None)\
       .na.replace("NA",None)\
       .na.replace("None",None)


df.show(55)
print(df.count())

df_no_duplicates = df.dropDuplicates()

df_no_duplicates.show(55) 
print(df_no_duplicates.count())

df_no_duplicates.show(55)
print(df.count())

df_cleaned = df_no_duplicates.dropna(how='all',subset = None)

df_cleaned.show(55)
print(df_cleaned.count())


#from pyspark.sql import SparkSession
from pyspark.sql.functions import datediff, to_date
 
df_cleaned = df_cleaned.withColumn('Date of Admission', to_date(df_cleaned['Date of Admission'], 'dd-MM-yyyy')) 
df_cleaned = df_cleaned.withColumn('Discharge Date', to_date(df_cleaned['Discharge Date'], 'dd-MM-yyyy')) 
df_cleaned = df_cleaned.withColumn('Admission Days', datediff(df_cleaned['Discharge Date'], df_cleaned['Date of Admission']))
 
df_cleaned.show(55)

df_cleaned.printSchema()

from pyspark.sql.functions import udf
import uuid
def generate_uuid():
    return str(uuid.uuid4())

generate_uuid_udf = udf(generate_uuid)
 
df_cleaned = df_cleaned.withColumn('UUID', generate_uuid_udf())
#df.show()


from pyspark.sql.functions import lit,col
from pyspark.sql.types import FloatType

def calculate_risk_score(age, max_age, medical_condition, billing_amount, max_billing_amount):
    if age is None or max_age is None or billing_amount is None or max_billing_amount is None:
        return None
    weight_age = 0.4
    weight_medical_condition = 0.3
    weight_billing_amount = 0.3
    age_score = (float(age) / float(max_age)) * weight_age
    diabetes_score = float((1 if medical_condition == "Diabetes" else 0)) * weight_medical_condition
    billing_score = round(float(billing_amount),2) / round(float(max_billing_amount),2) * weight_billing_amount
    answer=age_score + diabetes_score + billing_score
    answer=round(answer,2)
    return answer
 
calculate_risk_score_udf = udf(calculate_risk_score, FloatType())

max_age = df_cleaned.agg({"Age": "max"}).collect()[0][0]
max_billing_amount = df_cleaned.agg({"Billing Amount": "max"}).collect()[0][0]
 
df_cleaned = df_cleaned.withColumn("Risk Score", calculate_risk_score_udf(
    col("Age"),
    lit(max_age),
    col("Medical Condition"),
    col("Billing Amount"),
    lit(max_billing_amount)
))

df_cleaned.show()




# Define function to cast values to desired data types
def cast_column_values(column_name, data_type):
    return col(column_name).cast(data_type)
 
# Define mapping of column names to data types
column_data_types = {
    "ETL_batch_id": "integer",
    "ETL_batch_date": "date",
    "UUID": "string",
    "Name": "string",
    "Age": "integer",
    "Gender": "string",
    "Blood Type": "string",
    "Medical Condition": "string",
    "Date of Admission": "date",
    "Doctor": "string",
    "Hospital": "string",
    "Insurance Provider": "string",
    "Billing Amount": "float",
    "Room Number": "integer",
    "Admission Type": "string",
    "Discharge Date": "date",
    "Medication": "string",
    "Test Results": "string",
    "Admission Days": "integer",
    "Risk Score": "float"
    # Add more columns and respective data types as needed
}
 
# Apply function to convert data types for each column
for column, data_type in column_data_types.items():
    df_cleaned = df_cleaned.withColumn(column, cast_column_values(column, data_type))
df_cleaned.printSchema()
#df_cleaned.show()



desired_columns = ['ETL_batch_id', 'ETL_batch_date','UUID','Name','Age','Gender','Blood Type','Medical Condition','Date of Admission','Doctor','Hospital','Insurance Provider',
                   'Billing Amount','Room Number', 'Admission Type','Discharge Date','Medication','Test Results','Admission Days','Risk Score']


# Reorder columns
df_cleaned = df_cleaned.select(desired_columns + [col for col in df.columns if col not in desired_columns])





from awsglue.dynamicframe import DynamicFrame
dyf_new=DynamicFrame.fromDF(df_cleaned, glueContext, "final_df")

dyf_new.coalesce(2)

glueContext.write_dynamic_frame_from_options(frame=dyf_new, 
                                             connection_type = "dynamodb", 
                                             connection_options = {"tableName": "med"})
        

data1 = glueContext.write_dynamic_frame_from_options(frame=dyf_new,
                                                    connection_type="s3",
                                                    connection_options = {"path":output_path},
                                                    format='csv',
                                                    format_options={"quoteChar": -1,})
                                                    
import boto3
s3_client = boto3.client('s3')
bucket_name = bucket_name
directory_path = dir_path
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory_path)
output_files = [obj['Key'] for obj in response.get('Contents', [])]
for old_file_key in output_files:
    new_file_key = directory_path + 'Healthcare.csv'
    s3_client.copy_object(
        Bucket=bucket_name,
        CopySource={'Bucket': bucket_name, 'Key': old_file_key},
        Key=new_file_key
    )
    s3_client.delete_object(Bucket=bucket_name, Key=old_file_key)
