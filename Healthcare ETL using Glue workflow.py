import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
glue_client = boto3.client('glue')
glue_args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
workflow_args = glue_client.get_workflow_run_properties(Name=glue_args['WORKFLOW_NAME'],RunId=glue_args['WORKFLOW_RUN_ID'])["RunProperties"]

input_path = workflow_args['input_path']
output_path = workflow_args['output_path']
bucket_name = workflow_args['bucket_name']
dir_path = workflow_args['dir_path']
redshift_url = workflow_args['redshift_url']
redshift_table = workflow_args['redshift_table']
dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="csv",
    format_options={"withHeader": True}
)
df = dyf.toDF()

from datetime import datetime
from pyspark.sql.functions import lit
now = datetime.now()
etlbatch_id = now.strftime("%Y%m%d%H%M%S%f")[:17]
etlbatch_date =  r"""{}-{}-{}""".format(etlbatch_id[0:4],etlbatch_id[4:6],etlbatch_id[6:8]) #now.strftime("%Y-%m-%d")[:10]
start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(etlbatch_id,etlbatch_date)
df = df.withColumn("ETLBatchID", lit(etlbatch_id)) \
       .withColumn("ETLBatchDate", lit(etlbatch_date))
       
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import uuid 
from pyspark.sql import functions as F
uuidUdf = F.udf(lambda: str(uuid.uuid4()), StringType())
df = df.withColumn("uuid", uuidUdf())

from pyspark.sql.functions import col

remove_values = ['', 'null', 'NA', 'None']
for column in df.columns:
    for value in remove_values:
        df = df.filter(col(column) != value)

df=df.dropDuplicates()

from pyspark.sql.functions import datediff, to_date
df = df.withColumn('Date of Admission', to_date(df['Date of Admission'], 'dd-MM-yyyy'))
df = df.withColumn('Discharge Date', to_date(df['Discharge Date'], 'dd-MM-yyyy'))
df = df.withColumn('Admission Days', datediff(df['Discharge Date'], df['Date of Admission']))

from pyspark.sql.types import *
from pyspark.sql.functions import *
df.withColumn("Billing Amount",df["Billing Amount"].cast(DoubleType()))
df = df.withColumn("Billing Amount", round(df["Billing Amount"], 2))

from pyspark.sql.functions import col, when, max,format_number
max_age = df.select(max(df["Age"])).collect()[0][0]
max_billing = df.select(max(df["Billing Amount"])).collect()[0][0]
weight_age = 0.4
weight_medical_condition = 0.3
weight_billing_amount = 0.3
df = df.withColumn("Risk Score",
    (col("Age") / max_age) * weight_age +
    (when(col("Medical Condition") == 'Diabetes', 1).otherwise(0) * weight_medical_condition) +
    (col("Billing Amount") / max_billing) * weight_billing_amount
)
df = df.withColumn("Risk Score", format_number(df["Risk Score"], 2))

from pyspark.sql.types import *

df=df.withColumn("Age",df["Age"].cast(IntegerType()))
df=df.withColumn("Room Number",df["Room Number"].cast(IntegerType()))
df=df.withColumn("ETLBatchID",df["ETLBatchID"].cast(LongType()))
df=df.withColumn("ETLBatchDate",df["ETLBatchDate"].cast(DateType()))
df=df.withColumn("Admission Days",df["Admission Days"].cast(IntegerType()))
df=df.withColumn("Risk Score",df["Risk Score"].cast(FloatType()))

from pyspark.sql.functions import *
df1=df.select(df["uuid"],df["ETLBatchID"],df["ETLBatchDate"],df["Name"],df["Age"],df["Gender"],df["Blood Type"],df["Medical Condition"],df["Date of Admission"],df["Doctor"],df["Hospital"],df["Insurance Provider"],df["Billing Amount"],df["Room Number"],df["Admission Type"],df["Discharge Date"],df["Medication"],df["Test Results"],df["Admission Days"],df["Risk Score"])

df=df1

df.write.option("header", "true").csv(output_path)

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
    

redshift_url = redshift_url
redshift_username = "awsuser"
redshift_password = "aws_123CG"
redshift_table = redshift_table
df5.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", redshift_table) \
    .option("user", redshift_username) \
    .option("password", redshift_password) \
    .mode("overwrite") \
    .save()

job.commit()
