import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

import argparse
import logging

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

import argparse
import logging

from pyspark.sql import functions as sfunc
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from typing import List
import boto3
import time
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

placeholder = "LastTime.txt"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

## @type: DataSource
## @args: [database = "goodsloags", table_name = "views", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []

#
df_views = glueContext.create_dynamic_frame.from_catalog(database="goodsloags", table_name="views")

df_spark_views = df_views.toDF()

selected_spark_views = df_spark_views
.withColumn("count", sfunc.count("user_ip", "timestamp")).groupBy("user_ip", "timestamp")
.where(df_spark_views.count > 5)

user_ips = DynamicFrame.fromDF(selected_spark_views, glueContext, "user_ips")

ips = RenameField.apply(user_ips, "user_ip", "ip")
ips.show(2)

glueContext.write_dynamic_frame_from_options(
    frame=ips,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.output.tableName": "wrong_ip",
        "dynamodb.throughput.write.percent": "1.0"
    }
)
