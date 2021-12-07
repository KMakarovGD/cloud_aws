#!/usr/bin/python3

# -*- coding: utf-8 -*-
"""
Spark filter for suspicious ip-addressess
"""

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


def get_ip_from_s3_data_set(part_num):
    """
    get json files with wiewlogs
    :param time:
    :return:
    """

    str_time = time.strftime("%Y%m%d-%H%M")

    ip_s = []

    schema = StructType([ \
        StructField("item_id", IntegerType(), True), \
        StructField("timestamp", StringType(), True), \
        StructField("device_type", StringType(), True), \
        StructField("device_id", StringType(), True), \
        StructField("user_ip", StringType(), True) \
        ])

    with SparkSession.builder.appName("Myfilterapp").getOrCreate() as spark:
        df = spark.read.format('csv').schema(schema).options(header='true', inferSchema='true', delimiter=",") \
            .load("s3a://mackdevicelist/views/{}.csv".format(str_time)).groupBy("user_ip", "timestamp") \
            .withColumn("count", sfunc.count("user_ip", "timestamp"))

        if __name__ == '__main__':
            ip_s = df.select(df.user_ip).where(df.count > 5).collect()
        with open(placeholder, 'w') as f:
            f.write(str(str_time))
    return ip_s


def write_ip_to_db(ips: List[str]):
    """
    write suspicious ip to db
    :param ip: ipaddresses to save to DB
    :return:
    """
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table('wrong_ip')

    for ip in ips:
        with table.batch_writer() as batch:
            try:
                batch.put_item(Item={
                    'ip': ip,

                }
                )
                print("Wrote ip into batch.")
            except:
                print("Error processing invalid input row.")


def get_suspicious_ips(partitions: int):
    """
    :param partitions: The number of partitions to use for the calculation.

    """

    tries = 100000 * partitions
    logger.info(
        "Calculating pi with a total of %s tries in %s partitions.", tries, partitions)
    with SparkSession.builder.appName("My PyPi").getOrCreate() as spark:
        wrong_ips = get_ip_from_s3_data_set()

        logger.info("%s tries and %s hits gives pi estimate of %s.", tries)

        # write to DynamoDB
        write_ip_to_db(wrong_ips)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--partitions', default=2, type=int,
        help="The number of parallel partitions to use when calculating pi.")

    args = parser.parse_args()

    get_suspicious_ips(args.partitions)
