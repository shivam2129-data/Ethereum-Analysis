import sys, string
import os
import socket
import time
import operator
import boto3
import json
import pandas as pd                                                            
from pyspark.sql import SparkSession
from datetime import datetime
                                                            
                                                

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ether")\
        .getOrCreate()

    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=19: #total number of fields in blocks
                return False
            str(fields[9]) #address of the miner should be str
            int(fields[12]) #size should be int
            return True
        except:
            return False

    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    clean_lines = lines.filter(good_line)
    block = clean_lines.map(lambda b: (str(b.split(',')[9]),int(b.split(',')[12]))) #address and size
    block = block.reduceByKey(lambda a,b: a+b) #adding the sizes
    top10 = block.takeOrdered(10, key = lambda x: -x[1]) #taking top 10 szie
    
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'Ether' + date_time + '/top10-Miner.txt')
    my_result_object.put(Body=json.dumps(top10))
    
    
    

    spark.stop()