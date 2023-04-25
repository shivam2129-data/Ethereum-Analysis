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

    def good_line1(line):
        try:
            fields = line.split(',')
            if len(fields)!=15: #total fields in transactions
                return False
            str(fields[6]) #address field should be str
            int(fields[11]) #timestamp field should be int
            return True
        except:
            return False
    def good_line2(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:#total fields in contracts
                return False
            str(fields[0]) #address fields should be str
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

    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines = lines.filter(good_line1)
    lines1 = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    clean_lines1 = lines1.filter(good_line2)
        
    tran = clean_lines.map(lambda l: (str(l.split(',')[6]), float(l.split(',')[7]))) #transaction address and value
    tran_red = tran.reduceByKey(operator.add) #adding values of transactions
    tcjoin = tran_red.join(clean_lines1.map(lambda x: (str(x.split(',')[0]), 'contract'))) #joining transaction and contract via address field
    top10 = tcjoin.takeOrdered(10, key = lambda x: -x[1][0]) #taking top 10 values

    for record in top10:
        print("{},{}".format(record[0], float(record[1][0])))
        
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'Ether' + date_time + '/top10.txt')
    my_result_object.put(Body=json.dumps(top10))

    spark.stop()

