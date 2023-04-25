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
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
                                                            
                                               
if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ether")\
        .getOrCreate()

    def good_line1(line):
        try:
            fields = line.split(',')
            if len(fields)!=15: #total number of fields in transactions
                return False
            str(fields[6]) #address field should be str
            int(fields[11]) #timestamp should be int
            return True
        except:
            return False
        
    def good_line2(line):
        try:
            fields = line.split(',')
            if len(fields)!=8: #total number of fields in the csv of scams, note that it is different from the schema in json
                return False
            str(fields[6]) #address should be str
            str(fields[4]) #category of scam should be str
            str(fields[0]) #ID of scam should be str
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
    
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv").filter(good_line1)
    scams = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.csv").filter(good_line2)
    

    
    scam = scams.map(lambda b: (str(b.split(',')[6]), (str(b.split(',')[4]),str(b.split(',')[0])))) #address along with type and ID 

    
    tran = transactions.map(lambda b: (b.split(',')[6],(float(b.split(',')[7]),time.strftime('%Y-%m', time.gmtime(int(b.split(',')[11]))),1))) #address along with value, time in Year month and the count
    join = tran.join(scam) #joining with address field
    
    tscomb1 = join.map(lambda b: ((b[1][1][0],b[1][0][1]), (b[1][0][0], b[1][0][2]))) #Using Date and time as keys with value and count as values
    tscomb2= join.map(lambda b: ( b[1][1][1], (b[1][0][0]/100000000000000000000, b[1][0][2]))) #Using ID as key and Value, count and key, dividing as the value is a huge number thus making it easir to plot afterwards
    out1 = tscomb1.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) #adding corresponding values for value and count of date and time keys
    out1 = out1.map(lambda b: (b[0][1],(b[0][0],b[1][0]/100000000000000000000,b[1][1]))) #dividing as the value is a huge number thus making it easir to plot afterwards
    out2 = tscomb2.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) #adding corresponding values for ID of date and time keys
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'Ether' + date_time + '/scams.csv')
    my_result_object.put(Body=json.dumps(out1.take(2000)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'Ether' + date_time + '/most-lucarative.csv')
    my_result_object.put(Body=json.dumps(out2.take(2000)))
        
    spark.stop()