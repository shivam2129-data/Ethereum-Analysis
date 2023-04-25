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
            int(fields[11]) #timestamp should be int
            return True
        except:
            return False
    
    def good_line2(line):
        try:
            fields = line.split(',')
            if len(fields)!=6: #total fields in contracts
                return False
            str(fields[0]) #address of contracts should str
            return True
        except:
            return False
        
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
    
    gasAvg = clean_lines.map(lambda b: (time.strftime('%Y-%m', time.gmtime(int(b.split(',')[11]))),(float(b.split(',')[9]),1))) #time in Year month and the gase price along with count
    
    
    
    gasAvg = gasAvg.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) #adding corresponding values of count and gas price
    
    gasline = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    gas = gasline.filter(good_line2)
    
    gas = gas.map(lambda b:(str(b.split(',')[0]),'Contract')) #address of the contract
    tran = clean_lines.map(lambda b: ((str(b.split(',')[6]),time.strftime('%Y-%m', time.gmtime(int(b.split(',')[11])))),(int(b.split(',')[8]),1))) # address along with time in Year- month as key and the gas used, the count as values
    tran = tran.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) #adding corresponding values
    tran = tran.map(lambda b: (b[0][0],(b[0][1],b[1][0],b[1][1]))) #shifting the timestamp as a value to make fit for join
    mixed = tran.join(gas) #joining via address
    out = mixed.map(lambda b: (b[1][0][0],(b[1][0][1],b[1][0][2]))) # taking out the time as key, gas and count as values
    out = out.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) #adding corresponding counts and values
    


    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'Ether' + date_time + '/gas-contract.csv')
    my_result_object.put(Body=json.dumps(out.take(200)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'Ether' + date_time + '/gas-price-avg.csv')
    my_result_object.put(Body=json.dumps(gasAvg.take(200)))
    
    

    spark.stop()
