#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on Thu Sep 30 18:57:04 2021

@author: Jyothish KP
"""

from datetime import datetime

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, current_timestamp,avg,max,min,coalesce
from pyspark.sql.types import *


#Utils
def get_current_date_time_str():
    return datetime.now().strftime("%Y%m%d%H%M%S")

def close_spark_session(spark):
    spark.stop()
    
def get_spark_session(pipeline_name):
    spark = SparkSession.builder.master("local[*]").appName(pipeline_name).getOrCreate()  
    return spark

def read_basic_pipeline_config(pipeline_config_path):
    config = dict(line.strip().split('=') for line in open(pipeline_config_path) if not line.strip().startswith('#'))    
    return (config)

def get_schema():
    schema = StructType() \
        .add("device", StringType(),True) \
        .add("serial_no",StringType(),True) \
        .add("device_ts",StringType(),True) \
        .add("record_type",IntegerType(),True) \
        .add("hist_glucose",DoubleType(),True) \
        .add("scan_glucose",DoubleType(),True) 
    return schema

def save_data(df, num_partitions,out_path):
    df.coalesce(num_partitions).write.option("header", "true").csv(out_path)
	#df.write.option("header", "true").csv(out_path)


def transform(in_path, schema, spark):
    df = spark.read.options(header=True,delimiter=',').schema(schema).csv(in_path)
    df.printSchema()
    df.show(2)
    
    df1_1 = df.select(col("device_ts"), col("hist_glucose").alias("glucose")).where(col("hist_glucose").isNotNull())
    df1_2 = df.select(col("device_ts"), col("scan_glucose").alias("glucose")).where(col("scan_glucose").isNotNull())
    df1 = df1_1.union(df1_2)
    df1.cache()
    #df1.show(20)

    df1.createOrReplaceTempView("glucose_df1")
    df2 = spark.sql("SELECT CAST(UNIX_TIMESTAMP(device_ts, 'yyyy-MM-dd HH:mm') AS TIMESTAMP) AS device_ts,glucose FROM glucose_df1")
    df2.printSchema()
    df2.createOrReplaceTempView("glucose_df2")
    
    df3 = spark.sql("SELECT device_ts,to_date(device_ts) as snap_date,glucose,lpad(cast(year(device_ts) As String),4,'0') as year, lpad(cast(month(device_ts) AS String),2,'0') as month, lpad(cast(day(device_ts) AS String),2,'0') AS day, lpad(cast(hour(device_ts) as String),2,'0') as hour,lpad(cast(minute(device_ts) AS String),2,'0') as minute, current_timestamp() as load_ts FROM glucose_df2 WHERE device_ts is not null ")
    #df3 = spark.sql("SELECT device_ts,to_date(device_ts) as snap_date,glucose,year(device_ts) as year, month(device_ts) as month, day(device_ts) as day, hour(device_ts) as hour,minute(device_ts) as minute, current_timestamp() as load_ts FROM glucose_df2 WHERE device_ts is not null")
    df3.printSchema()
    df3.cache()
    df3.show(5)
    
    df4_01 = df3.groupBy("snap_date").agg(min("glucose").alias("lower_glucose"),max("glucose").alias("higer_glucose"),avg("glucose").alias("average_glucose")).orderBy("snap_date").where(col("snap_date").isNotNull())
    df4_01.createOrReplaceTempView("glucose_df4_01")
    df4 = spark.sql("select snap_date,lower_glucose, higer_glucose,round(average_glucose,1) as average_glucose FROM glucose_df4_01 ORDER BY snap_date")
    df4.cache()
    df4.show(20)
 
    df5_01 = df3.groupBy("snap_date","hour").agg(min("glucose").alias("lower_glucose"),max("glucose").alias("higer_glucose"),avg("glucose").alias("average_glucose")).orderBy("snap_date","hour").where(col("snap_date").isNotNull())
    df5_01.createOrReplaceTempView("glucose_df5_01")
    df5 = spark.sql("select snap_date,hour,lower_glucose, higer_glucose,round(average_glucose,1) as average_glucose FROM glucose_df5_01 ORDER BY snap_date,hour")   
    df5.cache()
    df5.show(40)
    
    return (df4,df5)
    

def get_config_params(pipeline_name):
    pipeline_config_path = f"conf/{pipeline_name}_pipeline.conf"
    print(f"pipeline_config_path : {pipeline_config_path}")
    config= read_basic_pipeline_config(pipeline_config_path)
    print(config)
    inbound=config["inbound"]
    inbound_filename=config['inbound.filename']
    outbound=config["outbound"]       
    num_partitions=int(config["numPartitions"])
    return (inbound,inbound_filename,outbound,num_partitions)

def service_request(pipeline_name):
    
    (inbound,inbound_filename,outbound,num_partitions) = get_config_params(pipeline_name)    
    in_path = f"{inbound}/{inbound_filename}"
    out_path = f"{outbound}/{get_current_date_time_str()}"
    schema = get_schema()
    print(f"in_path : {in_path}")    
    print(out_path)
    print(schema)   
    
    
    spark = get_spark_session(pipeline_name)
    
    (df_daily,df_hourly) = transform(in_path, schema, spark)
    out_path_daily = f"{out_path}_daily"
    save_data(df_daily,num_partitions,out_path_daily)
    
    out_path_hourly = f"{out_path}_hourly"
    save_data(df_hourly,num_partitions,out_path_hourly)  

    
    close_spark_session(spark)
    
    

    
