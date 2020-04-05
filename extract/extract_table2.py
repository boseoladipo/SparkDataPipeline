from pyspark.sql import SparkSession, Row
import threading
from datetime import datetime


spark = SparkSession.builder.appName('extract').config('spark.scheduler.mode','FAIR').enableHiveSupport().getOrCreate()
sc = spark.sparkContext

sc.addPyFile('/application/mock_3/helper_functions/extract_functions.py')
from extract_functions import cache_df, register_sql_table, write_table, extract_tables_using_upload, extract_tables_using_data1

project_home= '/tmp/application'
    
run_name = '/mock'
stage = '/extract'
data2 = '/data2_database'
data1 = '/data1_database'
combined = '/combined_database'

data2_results_folder = project_home+run_name+stage+data2
data1_results_folder = project_home+run_name+stage+data1
results_folders = {'data2_df':data2_results_folder,'data1_df':data1_results_folder}

stage_folder = project_home+run_name+stage

data2_source_folder = project_home+run_name+'/load'+data2
data1_source_folder = project_home+run_name+'/load'+data1
combined_source_folder = project_home+run_name+'/load'+combined

table_name = '/STTM_CUSTOMER'
upload_table_name = '/STTM_UPLOAD_CUSTOMER'

data1_combined_key = 'CUSTOMER_NO'

data1_df = spark.read.parquet(data1_source_folder+table_name)
combined_df = spark.read.parquet(combined_source_folder+table_name)
# upload_df = spark.read.parquet(combined_source_folder+upload_table_name)

# extracted_dfs = extract_tables_using_upload(upload_df, combined_df, data1_combined_key, spark)
extracted_dfs = extract_tables_using_data1(data1_df, combined_df, data1_combined_key, spark)


jobs = []

for df_name, df in extracted_dfs.items():
    thread = threading.Thread(target=write_table,args=(df, df_name, results_folders, table_name,spark))
    jobs.append(thread)

for job in jobs:
    job.start()

for job in jobs:
    job.join()
