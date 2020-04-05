from pyspark.sql import SparkSession, Row
import threading
from pyspark.sql.functions import col, lit
from pyspark import Row
from pyspark.sql.types import *
import datetime


spark = SparkSession.builder.enableHiveSupport().appName('testscript').config('spark.scheduler.mode','FAIR').getOrCreate()
sc = spark.sparkContext

sc.addPyFile('/application/mock/helper_functions/test_functions.py')
from test_functions import check_completeness, check_columns, summarize_exceptions, process

project_home= '/tmp/application'

run_name = '/mock'
actual_source_stage = '/extract'
expected_source_stage = '/transform'
db = '/data2_database'

table_name='table_name'
foreign_key = 'foreign_key'
columns_in_scope = [<column names>]

               

actual_source_folder = project_home+run_name+actual_source_stage+db+'/'+table_name
expected_source_folder = project_home+run_name+expected_source_stage+db+'/'+table_name
destination_folder = project_home+run_name+'/test'+db+'/'+table_name

actual_df = spark.read.parquet(actual_source_folder)
expected_df = spark.read.parquet(expected_source_folder)

process(expected_df, actual_df, destination_folder, spark, foreign_key, columns_in_scope, table_name)

