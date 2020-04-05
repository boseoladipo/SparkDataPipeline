from pyspark.sql import SparkSession, Row
import threading
from datetime import datetime
from pyspark.sql.functions import col, concat_ws, broadcast, when, regexp_replace, lit
from pyspark.sql.types import *

spark = SparkSession.builder.appName('transform_sttm_cust_account').getOrCreate()
sc = spark.sparkContext

sc.addPyFile('/application/mock/helper_functions/transform_functions.py')
from transform_functions import update_mapping, fill_with_column, fill_with_value, fill_nulls_with_column, fill_with_nulls

mock_name = 'mock'

main_df = spark.read.parquet('/tmp/application/'+mock_name+'/load/data2_database/STTM_CUST_ACCOUNT')
main_df = main_df.filter('account_type!="Y"')

# FILTER OUT RECORD STAT AND AUTH STAT
main_df = main_df.filter('col6=="O" and col5 not in ("111","222")')

main_df = main_df.select(<column names>)


# Get customer type
customer_df = spark.read.parquet('/tmp/application/'+mock_name+'/load/data2_database/STTM_CUSTOMER')
customer_df = customer_df.select('col1','col2')
main_df = main_df.join(customer_df, main_df.CUST_NO==customer_df.col1, how='left')
# main_df = main_df.withColumn('col3', concat_ws('_','col3','col2'))

# Apply account class mapping
class_map_df = spark.read.parquet('/tmp/application/'+mock_name+'/load/combined_database/MAPPING')

main_df = main_df.join(broadcast(class_map_df), on=[main_df.col3==class_map_df.data1ACLASS, main_df.col2==class_map_df.CUSTTYPE], how='left')

main_df = main_df.withColumn('col3', col('data2aclass'))


#Join
map_df = spark.read.parquet('/tmp/application/'+mock_name+'/load/combined_database/STTM_col3')
map_df = map_df.select(<column names>)

main_df = main_df.join(broadcast(map_df), on='col3', how='left')

# AC_CLASS_TYPE
main_df = main_df.withColumn('ACCOUNT_TYPE', col('AC_CLASS_TYPE'))

#Apply branch_code mapping
map_df = spark.read.parquet('/tmp/application/'+mock_name+'/load/combined_database/MIGT_BRANCH_MAPPING')

data1_branch_key = 'SOURCE_BRANCH'
data1_branch_key = 'DEST_BRANCH'
data1_replace_key = 'BRANCH_CODE'

main_df = update_mapping(main_df, map_df, data1_branch_key, data1_branch_key, data1_replace_key)


# LOCATION
map_df = spark.read.parquet('/tmp/application/'+mock_name+'/load/combined_database/MIGT_COUNTRY')
data1_key = 'data1_COUNTRY'
data1_key = 'data2_COUNTRY'
column_to_replace = ['LOCATION','COUNTRY_CODE']
main_df = update_mapping(main_df, map_df, data1_key, data1_key, column_to_replace)

ng_map_df = spark.createDataFrame([('NGA', 'NG'),('CHN','CHS')], ['data1_COUNTRY','data2_COUNTRY'])
data1_key = 'data1_COUNTRY'
data1_key = 'data2_COUNTRY'
column_to_replace = ['LOCATION','COUNTRY_CODE']
main_df = update_mapping(main_df, ng_map_df, data1_key, data1_key, column_to_replace)

# LIMIT_CCY
map_df = spark.read.csv('/tmp/application/'+mock_name+'/load/combined_database/LIMIT_CCY_MAPPING.csv', header=True)
data1_key = 'data1_CODE'
data1_key = 'data2_CODE'
column_to_replace = ['LIMIT_CCY','CCY']
main_df = update_mapping(main_df, map_df, data1_key, data1_key, column_to_replace)

# ACC_STATUS
map_df = spark.read.csv('/tmp/application/'+mock_name+'/load/combined_database/STATUS_CODE_MAPPING.csv', header=True)
data1_key = 'data1 STATUS_CODE'
data1_key = 'data1 STATUS CODE'
column_to_replace = ['ACC_STATUS']
main_df = update_mapping(main_df, map_df, data1_key, data1_key, column_to_replace)

#Apply  mapping
acc_status_map_df = spark.read.parquet('/tmp/application/'+mock_name+'/load/combined_database/STATUS')

map_df = acc_status_map_df.selectExpr('col3 as ACC_CLASS', 'STATUS', 'DR_', 'CR_')

main_df = main_df.drop('dr_','cr_')
main_df = main_df.join(broadcast(map_df), on=[main_df.col3==map_df.ACC_CLASS, main_df.ACC_STATUS==map_df.STATUS], how='left')


# CUST_NO
map_df = spark.read.parquet('/tmp/application/'+mock_name+'/load/combined_database/MAPPING')
joined_df = main_df.join(map_df, main_df.CUST_NO==map_df.SOURCE_CUSTOMER, how='left')
joined_df = joined_df.withColumn('DEST_CUSTOMER', when(joined_df.DEST_CUSTOMER.isNull(),joined_df.CUST_NO).otherwise(joined_df.DEST_CUSTOMER))
main_df = joined_df.withColumn('CUST_NO', joined_df.DEST_CUSTOMER).drop('source_customer','dest_customer')


# DEFAULT NULLS
columns_dict = {<columns and values>}

main_df = main_df.fillna(columns_dict)

# FILL WITH NULLS
main_df = fill_with_nulls(main_df, ['ATM','ATM_FACILITY'])

# REPLACE SPECIAL CHARACTERS
main_df = main_df.withColumn('ADDRESS1',regexp_replace(col('ADDRESS1'),'[<>~;^]',''))
main_df = main_df.withColumn('ADDRESS2',regexp_replace(col('ADDRESS2'),'[<>~;^]',''))
main_df = main_df.withColumn('ADDRESS3',regexp_replace(col('ADDRESS3'),'[<>~;^]',''))
main_df = main_df.withColumn('ADDRESS4',regexp_replace(col('ADDRESS4'),'[<>~;^]',''))


# CLEARING_AC_NO
columns_to_fill = ['ac']
main_df = fill_nulls_with_column(main_df, columns_to_fill, 'ac')


# FILL WITH VALUE
columns_to_fill = [<column names>]
main_df = fill_with_value(main_df, columns_to_fill, '0')


#write output to parquet
main_df.repartition(200).write.parquet('/tmp/application/'+mock_name+'/transform/data2_database/STTM_CUST_ACCOUNT_CASA', mode='overwrite')





# Split Extract into casa and td

main_df = spark.read.parquet('/tmp/application/'+mock_name+'/extract/data2_database/STTM_CUST_ACCOUNT')


main_df = main_df.select(<column names>)

main_df_casa = main_df.filter(main_df['ACCOUNT_TYPE'] != "Y")


#write output to parquet

main_df.repartition(200).write.parquet('/tmp/application/'+mock_name+'/extract/data2_database/STTM_CUST_ACCOUNT_CASA', mode='overwrite')