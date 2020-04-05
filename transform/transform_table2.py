from pyspark.sql import SparkSession, Row
import threading
from datetime import datetime
from pyspark.sql.functions import lit, when, concat_ws, col, concat


spark = SparkSession.builder.appName('transform').getOrCreate()
sc = spark.sparkContext

mock_name = 'mock'
table_name = 'table_name'

sc.addPyFile('/application/'+mock_name+'/helper_functions/transform_functions.py')
from transform_functions import update_mapping, fill_with_column, fill_with_value, fill_with_nulls, fill_nulls_with_column, fill_not_nulls_with_column

mock_name = 'mock'
table_name = 'table_name'

main_df = spark.read.parquet('/tmp/application/'+mock_name+'/load/data2_database/'+table_name)

# FILTER OUT RECORD STAT AND AUTH STAT
main_df = main_df.filter('record_stat=="O" and local_branch not in ("111","222")')

# EXT_REF_NO
columns_to_fill = ['EXT_REF_NO']
main_df = main_df.withColumn('EXT_REF_NO', concat(col('customer_no'),lit('_data2')))

# # INTRODUCER
# columns_to_fill = ['INTRODUCER']
# main_df = fill_not_nulls_with_column(main_df, columns_to_fill, 'CUSTOMER_NO')

# CUSTOMER_NO
map_df = spark.read.parquet('/tmp/application/'+mock_name+'/load/combined_database/MIGT_CUSTOMER_MAPPING')
joined_df = main_df.join(map_df, main_df.CUSTOMER_NO==map_df.SOURCE_CUSTOMER, how='left')
main_df = joined_df.withColumn('CUSTOMER_NO', when(joined_df.DEST_CUSTOMER.isNull(),joined_df.CUSTOMER_NO).otherwise(joined_df.DEST_CUSTOMER)).drop('source_customer','dest_customer')

# COUNTRY, NATIONALITY
map_df = spark.read.parquet('/tmp/application/'+mock_name+'/load/combined_database/MIGT_COUNTRY')
data2_key = 'data2_COUNTRY'
data1_key = 'data1_COUNTRY'
column_to_replace = ['COUNTRY','NATIONALITY','EXPOSURE_COUNTRY']
main_df = update_mapping(main_df, map_df, data2_key, data1_key, column_to_replace)

ng_map_df = spark.createDataFrame([('NGA', 'NG'),('CHN','CHS')], ['data2_COUNTRY','data1_COUNTRY'])
data2_key = 'data2_COUNTRY'
data1_key = 'data1_COUNTRY'
column_to_replace = ['COUNTRY','NATIONALITY','EXPOSURE_COUNTRY']
main_df = update_mapping(main_df, ng_map_df, data2_key, data1_key, column_to_replace)

# SHORT_NAME
main_df = main_df.withColumn('SHORT_NAME',when(main_df.SHORT_NAME.isNull(),main_df.CUSTOMER_NO).otherwise(main_df.SHORT_NAME))

count_df = main_df.groupby('SHORT_NAME').count().filter('count > 1')
main_df.registerTempTable('main_df')
count_df.registerTempTable('duplicate_sname')
main_df = spark.sql("""select a.*, case when b.short_name is not null then concat_ws('_',substring(b.short_name,1,10),a.customer_no)
                                   else a.short_name
                                   end as NEW_SHORT_NAME 
                        from main_df a 
                        left join duplicate_sname b on a.short_name=b.short_name""")
main_df = main_df.withColumn('SHORT_NAME', col('new_short_name'))


# LOCAL_BRANCH, LIAB_BR
map_df = spark.read.parquet('/tmp/application/'+mock_name+'/load/combined_database/MIGT_BRANCH_MAPPING')
data2_key = 'SOURCE_BRANCH'
data1_key = 'DEST_BRANCH'
column_to_replace = ['LOCAL_BRANCH','LIAB_BR']
main_df = update_mapping(main_df, map_df, data2_key, data1_key, column_to_replace)

UNIQUE_ID_VALUE, UNIQUE_ID_NAME
count_df = main_df.groupby(concat('UNIQUE_ID_NAME','UNIQUE_ID_VALUE').alias('combined')).count().filter('count > 1')
# duplicated_values = [str(row.UNIQUE_ID_NAME)+'_'+str(row.UNIQUE_ID_VALUE) for row in count_df.collect()]
count_df.registerTempTable('duplicate_id')
main_df.registerTempTable('main_df')

main_df = spark.sql("""select a.*, case when b.combined is null then a.unique_id_name end as NEW_NAME,
                        case when b.combined is null then a.unique_id_value end as NEW_VALUE
                        from main_df a 
                        left join duplicate_id b on concat(a.unique_id_name,a.unique_id_value)=b.combined""")

main_df = main_df.withColumn('DUPLICATE_ID_FLAG',when(concat_ws('_','unique_id_name','unique_id_value').isin(duplicated_values),lit('YES')).otherwise(lit('NO')))

main_df = main_df.withColumn('UNIQUE_ID_NAME',when(col('DUPLICATE_ID_FLAG')=='YES',lit(None)).otherwise(main_df.UNIQUE_ID_NAME))
main_df = main_df.withColumn('UNIQUE_ID_VALUE',when(col('DUPLICATE_ID_FLAG')=='YES',lit(None)).otherwise(main_df.UNIQUE_ID_VALUE))
main_df = main_df.drop('duplicate_id_flag')
main_df = main_df.withColumn('UNIQUE_ID_NAME', col('new_name'))
main_df = main_df.withColumn('UNIQUE_ID_VALUE', col('new_value'))



# CUSTOMER_CATEGORY
map_df = spark.read.csv('/tmp/application/'+mock_name+'/load/combined_database/CUSTOMER_CATEGORY_MAPPING.csv', header=True)
data2_key = 'data2_CATEGORY'
data1_key = 'data1_CATEGORY'
column_to_replace = ['CUSTOMER_CATEGORY']
main_df = update_mapping(main_df, map_df, data2_key, data1_key, column_to_replace)

# LIMIT_CCY
map_df = spark.read.csv('/tmp/application/'+mock_name+'/load/combined_database/LIMIT_CCY_MAPPING.csv', header=True)
data2_key = 'data2_CODE'
data1_key = 'data1_CODE'
column_to_replace = ['LIMIT_CCY']
main_df = update_mapping(main_df, map_df, data2_key, data1_key, column_to_replace)

# Fill with cusotmer_no
columns_to_fill = ['LIABILITY_NO', 'FX_NETTING_CUSTOMER', 'ELCM_CUSTOMER_NO', 'INTRODUCER']
main_df = fill_not_nulls_with_column(main_df, columns_to_fill, 'CUSTOMER_NO')

# # LIAB_NODE
# columns_to_fill = ['LIAB_NODE']
# main_df = fill_with_value(main_df, columns_to_fill, 'FCUBSPAR')


# ADDRESS_LINE 2
main_df = main_df.withColumn('ADDRESS_LINE2', 
                                when(col('address_line2').isNull(),concat(lit('-'),col('address_line3')))\
                                    .otherwise(concat_ws('-',col('address_line2'),col('address_line3')).substr(1,105)))

# Fill with NULL
main_df = fill_with_nulls(main_df, ['ADDRESS_LINE3', 
                                        # 'AML_CUSTOMER_GRP',
                                        # 'UTILITY_PROVIDER_TYPE',
                                        'AUTO_CUST_AC_NO','RM_ID',
                                        'GROUP_CODE','CHARGE_GROUP','TAX_GROUP',
                                        # 'MFI_CUSTOMER',
                                        ])


# DEFAULT NULLS
columns_dict = {'FROZEN':'N',
                    'DECEASED':'N',
                    'WHEREABOUTS_UNKNOWN':'N',
                    # 'DEFAULT_MEDIA':'MAIL',
                    # 'TRACK_LIMITS':'N',
                    'AML_REQUIRED':'N',
                    'INDIVIDUAL_TAX_CERT_REQD':'N',
                    'CONSOL_TAX_CERT_REQD':'N',
                    'RP_CUSTOMER':'N',
                    'CLS_PARTICIPANT':'N',
                    'UNADVISED':'N',
                    'CLS_CCY_ALLOWED':'D',
                    # 'JV_LIMIT_TRACKING':'N',
                    # 'JOINT_VENTURE':'N',
                    'AUTO_CREATE_ACCOUNT':'N',
                    # 'GROUP_CODE':'INDIVIDUAL',
                    'LANGUAGE':'ENG',
                    'LIMIT_CCY':'NGN',
                    'NATIONALITY':'NG',
                    'COUNTRY':'NG',
                    'EXPOSURE_COUNTRY':'NG'}

main_df = main_df.fillna(columns_dict)


#write output to parquet
main_df.repartition(200).write.parquet('/tmp/application/'+mock_name+'/transform/data2_database/'+table_name, mode='overwrite')