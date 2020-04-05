from pyspark.sql.functions import col, lit
from pyspark.sql.types import *

def get_common_columns(expected_data, actual_data):
    """ Get list of columns that exists in two dataframes
        Args:
            expected_data
    """
    expected_columns = expected_data.columns
    migrated_columns = actual_data.columns
    common_columns = list(set(expected_columns).intersection(migrated_columns))
    return common_columns

def check_completeness(expected_data, actual_data, foreign_key, folder_name, spark, table_name):
    actual_keys = actual_data.select(foreign_key)
    expected_keys = expected_data.select(foreign_key)
    in_scope_not_migrated_keys = expected_keys.subtract(actual_keys)
    migrated_not_in_scope_keys = actual_keys.subtract(expected_keys)
    actual_keys.createOrReplaceTempView('actual_keys')
    expected_data.createOrReplaceTempView('expected_data')
    expected_migrated = spark.sql('select * from expected_data where {0} in (select {0} from actual_keys)'.format(foreign_key))
    in_scope_not_migrated_keys.write.parquet(folder_name+'_IN_SCOPE_NOT_MIGRATED', mode='overwrite')
    in_scope_not_migrated_keys.write.mode('overwrite').saveAsTable('dataplatforms.'+table_name+'_IN_SCOPE_NOT_MIGRATED')
    migrated_not_in_scope_keys.write.parquet(folder_name+'_MIGRATED_NOT_IN_SCOPE', mode='overwrite')
    migrated_not_in_scope_keys.write.mode('overwrite').saveAsTable('dataplatforms.'+table_name+'_MIGRATED_NOT_IN_SCOPE')
    expected_migrated.write.parquet(folder_name+'_MIGRATED', mode='overwrite')
    completeness_counts = spark.createDataFrame([(table_name, 'CORRECTLY_MIGRATED', expected_migrated.count()),
                                                    (table_name, 'IN_SCOPE_NOT_MIGRATED',in_scope_not_migrated_keys.count()),
                                                    (table_name, 'MIGRATED_NOT_IN_SCOPE',migrated_not_in_scope_keys.count()),
                                                    (table_name, 'OFSS_MIGRATED (extract)', actual_keys.count()),
                                                    (table_name, 'PWC_EXPECTED (transform)', expected_keys.count())
                                                        ], 
                                                ['TABLE_NAME','AREA','COUNT'])                                           
    completeness_counts.write.mode('overwrite').saveAsTable('dataplatforms.'+table_name+'_COMPLETENESS_COUNTS')                                               

def check_columns(expected_data, actual_data, common_columns, folder_name, foreign_key, spark):
    if foreign_key in common_columns:
        common_columns.remove(foreign_key)
    data_placeholder = spark.createDataFrame([('NO EXCEPTIONS','NO EXCEPTIONS','NO EXCEPTIONS','NO EXCEPTIONS')],[foreign_key,'COLUMN_NAME','data2_VALUE','data1_VALUE'])
    data_placeholder.write.parquet(folder_name, mode='overwrite')
    count = 1
    for column in common_columns:
        print("""running {}: FINE, DON'T PANIC""".format(column))
        actual = actual_data.select(foreign_key,column)
        expected = expected_data.select(foreign_key,column)
        subtracted_df = expected.subtract(actual)
        if subtracted_df.cache().count()>0:
            subtracted_df = subtracted_df.withColumn('COLUMN_NAME',lit(column))
            subtracted_df = subtracted_df.withColumn('data2_VALUE', col(column).astype(StringType())).drop(column)
            actual = actual.withColumn('data1_VALUE', col(column).astype(StringType())).drop(column)
            joined_df = subtracted_df.join(actual, on=foreign_key, how='left')
            if count==1:
                joined_df.write.parquet(folder_name, mode='overwrite')
                joined_df.limit(100).write.parquet(folder_name+'_SAMPLE', mode='overwrite')
            else:
                joined_df.write.parquet(folder_name, mode='append')
                joined_df.limit(100).write.parquet(folder_name+'_SAMPLE', mode='append')
            count+=1
            print("""written {} to file: FINE, DON'T PANIC""".format(column))
        spark.catalog.clearCache()

def summarize_exceptions(folder_name,table_name, common_columns, spark):
    exceptions_df = spark.read.parquet(folder_name)
    for column in common_columns:
        df = exceptions_df.filter(exceptions_df.COLUMN_NAME==column).limit(100)
        if common_columns.index(column)==0:
            df.write.parquet(folder_name+'_SAMPLE', mode='overwrite')
            df.write.mode('overwrite').saveAsTable('dataplatforms.'+table_name+'_SAMPLE')
        else:
            df.write.parquet(folder_name+'_SAMPLE', mode='append')
            df.write.mode('append').saveAsTable('dataplatforms.'+table_name+'_SAMPLE')
    summary_df = exceptions_df.groupBy('column_name').count()
    summary_df.write.parquet(folder_name+'_GROUPED', mode='overwrite')
    summary_df.write.mode('overwrite').saveAsTable('dataplatforms.'+table_name+'_GROUPED')

def process(expected_df, actual_df, folder_name, spark, foreign_key, columns_in_scope, table_name):
    # common_columns = get_common_columns(expected_df,actual_df)
    common_columns = columns_in_scope
    check_completeness(expected_df, actual_df, foreign_key, folder_name, spark, table_name)
    expected_migrated = spark.read.parquet(folder_name+'_MIGRATED')
    check_columns(expected_migrated,actual_df, common_columns, folder_name, foreign_key, spark)
    summarize_exceptions(folder_name,table_name, common_columns, spark)