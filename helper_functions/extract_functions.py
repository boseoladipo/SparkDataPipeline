def cache_df(*dataframes):
    """ Cache dataframes
        Args:
        *dataframes: Individual dataframes or list of dataframes to cache
    """
    for df in dataframes:
        df.cache().count()

def register_sql_table(dataframes_dict):
    """ Register tables for  use with spark SQL
        Args:
            dataframes_dict: A dictionary with dataframe names as keys and dataframe objects as values
    """
    for df_name, df in dataframes_dict.items():
            df.createOrReplaceTempView(df_name)

def write_table(df, df_name, results_folders, table_name, spark):
    """ Write tables to parquet
        Args:
            df: dataframe to write to parquet
            df_name: name of dataframe to write to parquet
            results_folders: dictionary with dataframe name as keys and folder to save dataframe as values
            table_name: name of table to be used as folder name on HDFS
            spark: SparkSession object
    """
    spark.sparkContext.setLocalProperty('spark.scheduler.pool', df_name)
    folder_name = results_folders[df_name]+table_name
    df.write.parquet(folder_name, mode='overwrite')
    spark.sparkContext.setLocalProperty('spark.scheduler.pool', None)

def extract_tables_using_data1(data1_df, combined_df, data1_combined_key, spark):
    """ Split combined data into data1 dataframe and data2 datafame using keys from data1 table
        Args:
            data1_df: source data from data1 database
            combined_df: source data from combined database
            data1_combined_key: column that is key for data1_df and combined_df
            spark: SparkSession object
        Returns:
            extracted_tables: dictionary with dataframe name as keys and extracted dataframe objects as values            
    """
    data1_keys = data1_df.select(data1_combined_key)
    cache_df(data1_keys)
    register_sql_table({'data1_keys':data1_keys, 'data1_df':data1_df, 'combined_df':combined_df})
    data1_extract = spark.sql('select * from combined_df where {0} in (select {0} from data1_keys)'.format(data1_combined_key))
    data2_extract = spark.sql('select * from combined_df a where not exists (select 1 from data1_keys b where a.{0} = b.{0})'.format(data1_combined_key))
    extracted_tables = {'data1_df':data1_extract,'data2_df':data2_extract}
    return extracted_tables


def extract_tables_using_mapping(data1_df, combined_df, mapping_df, data1_combined_key, data2_mapping_key, spark):
    """ Split combined data into data1 dataframe and data2 datafame using keys from data1 table
        Args:
            data1_df: source data from data1 database
            combined_df: source data from combined database
            mapping_df: table mapping old data2 keys to new data1 keys
            data1_combined_key: column that is key for data1_df and combined_df
            data2_mapping_key: column that contains new data1 keys from mapping_df
            spark: SparkSession object
        Returns:
            extracted_tables: dictionary with dataframe name as keys and extracted dataframe objects as values            
    """
    # data1_keys = data1_df.select(data1_combined_key)
    # data2_keys = mapping_df.select(data2_mapping_key)
    # cache_df(data1_keys, data2_keys)
    # register_sql_table({'data1_keys':data1_keys, 'data2_keys':data2_keys, 'combined_df':combined_df})
    # data1_extract = spark.sql('select * from combined_df where {0} in (select {0} from data1_keys)'.format(data1_combined_key))
    # data2_extract = spark.sql('select * from combined_df where {0} in (select {1} from data2_keys)'.format(data1_combined_key, data2_mapping_key))
    # return {'data1_df':data1_extract,'data2_df':data2_extract}
    raise Exception('Use extract_tables_using_upload function')


def extract_tables_using_upload(upload_df, combined_df, data1_combined_key, spark):
    """ Split combined data into data1 dataframe and data2 datafame using keys from data1 table
        Args:
            data1_df: source data from data1 database
            combined_df: source data from combined database
            mapping_df: table mapping old data2 keys to new data1 keys
            data1_combined_key: column that is key for data1_df and combined_df
            data2_mapping_key: column that contains new data1 keys from mapping_df
            spark: SparkSession object
        Returns:
            extracted_tables: dictionary with dataframe name as keys and extracted dataframe objects as values            
    """
    data2_keys = upload_df.select(data1_combined_key)
    cache_df(data2_keys)
    register_sql_table({'data2_keys':data2_keys, 'combined_df':combined_df})
    data2_extract = spark.sql('select * from combined_df where {0} in (select {0} from data2_keys)'.format(data1_combined_key))
    data1_extract = spark.sql('select * from combined_df a where not exists (select 1 from data2_keys b where a.{0} = b.{0})'.format(data1_combined_key))
    extracted_tables = {'data1_df':data1_extract,'data2_df':data2_extract}
    return extracted_tables