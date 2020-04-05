from pyspark.sql.functions import lit, when, col
from pyspark.sql.types import *

def update_mapping(main_df, mapping_df, mapping_key, mapping_value, replacing_value):
    """ Update column with new values from mapping table
        Args:
            main_df: dataframe to be replaced with new values
            mapping_df: dataframe to be used for mapping
            mapping_key: column in mapping_df that contains values to be used as key between mapping_df and main_df
            mapping_value: column in mapping_df that holds values to be placed in main_df
            replacing_value: column or list of columns in main_df to be replaced with new values
        Returns:
            mapped_df: dataframe with values replaced
    """
    diamond_df = main_df
    map_df = mapping_df.select(mapping_key, mapping_value).dropna(how='all')
    map_dict = {a:b for a,b in map_df.collect()}
    mapped_df = diamond_df.replace(map_dict,None,replacing_value)    
    return mapped_df


def fill_with_column(df, column_names, fill_column):
    """ Fill column in dataframe with values from another column in dataframe
        Args:
            df: dataframe to be filled
            column_names: list of columns to be filled with value from other column
            fill_column: name of column that holds values to be placed in other columns
        Returns:
            df: dataframe with values filled
    """
    if isinstance(column_names, list):
        for column in column_names:
            df = df.withColumn(column, df[fill_column])
        return df
    else:
        df = df.withColumn(column_names, df[fill_column])
        return df

def fill_with_value(df, column_names, fill_value):
    """ Fill column in dataframe with string value
        Args:
            df: dataframe to be filled
            column_names: list of columns to be filled with value from other column
            fill_value: value to be placed in other columns
        Returns:
            df: dataframe with values filled
    """
    if isinstance(column_names, list):
        for column in column_names:
            df = df.withColumn(column, lit(fill_value))
        return df
    else:
        df = df.withColumn(column_names, df[fill_column])
        return df


# def fill_nulls_with_value(df, column_names, fill_value):
#     """ Fill nulls in column in dataframe with string value
#         Args:
#             df: dataframe to be filled
#             column_names: list of columns to be filled with value from other column
#             fill_value: value to be placed in other columns
#         Returns:
#             df: dataframe with values filled
#     """
#     if isinstance(column_names, list):
#         for column in column_names:
#         df = df.withColumn(column, when(df[column].isNull(),fill_value).otherwise(df[column]))
#         return df
#     else:
#         df = df.withColumn(column_names, when(df[column_names].isNull(),fill_value).otherwise(df[column_names]))


def fill_with_nulls(df, column_names):
    """ Fill column in dataframe with string value
        Args:
            df: dataframe to be filled
            column_names: list of columns to be filled with value from other column
            fill_value: value to be placed in other columns
        Returns:
            df: dataframe with values filled
    """
    if isinstance(column_names, list):
        for column in column_names:
            df = df.withColumn(column,lit(None)).withColumn(column, col(column).astype(StringType()))
        return df
    else:
        df = df.withColumn(column_names,lit(None)).withColumn(column_names, col(column_names).astype(StringType()))
        return df

def fill_nulls_with_column(df, column_names, fill_column):
    """ Fill nulls in column in dataframe with values from another column in dataframe
        Args:
            df: dataframe to be filled
            column_names: list of columns to be filled with value from other column
            fill_value: value to be placed in other columns
        Returns:
            df: dataframe with values filled
    """
    if isinstance(column_names, list):
        for column in column_names:
            df = df.withColumn(column, when(df[column].isNull(),df[fill_column]).otherwise(df[column]))
        return df
    else:
        df = df.withColumn(column_names, when(df[column_names].isNull(),df[fill_column]).otherwise(df[column_names]))
        return df

def fill_not_nulls_with_column(df, column_names, fill_column):
    """ Fill nulls in column in dataframe with values from another column in dataframe
        Args:
            df: dataframe to be filled
            column_names: list of columns to be filled with value from other column
            fill_value: value to be placed in other columns
        Returns:
            df: dataframe with values filled
    """
    if isinstance(column_names, list):
        for column in column_names:
            df = df.withColumn(column, when(col(column).isNull(), col(column)).otherwise(col(fill_column)))
        return df
    else:
        df = df.withColumn(column_names, when(col(column_names).isNull(), col(column_names)).otherwise(col(fill_column_names)))
        return df        