# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. IMPORTS

# COMMAND ----------

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, DoubleType, TimestampType, DateType
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. PARAMETERS

# COMMAND ----------

# Blob storage information
RAW_STORAGE_ACC = "robertodevoteamsaweblob"
BRONZE_STORAGE_ACC = "robertodevoteamsawe"

RAW_CONTAINER = "raw"
BRONZE_CONTAINER = "budgetthuis"
STORAGE_ACCOUNT_KEY = "YWv1DkRCCzK18RGy9OBDExmsn11/zzho5ib3PBHEsxIJDN5UY0YjC0fi0rgS4JEEY1IBQwFIe55B+AStKB/uRQ=="

# File
TOP_HIERARCHY_LEVEL = "ns5:P4MeteringPoint"
REMOVE_FROM_COLUMN_NAMES = "ns5:"
WRITE_TO_DELTA = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. LOAD BLOB

# COMMAND ----------

def load_blob(storage_account: str, container: str, key: str) -> None:
    """Mounts a blob storage to databricks."""
    try:
        dbutils.fs.mount(
            source = f"wasbs://{container}@{storage_account}.blob.core.windows.net/",
            mount_point = f"/mnt/{storage_account}/{container}",
            extra_configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net": key},
        )
    except Exception as e:
        print("Already Mounted")

# COMMAND ----------

load_blob(RAW_STORAGE_ACC, RAW_CONTAINER, STORAGE_ACCOUNT_KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. XML

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  **4.0 TRANSFORMATIONS FUNCTIONS**

# COMMAND ----------

def find_other_columns(df: DataFrame, column_name: str) -> list:
    """Returns a list of column names that are not the column_name."""
    other_columns = [
        other_column 
        for other_column in df.columns 
        if other_column != column_name
    ]
    return other_columns


def flatten_column(df: DataFrame, column_name: str) -> DataFrame:
    """Unnests a json-like column of a spark dataframe."""
    # Get a list of the nested column names
    nested_columns = df.select(col(f"{column_name}.*")).columns
    unpack_nested_columns = [
        col(f"{column_name}.{nested_column}").alias(f"{column_name}_{nested_column}") 
        for nested_column in nested_columns
    ]

    # Unnest the columns
    df = df.select(
        *find_other_columns(df, column_name),
        *unpack_nested_columns,
    )
    return df


def explode_array_column(df: DataFrame, column_name: str) -> DataFrame:
    """Explodes an array-column of a spark dataframe."""
    # Explode the array column
    return df.select(
        *find_other_columns(df, column_name),
        explode(column_name).alias(column_name),
    )


def flatten_dataframe(df: DataFrame) -> DataFrame:
    """Explodes all array types and flattens all struct types of a spark dataframe."""
    columns_types = dict(df.dtypes).items()
    for column, data_type in columns_types:
        # If a column is an array-type, explode it
        if data_type.startswith("array"):
            df = explode_array_column(df, column)
            return flatten_dataframe(df)
        # If a column is nested (struct-type), flatten it
        elif data_type.startswith("struct"):
            df = flatten_column(df, column)
            return flatten_dataframe(df)
    return df


def remove_column_name_string(df: DataFrame, removed_string: str) -> DataFrame:
    """Removes a string from names of normal/nested columns of a dataframe."""
    data_types = dict(df.dtypes)
    struct = "struct"

    for column in df.columns:
        data_type = data_types[column]

        # If the data type is 'struct' (nested column)
        if data_type.startswith(struct):

            # Get nested schema of a column & remove the string from the names
            schema = df.select(col(f"{column}.*")).schema
            schema = schema.simpleString().replace(removed_string, "")

  
            # Cast the nested schema of the column
            df = df.select(
                *find_other_columns(df, column),
                col(column).cast(schema),
            )

        # Finally, remove the string from the top level column name
        df = df.withColumnRenamed(column, column.replace(removed_string, ""))

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **4.1 READ XML FILE**

# COMMAND ----------

file_type = "xml"

# 1. Read XML file
df = (
    spark
    .read
    .format(file_type)
    .option("rowTag", TOP_HIERARCHY_LEVEL)
    .load(f"/mnt/{RAW_STORAGE_ACC}/{RAW_CONTAINER}/*")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **4.2 TRANSFORM & WRITE**

# COMMAND ----------

# 2. Rename the columns and flatten the dataframe
df = (
    remove_column_name_string(df, REMOVE_FROM_COLUMN_NAMES)
    .transform(flatten_dataframe)
)
display(df)

# 3. Write to delta
if WRITE_TO_DELTA:
    (
        df
        .write
        .mode("append")
        .format("delta")
        .save(f"abfss://{BRONZE_CONTAINER}@{BRONZE_STORAGE_ACC}.dfs.core.windows.net/")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##5 CHECK IF IT WAS WRITTEN CORRECTLY##

# COMMAND ----------


df2 = (
    spark
    .read
    .format("delta")
    .load(f"abfss://{BRONZE_CONTAINER}@{BRONZE_STORAGE_ACC}.dfs.core.windows.net/")
)
display(df2)

# COMMAND ----------


