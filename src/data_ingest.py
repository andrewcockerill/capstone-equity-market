# Databricks notebook source
# Packages
import numpy as np
import re
import json
import pyspark.sql.types as T
import pyspark.sql.functions as F
from functools import reduce

# Constants
STORAGE_ACCOUNT_NAME = dbutils.secrets.get('key-vault-scope', 'storage-account-name')
STORAGE_ACCOUNT_ACCESS_KEY = dbutils.secrets.get('key-vault-scope', 'storage-account-access-key')
CAPSTONE_CONTAINER_NAME = dbutils.secrets.get('key-vault-scope', 'capstone-container-name')
CONF_NAME = f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
WASB_NAME = f"wasbs://{CAPSTONE_CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
MOUNT_POINT = f"/mnt/{CAPSTONE_CONTAINER_NAME}"
OUTPUT_DIR = "guided_capstone/daily_load/ingest_stg"

# Mount Azure storage space
if any(mount.mountPoint == MOUNT_POINT for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(MOUNT_POINT)

dbutils.fs.mount(
    source = WASB_NAME,
    mount_point = MOUNT_POINT,
    extra_configs = {CONF_NAME: STORAGE_ACCOUNT_ACCESS_KEY}
)

# Set spark configs
sc._jsc.hadoopConfiguration().set(CONF_NAME, STORAGE_ACCOUNT_ACCESS_KEY)

# Parsing/export functions
def parse_csv(line):
    """Parses one line of a financial .csv file to extract trade/quote/bad line elements."""

    # Setup indices
    record_type_pos = 2
    index_lookup = {
        "T": [0, 2, 3, 6, 4, 5, 1, 7, 8, None, None, None, None, 2],
        "Q": [0, 2, 3, 6, 4, 5, 1, None, None, 7, 8, 9, 10, 2],
        "B": [None for i in range(13)] + ['B']
    }

    # Split record
    record = line.split(",")
    record = [i.strip() for i in record]

    # Assign to tuples and return
    try:
        record_type = record[record_type_pos]
        ref = index_lookup[record_type]
        event = tuple([record[i] if i is not None else None for i in ref])
    except Exception as e:
        ref = index_lookup['B']
        event = tuple([record[i] if i is not None else None for i in ref])
    
    return event

def parse_json(line):
    """Parses one line of a financial .csv file to extract trade/quote/bad line elements."""

    record_json = json.loads(line)
    try:
        record_names = ['trade_dt','event_type','symbol','exchange','event_tm','event_seq_nb','file_tm','price','size','bid_pr','bid_size','ask_pr','ask_size']
        keys = record_json.keys()
        event = tuple([record_json[i] if i in keys else None for i in record_names] + [record_json['event_type']])
    except Exception as e:
        event = tuple([None for i in range(13)] + ['B'])
    
    return event

def text_to_df(path, file_type):
    """Converts a .csv or .json file to a Spark DataFrame."""

    # Load text file and reference parsers
    raw_data = sc.textFile(path)
    parsers = {"csv": parse_csv, "json": parse_json}

    # Parse data
    parsed_data = raw_data.map(lambda x: parsers[file_type](x))
    parsed_df = spark.createDataFrame(parsed_data)

    # Formatting and data types
    old_cols = parsed_df.columns
    new_cols = ['trade_dt','event_type','symbol','exchange','event_tm','event_seq_nb','file_tm','price','size','bid_pr','bid_size','ask_pr','ask_size', 'partition']
    new_cols_dtypes = [T.DateType(), T.StringType(), T.StringType(), T.StringType(), T.TimestampType(), T.IntegerType(), T.TimestampType(), T.DecimalType(), T.IntegerType(),
                  T.DecimalType(), T.IntegerType(), T.DecimalType(), T.IntegerType(), T.StringType()]
    for old_col, new_col, new_dtype in zip(old_cols, new_cols, new_cols_dtypes) :
        parsed_df = parsed_df.withColumnRenamed(old_col, new_col)
        parsed_df = parsed_df.withColumn(new_col, F.col(new_col).cast(new_dtype))

    parsed_df = parsed_df.select(new_cols)

    return parsed_df

def list_files(filetype="csv"):
    """List csv/json files in blob storage"""
    if filetype == "csv":
        return [i.path+"NYSE" for i in dbutils.fs.ls(WASB_NAME+"/data/csv") if not bool(re.search("\.DS",i.path))]
    elif filetype == "json":
        return [i.path+"NASDAQ" for i in dbutils.fs.ls(WASB_NAME+"/data/json") if not bool(re.search("\.DS",i.path))]
    else:
        raise ValueError("filetype must be of type csv or json")

def ingest_from_blobstore():
    """Ingest all available csv and json files in blob storage and save as a parquet file"""
    csv_files = list_files('csv')
    json_files = list_files('json')
    data_frames = []

    for f in csv_files:
        context = text_to_df(f, 'csv')
        data_frames.append(context)

    for f in json_files:
        context = text_to_df(f, 'json')
        data_frames.append(context)

    if len(data_frames) > 0:
        output = reduce(lambda x, y: x.unionAll(y), data_frames)
        output.write.partitionBy('partition').mode('overwrite').parquet(OUTPUT_DIR)

ingest_from_blobstore()
