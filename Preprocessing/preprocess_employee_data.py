import sys
from pyspark.sql import SparkSession

# Local utility module
from utils.cleaning_utils import *


#"start spark session"
spark = SparkSession.builder \
    .appName("CSV_Metadata_Preprocessing") \
    .getOrCreate()

#"Define Paths"
raw_csv_path = r"C:\Users\ksant\PycharmProjects\PythonProject\pyspark-etl-project\raw_data\employee_data.csv"
refined_output_path = r"C:\Users\ksant\PycharmProjects\PythonProject\pyspark-etl-project\output\employee_data_cleaned.csv"

# Local metadata
yaml_path = r"C:\Users\ksant\PycharmProjects\PythonProject\pyspark-etl-project\metadata\schema.yaml"
rename_csv_path = r"C:\Users\ksant\PycharmProjects\PythonProject\pyspark-etl-project\metadata\rename_map.csv"

#Load Metadata
schema_metadata = load_yaml_metadata(yaml_path)
rename_dict = load_rename_map(rename_csv_path)

#Read Raw CSV
raw_df = spark.read.option("header", True).csv(raw_csv_path)

#Apply Transformations (Step-by-Step)

df_renamed = rename_columns(raw_df, rename_dict)
df_no_null = validate_null(df_renamed,schema_metadata)
df_casted  = validate_null(df_no_null,schema_metadata)
df_final   = remove_duplicates(df_casted)

#this is applicable when aws s3 bucket is there and if local dir then it will be created as folder
#df_final.write.mode("overwrite").option("header", True).csv(refined_output_path)

df_final.toPandas().to_csv(refined_output_path, index=False)
