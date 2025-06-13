import sys, os
from pyspark.sql import SparkSession
from utils.cleaning_utils_v2 import *
import yaml

def get_table_name(csv_filename, mapping_path):
    with open(mapping_path, 'r') as f:
        mapping = yaml.safe_load(f)
    return mapping.get(csv_filename)

if __name__ == "__main__":
    input_file = sys.argv[1]
    input_path = os.path.join("raw_data", input_file)
    output_csv_path = sys.argv[2]

    rename_map_path = "metadata/rename_map.csv"
    schema_yaml_path = "metadata/schema.yaml"
    table_mapping_path = "metadata/table_mapping.yaml"

    spark = SparkSession.builder.appName("CSVPreprocessingPipeline").getOrCreate()

    raw_df = spark.read.option("header", True).csv(input_path)
    rename_dict = load_rename_map(rename_map_path)
    schema_metadata = load_yaml_metadata(schema_yaml_path)

    df_renamed = rename_columns(raw_df, rename_dict)
    df_no_null = validate_null(df_renamed, schema_metadata)
    df_casted = validate_data_types(df_no_null, schema_metadata)
    df_final = remove_duplicates(df_casted)


    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)


    df_final.write.mode("overwrite").option("header", True).csv(output_csv_path)

    table_name = get_table_name(input_file, table_mapping_path)
    if table_name:
        df_final.write.format("delta").mode("overwrite").saveAsTable(f"bronze.{table_name}")
        print(f"✅ Loaded into Delta Table: bronze.{table_name}")
    else:
        print(f"⚠️ No Delta table mapped for {input_file}")