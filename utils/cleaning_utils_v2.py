import yaml
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, DoubleType, DateType

#"Load schema metadata from YAML"
def load_yaml_metadata(yaml_path):
    """
    reads a yaml file returns the column metadata
    """
    with open(yaml_path, 'r') as file:
        metadata    = yaml.safe_load(file)
    return metadata['columns']

#testing function load_yaml_metadata
# if __name__ == "__main__":
#     path = r"C:\Users\ksant\PycharmProjects\PythonProject\pyspark-etl-project\metadata\schema.yaml"
#     result = load_yaml_metadata(path)
#     #print("Loaded Metadata:")
#     print(result)

#"Load column rename map"
def load_rename_map(csv_path):
    """
    Reads a CSV with old-new column mappings and returns a dictionary.
    """
    df=pd.read_csv(csv_path)
    return dict(zip(df['old'],df['new']))

# #testing function load_rename_map
# if __name__ == "__main__":
#     csv_path = r"C:\Users\ksant\PycharmProjects\PythonProject\metadata\rename_map.csv"
#     result = load_rename_map(csv_path)
#     print("Loaded Metadata:")
#     print(result)

#"Rename columns using metadata map"
def rename_columns(df,rename_dict):
    for old_col,new_col in rename_dict.items():
        df=df.withColumnRenamed(old_col,new_col)
    return df

# #testing function rename_columns
# if __name__ == "__main__":
#     csv_path = r"C:\Users\ksant\PycharmProjects\PythonProject\metadata\rename_map.csv"
#     result = rename_columns(df,rename_dict=)
#     print("Loaded Metadata:")
#     print(result)

#"Validate nulls using YAML metadata"
def validate_null(df,schema_metadata):
    """
    Drops rows with nulls in non-nullable columns.
    """
    non_nullable_cols=[col_def['name'] for col_def in schema_metadata if not col_def.get('nullable',True)]
    df=df.dropna(subset=non_nullable_cols)
    return df


#"Cast columns to correct data types"
def validate_data_types(df, schema_metadata):
    """
    Casts columns to types defined in metadata.
    """
    for col_def in schema_metadata:
        col_name = col_def['name']
        dtype = col_def['dtype']

        if dtype == 'string':
            df = df.withColumn(col_name, col(col_name).cast(StringType()))
        elif dtype == 'double':
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
        elif dtype == 'date':
            df = df.withColumn(col_name, col(col_name).cast(DateType()))
        # Add more types as needed
    return df

#"Remove Duplicates"
def remove_duplicates(df):
    """
    Removes duplicate rows from DataFrame.
    """
    return df.dropDuplicates()
