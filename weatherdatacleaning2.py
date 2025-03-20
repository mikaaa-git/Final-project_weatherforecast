
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, count, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Load CSV file
data_path = "weather_data.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Initial counts
initial_count = df.count()
initial_duplicates_df = df.exceptAll(df.dropDuplicates())
initial_duplicates = initial_duplicates_df.count()
initial_nulls = df.select([count(when(col(c).isNull(), 1)).alias(c) for c in df.columns]).collect()

# Print duplicate rows
if initial_duplicates > 0:
    print("Duplicate rows found:")
    initial_duplicates_df.show()

# Remove duplicates
df = df.dropDuplicates()
print(f"Row count after removing duplicates: {df.count()}")

# Handling null values (drop rows where all values are null)
df = df.dropna(how='all')
print(f"Row count after dropping null rows: {df.count()}")

# Print rows that had all null values before removal
null_rows_df = df.filter(
    sum(when(col(c).isNull(), 1).otherwise(0) for c in df.columns) == len(df.columns)
)
if null_rows_df.count() > 0:
    print("Rows with all null values:")
    null_rows_df.show()

# Fill missing values with mean for numerical columns
numeric_columns = [col_name for col_name, dtype in df.dtypes if dtype in ('int', 'double', 'float')]
for col_name in numeric_columns:
    mean_value = df.select(mean(col(col_name))).collect()[0][0]
    if mean_value is not None:
        df = df.fillna({col_name: mean_value})

# Detect and remove outliers using IQR method
def remove_outliers(df, column):
    stats = df.select(
        mean(col(column)).alias("mean"),
        stddev(col(column)).alias("stddev")
    ).collect()[0]
    if stats.stddev is not None:
        lower_bound = stats.mean - 3 * stats.stddev
        upper_bound = stats.mean + 3 * stats.stddev
        return df.filter((col(column) >= lower_bound) & (col(column) <= upper_bound))
    return df

for col_name in numeric_columns:
    df = remove_outliers(df, col_name)
    print(f"Row count after removing outliers from {col_name}: {df.count()}")

# Final counts
final_count = df.count()
final_nulls = df.select([count(when(col(c).isNull(), 1)).alias(c) for c in df.columns]).collect()

# Save cleaned data to CSV
output_path = "cleaned_weather_data.csv"
df.write.csv(output_path, header=True, mode='overwrite')

# Print report
print("Data Cleaning Report:")
print(f"Initial row count: {initial_count}")
print(f"Duplicate rows removed: {initial_duplicates}")
print("Initial Null Values:")
print(initial_nulls)
print(f"Final row count: {final_count}")
print("Final Null Values:")
print(final_nulls)
print("Data cleaning complete. Saved to", output_path)

# Stop Spark session
spark.stop()
