import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, current_date, year, month, dayofmonth, expr

def run_glue_job():
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    RAW_BUCKET_PATH = "s3://finrisk360-raw-322960458535/loans/"
    SILVER_BUCKET_PATH = "s3://finrisk360-silver-322960458535/loans/"

    df = spark.read.json(RAW_BUCKET_PATH)

    df_cleaned = df \
        .withColumn("loan_amount", when(col("loan_amount") == "NA", None).otherwise(col("loan_amount").cast("float"))) \
        .withColumn("ltv", when(col("ltv") == "NA", None).otherwise(col("ltv").cast("float"))) \
        .withColumn("income", when(col("income") == "NA", None).otherwise(col("income").cast("float"))) \
        .withColumn("interest_rate", when(col("interest_rate") == "NA", None).otherwise(col("interest_rate").cast("float")))

    dti_expr = when(col("dti") == "NA", None) \
        .when(col("dti") == "<20%", 20.0) \
        .when(col("dti") == "20%-<30%", 25.0) \
        .when(col("dti") == "30%-<36%", 33.0) \
        .when(col("dti") == "50%-60%", 55.0) \
        .when(col("dti") == ">60%", 60.0) \
        .otherwise(col("dti").cast("float"))

    df_cleaned = df_cleaned.withColumn("dti", dti_expr)

    # Drop records where loan_amount is null
    df_cleaned = df_cleaned.filter(col("loan_amount").isNotNull())

    # Add processing_date column
    df_cleaned = df_cleaned.withColumn("processing_date", current_date())

    # Validate action_taken and loan_type
    df_validated = df_cleaned.filter(col("action_taken").isin([1, 2, 3, 4, 5, 6, 7, 8]))
    df_validated = df_validated.filter(col("loan_type").isin([1, 2, 3, 4]))

    # Add partitions matching output expectations
    df_output = df_validated \
        .withColumn("year", year(col("processing_date")).cast("string")) \
        .withColumn("month", expr("lpad(month(processing_date), 2, '0')")) \
        .withColumn("day", expr("lpad(dayofmonth(processing_date), 2, '0')"))

    # Write output to Silver S3
    df_output.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(SILVER_BUCKET_PATH)

    print("Job finrisk360-raw-to-silver completed successfully.")

if __name__ == '__main__':
    run_glue_job()
