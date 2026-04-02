import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, lit

def run_glue_job():
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    SILVER_BUCKET_PATH = "s3://finrisk360-silver-322960458535/loans/"
    GOLD_BUCKET_PATH = "s3://finrisk360-gold-322960458535/risk_scores/"

    # Read Parquet from Silver bucket
    df = spark.read.parquet(SILVER_BUCKET_PATH)

    # Calculate sub-scores
    df_scored = df \
        .withColumn("ltv_score", 
            when(col("ltv").isNull(), 0.5)
            .when(col("ltv") < 80.0, 0.2)
            .when(col("ltv") <= 90.0, 0.5)
            .when(col("ltv") <= 95.0, 0.8)
            .otherwise(1.0)
        ) \
        .withColumn("dti_score",
            when(col("dti").isNull(), 0.5)
            .when(col("dti") < 36.0, 0.2)
            .when(col("dti") <= 43.0, 0.5)
            .when(col("dti") <= 50.0, 0.8)
            .otherwise(1.0)
        ) \
        .withColumn("rate_spread_score",
            when(col("interest_rate").isNull(), 0.5)
            .when(col("interest_rate") < 4.0, 0.2)
            .when(col("interest_rate") <= 6.0, 0.5)
            .when(col("interest_rate") <= 8.0, 0.8)
            .otherwise(1.0)
        ) \
        .withColumn("macro_stress_score", lit(0.6))

    # Calculate overall risk_score
    df_scored = df_scored.withColumn("risk_score", 
        (col("ltv_score") * 0.30) + (col("dti_score") * 0.25) + (col("rate_spread_score") * 0.25) + (col("macro_stress_score") * 0.20)
    )

    # Classify risk_tier
    df_scored = df_scored.withColumn("risk_tier",
        when(col("risk_score") < 0.3, "LOW")
        .when(col("risk_score") <= 0.6, "MEDIUM")
        .when(col("risk_score") <= 0.8, "HIGH")
        .otherwise("CRITICAL")
    )

    # Select final columns plus partition columns
    final_cols = [
        "loan_amount", "ltv", "dti", "interest_rate", "income",
        "ltv_score", "dti_score", "rate_spread_score", "macro_stress_score",
        "risk_score", "risk_tier", "processing_date", "action_taken", "loan_type",
        "year", "month", "day"
    ]
    df_final = df_scored.select([c for c in final_cols if c in df_scored.columns])

    # Write output to Gold bucket
    df_final.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(GOLD_BUCKET_PATH)

    print("Job finrisk360-silver-to-gold completed successfully.")

if __name__ == '__main__':
    run_glue_job()
