from pyspark.sql.functions import *

def transformation_customers(customers_df):
    return customers_df.withColumn("ingestion_date", current_timestamp()) \
                        .distinct() \
                        .withColumnRenamed("emp_title", "member_title") \
                        .withColumnRenamed("emp_length", "membmer_tenure_in_year") \
                        .withColumnRenamed("annual_inc", "annual_income") \
                        .withColumnRenamed("tot_hi_cred_lim", "total_high_credit_limit") \
                        .withColumn("membmer_tenure_in_year", regexp_replace(col("membmer_tenure_in_year"),"[^0-9]","")) \
                        .withColumn("verification_status", when(col("verification_status") == "38000.0", None).otherwise(col("verification_status"))) \
                        .withColumn("membmer_tenure_in_year", col("membmer_tenure_in_year").cast("integer")) \
                        .withColumn("annual_income", col("annual_income").cast("double")) \
                        .fillna({"membmer_tenure_in_year":0})

def high_credit_groupby(customers_manu_df):
    return customers_manu_df.where("home_ownership is not null") \
                            .groupBy("home_ownership") \
                            .sum("total_high_credit_limit") \
                            .withColumn("sum_total_high_credit_limit", round("sum(total_high_credit_limit)", 2)) \
                            .select("home_ownership", "sum_total_high_credit_limit")
