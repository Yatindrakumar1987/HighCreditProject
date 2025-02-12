import sys
from lib import DataManipulation, DataReader, Utils
from pyspark.sql.functions import *

if __name__ == '__main__':

    if len(sys.argv)<2:
        print("Please spcify the environment")
        sys.exit(-1)

job_run_env = sys.argv[1]

print("Creating Spark Session")

spark = Utils.get_spark_session(job_run_env)

print("Spark Session Created")

customers_df = DataReader.read_customers(spark, job_run_env)

customers_manu_df = DataManipulation.transformation_customers(customers_df)

customers_high_credit_df = DataManipulation.high_credit_groupby(customers_manu_df)

customers_high_credit_df.show()

print("end of main")