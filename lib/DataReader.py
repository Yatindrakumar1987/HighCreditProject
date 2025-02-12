from lib import ConfigReader

# creating customer dataframe
def read_customers(spark,env):
    conf = ConfigReader.get_app_config(env)
    customers_file_path = conf["customers.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(customers_file_path)
