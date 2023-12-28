from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder \
    .appName("writetohive") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Write to Hive using foreachBatch
def write_to_hive(batch_df, batch_id):
    print(f"Processing batch: {batch_id}")
    
    # Select columns
    hive_df = batch_df.select("numbers", "contract_name", "banking", "bike_stands",
                               "available_bike_stands", "available_bikes", "address",
                               "status", "position", "timestamps")

    # Print the first few rows for debugging
    print("Sample data in the batch:")
    hive_df.show()

    # Write to Hive
    hive_df.write.saveAsTable(name="velib_stations.velib_stations", format="hive", mode='append')
    print("Data written to Hive successfully.")

# Create the Hive database and use it
spark.sql("CREATE DATABASE IF NOT EXISTS velib_stations")
spark.sql("USE velib_stations")

# Define the Hive table schema
hive_table_schema = """
    CREATE TABLE IF NOT EXISTS velib_stations (
        numbers INT,
        contract_name STRING,
        banking STRING,
        bike_stands INT,
        available_bike_stands INT,
        available_bikes INT,
        address STRING,
        status STRING,
        position STRUCT<lat: DOUBLE, lng: DOUBLE>,
        timestamps STRING
    )
"""

# Create the Hive table
spark.sql(hive_table_schema)

schema = StructType([
    StructField("numbers", IntegerType(), True),
    StructField("contract_name", StringType(), True),
    StructField("banking", StringType(), True),
    StructField("bike_stands", IntegerType(), True),
    StructField("available_bike_stands", IntegerType(), True),
    StructField("available_bikes", IntegerType(), True),
    StructField("address", StringType(), True),
    StructField("status", StringType(), True),
    StructField("position", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True)
    ]), True),
    StructField("timestamps", StringType(), True),
])

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "velib_stations") \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

json_df = json_df.withColumn("position", col("position").alias("position").cast("struct<lat:double, lng:double>"))

zero_bikes_df = json_df.filter(col("available_bikes") == 0)

# Write to Hive
hive_query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(write_to_hive) \
    .option("checkpointLocation", "/home/wissal/velib-project/checkpoints/new") \
    .start()

hive_query.awaitTermination()

