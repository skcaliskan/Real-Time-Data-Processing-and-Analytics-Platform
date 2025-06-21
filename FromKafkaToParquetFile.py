

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark-kafka-cassandra_read") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.cassandra.connection.host", "Ip_Adress") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()



df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "Ip_Adress:9092") \
    .option("failOnDataLoss", "false")\
    .option("startingOffsets", "earliest")\
    .option("subscribe", "powerbreakdown").load()


from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType, DoubleType, MapType

schema = StructType([
    StructField("zone", StringType()),
    StructField("datetime", StringType()),
    StructField("updatedAt", StringType()),
    StructField("createdAt", StringType()),
    StructField("powerConsumptionBreakdown", MapType(StringType(), IntegerType())),
    StructField("powerProductionBreakdown", MapType(StringType(), IntegerType())),
    StructField("powerImportBreakdown", MapType(StringType(), IntegerType())),
    StructField("powerExportBreakdown", MapType(StringType(), IntegerType())),
    StructField("fossilFreePercentage", IntegerType()),
    StructField("renewablePercentage", IntegerType()),
    StructField("powerConsumptionTotal", IntegerType()),
    StructField("powerProductionTotal", IntegerType()),
    StructField("powerImportTotal", IntegerType()),
    StructField("powerExportTotal", IntegerType()),
    StructField("isEstimated", BooleanType()),
    StructField("estimationMethod", StringType()),
    StructField("GetDataDateTime", StringType()),
    StructField("id", StringType()),
    StructField("zoneid", IntegerType())
])

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

from pyspark.sql.functions import from_json, col
df = df.withColumn("value", from_json("value", schema)).select("value.*")



json_df = df.select(
    "id",
    "zoneid",
    "zone",
    "getdatadatetime",
    col("powerConsumptionBreakdown.nuclear").alias("cons_nuclear"),
    col("powerConsumptionBreakdown.geothermal").alias("cons_geothermal"),
    col("powerConsumptionBreakdown.biomass").alias("cons_biomass"),
    col("powerConsumptionBreakdown.coal").alias("cons_coal"),
    col("powerConsumptionBreakdown.wind").alias("cons_wind"),
    col("powerConsumptionBreakdown.solar").alias("cons_solar"),
    col("powerConsumptionBreakdown.hydro").alias("cons_hydro"),
    col("powerConsumptionBreakdown.gas").alias("cons_gas"),
    col("powerConsumptionBreakdown.oil").alias("cons_oil"),
    col("powerConsumptionBreakdown.unknown").alias("cons_unknown"),
    col("powerProductionBreakdown.nuclear").alias("prod_nuclear"),
    col("powerProductionBreakdown.geothermal").alias("prod_geothermal"),
    col("powerProductionBreakdown.biomass").alias("prod_biomass"),
    col("powerProductionBreakdown.coal").alias("prod_coal"),
    col("powerProductionBreakdown.wind").alias("prod_wind"),
    col("powerProductionBreakdown.solar").alias("prod_solar"),
    col("powerProductionBreakdown.hydro").alias("prod_hydro"),
    col("powerProductionBreakdown.gas").alias("prod_gas"),
    col("powerProductionBreakdown.oil").alias("prod_oil"),
    col("powerProductionBreakdown.unknown").alias("prod_unknown"),
    col("powerConsumptionTotal").alias("consumption_total"),
    col("powerProductionTotal").alias("production_total"),
    col("powerImportTotal").alias("import_total"),
    col("powerExportTotal").alias("export_total")
)

json_df=json_df.fillna(0)


json_df.write.mode("overwrite").parquet("gs://dataproc-staging-us-central1-398813168928-x88jcdn7/output.parquet")


df = spark.read.parquet("gs://dataproc-staging-us-central1-398813168928-x88jcdn7/output.parquet")
df.createOrReplaceTempView("data")
spark.sql("SELECT * FROM data").show()





