from array import array
import findspark
findspark.init()
from pyspark.sql import SparkSession
from config_manager import ConfigManager
from data_processor import DataProcessor
from pyspark.sql.types import *
from pyspark.sql.functions import  col
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Build Spark session
    spark = SparkSession.builder.config("spark.jars", "postgresql-42.4.2.jar")\
        .config("spark.jars", "hadoop-aws-2.7.3.jar")\
        .config("spark.jars", "aws-java-sdk-1.11.30.jar")\
        .config("spark.jars", "jets3t-0.9.4.jar")\
        .appName("SAMPLE_ETL").getOrCreate()

    config_manager = ConfigManager()
    config = config_manager.get_config()

    data_path = "/home/data/"

    data_processor = DataProcessor(spark, config)

    if int(config["READ_FROM_S3"]):
        df = data_processor.read_data_from_s3()
    else:
        df = spark.read.option("delimiter", "\t").option("header", True).csv(data_path)

    [cardViewDf, articleViewDf] = data_processor.prepare_main_df(df)

    # Calculating article_performance DataFrame
    finallDfTable = data_processor.calc_stage_dfs(cardViewDf, articleViewDf, ["article_id", "date", "title", "category"])

    # Calculating CTR DataFrame
    ctrFinallDf = data_processor.calc_stage_dfs(cardViewDf, articleViewDf, ["user_id", "EVENTDATE"])
    ctrFinallDfTable = ctrFinallDf.withColumn("ctr", col("article_views") / col("card_views"))\
        .select(col("user_id"), col("EVENTDATE").alias("date"), col("ctr"))

    # Write to DB
    connection_uri = "jdbc:postgresql://{}:{}/{}".format(
        config["POSTGRES_HOST"],
        config["POSTGRES_PORT"],
        config["POSTGRES_DB"],
    )

    data_processor.load_to_db(ctrFinallDfTable, "user_performance", connection_uri)
    data_processor.load_to_db(finallDfTable, "article_performance", connection_uri)
