from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_json, count
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DateType
from pyspark.sql.dataframe import DataFrame
from array import array


class DataProcessor:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.CONFIG = config
        
    def calc_stage_dfs(self, df1: DataFrame, df2: DataFrame , columns: array):
        cardViewDfCount = df1.groupBy(columns)\
            .agg(
                count(lit(1)).alias("card_views")
            )
        articleViewDfCount = df2.groupBy(columns)\
            .agg(
                count(lit(1)).alias("article_views")
            )
        finallDfTable = cardViewDfCount.join(articleViewDfCount, columns, how='full').fillna(0, subset=['article_views', 'card_views'])
        return finallDfTable

    def prepare_main_df(self, df: DataFrame):
        schema = StructType([ \
            StructField("category",StringType(),True), \
            StructField("id",StringType(),True), \
            StructField("noteType",StringType(),True), \
            StructField("orientation", StringType(), True), \
            StructField("position", StringType(), True), \
            StructField("publishTime",StringType(),True), \
            StructField("sourceDomain",StringType(),True), \
            StructField("sourceName",StringType(),True), \
            StructField("stream", StringType(), True), \
            StructField("streamType", StringType(), True), \
            StructField("subcategories", ArrayType(StringType()),True), \
            StructField("title", StringType(), True), \
            StructField("url", StringType(), True) \
        ])
        dfFromCSVJSON =  df.na.drop(subset=["ATTRIBUTES"]).select(col("TIMESTAMP"), col("EVENT_NAME"), col("MD5(USER_ID)")\
            .alias("user_id"), (from_json(col("ATTRIBUTES"), schema))\
            .alias("ATTRIBUTES")).select("TIMESTAMP","USER_ID" ,"EVENT_NAME","ATTRIBUTES.*")
        finalDf = dfFromCSVJSON.select(col("id")\
            .alias("article_id"), col("publishTime"), col("title"), col("category"), col("EVENT_NAME"), col("TIMESTAMP"), col("user_id"))\
            .withColumn("date",col("publishTime").cast(DateType()))\
            .withColumn("EVENTDATE", col("TIMESTAMP").cast(DateType()))
        cardViewDf = finalDf.filter(finalDf.EVENT_NAME.isin(['top_news_card_viewed', 'my_news_card_viewed']))
        articleViewDf = finalDf.filter(finalDf.EVENT_NAME == 'article_viewed')
        return cardViewDf, articleViewDf

    def load_to_db(self, df: DataFrame, targetTable: str, connection_uri:str):
        df\
        .write\
        .format("jdbc")\
        .option("url", connection_uri)\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", targetTable)\
        .option("user", self.CONFIG["POSTGRES_USER"])\
        .option("password", self.CONFIG["POSTGRES_PASSWORD"])\
        .option("numPartitions", 10)\
        .mode("append")\
        .save()

    def read_data_from_s3(self):
        """
        The function to retrieve data from s3 bucket and concat them into a pandas dataframe.
            
        Returns:
            (dataFrame): a concatenated dataframe which has been loaded from all tsv files
                            in a desired directory inside s3 bucket.
        """
    
        
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        self.spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")

        s3_url = "s3a://"+self.CONFIG["AWS_KEY"]+":"+self.CONFIG["AWS_SECRET"] +"@" + self.CONFIG["AWS_S3_PATH"]

        df = self.spark.read.option("delimiter", "\t").option("header",True)\
            .csv(s3_url)
        return df


