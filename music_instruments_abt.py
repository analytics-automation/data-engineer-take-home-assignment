from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, expr, substring, length, split, avg, lit, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession \
    .builder \
    .appName("music_instruments_abt") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

input_file = "s3://analytics-automation-take-home-assignment/input/Musical_instruments_reviews.csv"
version = "v1"
output_bucket = "s3://analytics-automation-take-home-assignment/"
glue_database = "amzn-music-reviews-curated"
output_table1 = "time_related_review_trend"
output_table2 = "product_statistics"

# Read csv file on s3 and convert it to a df
music_instruments_df = spark.read.format("csv") \
    .option("delimiter", ",") \
    .option("quote", "\"") \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .option("inferSchema", "true") \
    .load(input_file)

# Remove duplicates
music_instruments_df = music_instruments_df.dropDuplicates(['reviewerId', 'asin', 'reviewTime'])

# Create transformation1: Exploring time related trends among users reading reviews
hourly_window = Window.partitionBy('unixReviewTime')
product_hourly_window = Window.partitionBy(['unixReviewTime', 'asin'])

music_instruments_df = music_instruments_df \
    .withColumn("year", from_unixtime(music_instruments_df['unixReviewTime'], 'yyyy')) \
    .withColumn("month", from_unixtime(music_instruments_df['unixReviewTime'], 'MM')) \
    .withColumn("day", from_unixtime(music_instruments_df['unixReviewTime'], 'dd')) \
    .withColumn("hour", from_unixtime(music_instruments_df['unixReviewTime'], 'HH')) \
    .withColumn("unixReviewTime", from_unixtime(music_instruments_df['unixReviewTime'], 'yyyyMMdd HH')) \
    .withColumn("helpful1", expr("split(substring(helpful, 2, length(helpful)-2), ',')[0]")) \
    .withColumn("helpful2", expr("split(substring(helpful, 2, length(helpful)-2), ',')[1]")) \
    .withColumn("overall", music_instruments_df["overall"].cast(DoubleType()))

time_related_review_trend = music_instruments_df \
    .withColumn("helpful1", music_instruments_df["helpful1"].cast(DoubleType())) \
    .withColumn("helpful2", music_instruments_df["helpful2"].cast(DoubleType())) \
    .withColumn("average_helpful_rating", avg("helpful1").over(hourly_window)) \
    .withColumn("median_helpful_rating", expr('percentile_approx(helpful1, 0.5)').over(hourly_window)) \
    .withColumn("average_overall_rating", avg("overall").over(product_hourly_window)) \
    .withColumn("median_overall_rating", expr('percentile_approx(overall, 0.5)').over(product_hourly_window)) \
    .select("year", "month", "day", "hour", "average_helpful_rating", "median_helpful_rating", "average_overall_rating", "median_overall_rating") \
    .distinct() \
    .withColumn("version", lit(version))

# Write transformation table1 to output path
sink = glue_context.getSink(
    connection_type="s3",
    path=f"{output_bucket}/output/table_1/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["version", "year", "month", "day", "hour"],
    transformation_ctx="write_to_output_sink",
)

sink.setFormat("parquet", useGlueParquetWriter=True)
sink.setCatalogInfo(
    catalogDatabase=glue_database,
    catalogTableName=output_table1)
sink.writeFrame(time_related_review_trend)

# Create transformation2: Statistics by product
product_statistics = music_instruments_df \
    .withColumn("summary_length", expr('length(summary)').cast(IntegerType())) \
    .withColumnRenamed("asin", "product_id") \
    .groupBy("product_id") \
    .agg(avg("overall").alias("average_overall_rating"),
        expr('percentile_approx(overall, 0.5)').alias("median_overall_rating"),
        countDistinct(music_instruments_df['reviewerId']).alias("unique_reviewers"),
        avg("summary_length").alias("average_summary_length"),
        expr('percentile_approx(summary_length, 0.5)').alias("median_summary_length"))

# Write transformation table2 to output path
sink = glue_context.getSink(
    connection_type="s3",
    path=f"{output_bucket}/output/table_2/version=v1/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    transformation_ctx="write_to_output_sink",
)

sink.setFormat("parquet", useGlueParquetWriter=True)
sink.setCatalogInfo(
    catalogDatabase=glue_database,
    catalogTableName=output_table2)
sink.writeFrame(product_statistics)
