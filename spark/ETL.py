from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


def new(spark_session):
    # path = "data"  # For using in Pycharm
    path = "spark/data"    # For using in VSCode
    
    raw_df = spark.read.json(path, multiLine="true")
    # raw_df.printSchema()
    raw_df.show(1, True)

    body_text_df = raw_df.select("paper_id", "body_text")
    # body_text_df.printSchema()
    # body_text_df.show(5, truncate=True)

    body_text_item_df = raw_df \
        .select("paper_id", explode("body_text.text").alias("body_text_item"))
    # body_text_item_df.printSchema()
    body_text_item_df.show(20, truncate=True)


if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("COVID-19 Effective Drugs") \
        .master("local[3]") \
        .getOrCreate()
    # $example off:init_session$
    new(spark)
