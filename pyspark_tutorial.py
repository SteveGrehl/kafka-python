#!/opt/homebrew/bin/python3
from pyspark.sql import SparkSession
import logging

def main():
    logging.basicConfig(level=logging.ERROR)
    logger = logging.getLogger("main")
    logger.setLevel(logging.DEBUG)
    logger.info("Hello :)")

    spark: SparkSession = SparkSession.builder\
                .master("local[*]")\
                .appName("wikimedia.recentchange.consumer")\
                .getOrCreate()

    # spark.sparkContext.setLogLevel('CRITICAL')

    kafkaStreamDF = spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", "localhost:9092")\
            .option("subscribe", "wikimedia.recentchange")\
            .option("includeHeaders", "true")\
            .option("failOnDataLoss", False)\
            .option("startingOffsets", "latest")\
            .load()

    df = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json")

    logger.info(df)


if __name__ == "__main__":
    main()
    