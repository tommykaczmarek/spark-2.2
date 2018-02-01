package com.losmotylos.spark.structuredstreaming

import org.apache.spark.sql.SparkSession

object StreamingCheckpoints extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()


  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("startingOffsets", "earliest")
    .option("subscribe", "DEST")
    .load()

  val query = df
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .option("checkpointLocation", "checkpoint")
    .format("csv")
    .option("path", "out")
    .start()

  query.awaitTermination()
}

