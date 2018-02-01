package com.losmotylos.spark.structuredstreaming

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

object StreamingCheckpointForeachWriter {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()


  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("startingOffsets", "earliest")
    .option("subscribe", "SOURCE_TOPIC")
    .load()

  val writer = new ForeachWriter[Row] {

    override def open(partitionId: Long, version: Long): Boolean = true

    override def process(value: Row): Unit = println(value)

    override def close(errorOrNull: Throwable): Unit = println("closed")
  }


  val query = df
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .option("checkpointLocation", "checkpoint")
    .foreach(writer)
    .start()

  query.awaitTermination()
}
