package com.losmotylos.spark.structuredstreaming

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{Encoders, SparkSession}

/**
  * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  */
object StreamingAppendMode extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val stream = new MemoryStream[(Timestamp, String)](0, spark.sqlContext)(Encoders.product)

  val df = stream.toDF().select($"_1".alias("t"), $"_2".alias("value"))
  df.printSchema()

  val foo = df
    .withWatermark("t", "10 days")
    .groupBy(
      org.apache.spark.sql.functions.window($"t", "10 minutes", "5 minutes") as "window",
      $"value")
    .count()


  val outputStream = foo
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
  outputStream.processAllAvailable()

  stream.addData(
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 18, 12, 7, 0)), "dog"),
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 18, 12, 8, 0)), "owl")
  );
  outputStream.processAllAvailable()

  stream.addData(
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 18, 12, 14, 0)), "dog"),
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 18, 12, 9, 0)), "cat")
  );
  outputStream.processAllAvailable()
  stream.addData(
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 18, 12, 8, 0)), "dog"),
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 18, 12, 13, 0)), "owl"),
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 18, 12, 15, 0)), "cat"),
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 18, 12, 21, 0)), "owl")
  );
  outputStream.processAllAvailable()
  stream.addData(
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 18, 12, 4, 0)), "donkey"),
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 18, 12, 17, 0)), "owl"),
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 18, 12, 26, 0)), "owl"),
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 30, 12, 9, 0)), "cat")
  );
  outputStream.processAllAvailable()
  stream.addData(
    (Timestamp.valueOf(LocalDateTime.of(2017, 1, 18, 12, 9, 0)), "cat")
  );
  outputStream.processAllAvailable()

  println("THE END")
  System.in.read()
}
