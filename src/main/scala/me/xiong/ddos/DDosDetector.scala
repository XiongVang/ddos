package me.xiong.ddos

import org.apache.spark.sql.SparkSession

object DDosDetector {
  def main(args: Array[String]) = {

    if (args.length < 4) {
      System.err.println("Usage: DDosDetector <kafka-bootstrap-servers> <input-topic> <output-topic> <checkpoint>")
      System.exit(1)
    }

    val kafkaBootstrapServers = args(0)
    val inputTopic = args(1)
    val outputTopic = args(2)
    val checkpoint = args(3)

    val spark = SparkSession
      .builder()
      .appName("DDos Detector")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .load()

    val ds = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .option("checkpointLocation", checkpoint)
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", outputTopic)
      .start()

    ds.awaitTermination()
  }
}
