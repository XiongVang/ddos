import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split, trim}
import org.apache.spark.sql.streaming.Trigger

object LoadAccessLog {

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage: LoadAccessLog <text-file> <kafka-bootstrap-servers> <output-topic> <checkpoint>")
      System.exit(1)
    }

    val textFile = args(0)
    val kafkaBootstrapServers = args(1)
    val outputTopic = args(2)
    val checkpoint = args(3)

    val spark = SparkSession
      .builder()
      .appName("Load Access Log")
      .getOrCreate()

    val df = spark
      .readStream
      .textFile(textFile)

    val map = df
      .withColumn("key", trim(split(col("value"), " ")(0)))
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val ds = map
      .writeStream
      .trigger(Trigger.Once())
      .option("checkpointLocation", checkpoint)
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", outputTopic)
      .start()

    ds.awaitTermination()
  }
}
