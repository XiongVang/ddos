import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, Trigger}

object DDosDetector {
  def main(args: Array[String]) = {

    if (args.length < 4) {
      System.err.println("Usage: DDosDetector <kafka-bootstrap-servers> <input-topic> <output-directory> <checkpoint>")
      System.exit(1)
    }

    val kafkaBootstrapServers = args(0)
    val inputTopic = args(1)
    val outputDirectory = args(2)
    val checkpoint = args(3)

    val spark = SparkSession
      .builder()
      .appName("DDos Detector")
      .getOrCreate()

    val timestamp_format = udf(convertToTimeStampFormat)

    //    val kafkaStream = spark
    //      .readStream
    //      .format("kafka")
    //      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
    //      .option("subscribe", inputTopic)
    //      .load()

        val fileStream = spark
          .readStream
          .textFile("/Users/vang4999/data-eng/phdata/access-logs/")

//    val batchFile = spark
//      .read
//      .textFile("/Users/vang4999/data-eng/phdata/access-logs/")

    val df = fileStream
      .selectExpr("CAST(value AS STRING)")
      .where(col("value").rlike("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})?"))
      .withColumn("ip", trim(split(col("value"), " ")(0)))
      .withColumn("event_datetime", trim(split(col("value"), " ")(3)))
      .withColumn("event_datetime", expr("substring(event_datetime,2,length(event_datetime) -1)"))
      .withColumn("event_datetime", to_timestamp(timestamp_format(col("event_datetime"))))
      .withWatermark("event_datetime", "1 minutes")
      .groupBy(window(col("event_datetime"), "1 minutes"), col("ip"))
      .count()
      .where(col("count") > 100)

    val outputStream = df
      .coalesce(5)
      .writeStream
      .queryName("ddos-detector")
      .format("parquet")
      .option("path", outputDirectory)
      .option("checkpointLocation", checkpoint)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode(OutputMode.Append())
      .start()
    outputStream.awaitTermination()

//    df
//      .coalesce(5)
//      .write
//      .format("parquet")
//      .option("path", outputDirectory)
//      .save()

  }

  // converts "25/May/2015:23:11:53" to "2015-05-25 23:11:53"
  private val convertToTimeStampFormat = (datetime: String) => {
    val builder = new StringBuilder
    builder ++= extractYear(datetime)
    builder += '-'
    builder ++= extractMonthNumber(datetime)
    builder += '-'
    builder ++= extractDayOfMonth(datetime)
    builder += ' '
    builder ++= extractTime(datetime)
    builder.toString()
  }

  private def extractMonthNumber(datetime: String): String = {
    val month = datetime.split("/")(1)
    month match {
      case "January" => "01"
      case "February" => "02"
      case "March" => "03"
      case "April" => "04"
      case "May" => "05"
      case "June" => "06"
      case "July" => "07"
      case "August" => "08"
      case "September" => "09"
      case "October" => "10"
      case "November" => "11"
      case "December" => "12"
      case _ => null
    }
  }

  private def extractDayOfMonth(datetime: String): String = {
    datetime.split("/")(0)
  }

  private def extractYear(datetime: String): String = {
    datetime.split("/")(2).split(":")(0)
  }

  private def extractTime(datetime: String): String = {
    datetime.substring(datetime.indexOf(":") + 1)
  }

}
