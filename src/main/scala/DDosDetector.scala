import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, Trigger}


class MysqlSink() extends ForeachWriter[Row] {
  val driver = "com.mysql.cj.jdbc.Driver"
  var connection: Connection = _
  var statement: Statement = _

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/playground?useSSL=false", "root", "")
    statement = connection.createStatement
    true
  }

  def process(value: Row): Unit = {
    statement.executeUpdate("replace into ddos_attacks(start,end,ip,count) values("
      + "'" + value.getTimestamp(0) + "'" + "," //ip
      + "'" + value.getTimestamp(1) + "'" + "," //domain
      + "'" + value.getString(2) + "'" + "," //time
      + value.getLong(3) //count
      + ")")
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}

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

    spark.sparkContext.setLogLevel("WARN")

    val timestamp_format = udf(convertToTimeStampFormat)

    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "earliest")
      .load()

//    val fileStream = spark
//      .readStream
//      .textFile("/Users/vang4999/data-eng/phdata/access-logs/")

    val df = kafkaStream
      .selectExpr("CAST(value AS STRING)")
      .where(col("value").rlike("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})?"))
      .withColumn("ip", trim(split(col("value"), " ")(0)))
      .withColumn("event_datetime", trim(split(col("value"), " ")(3)))
      .withColumn("event_datetime", expr("substring(event_datetime,2,length(event_datetime) -1)"))
      .withColumn("event_datetime", to_timestamp(timestamp_format(col("event_datetime"))))
      .withWatermark("event_datetime", "500 milliseconds")
      .groupBy(window(col("event_datetime"), "1 minutes"), col("ip"))
      .count()
      .where(col("count") > 100)
      .select(
        col("window").getField("start").as("start"),
        col("window").getField("end").as("end"),
        col("ip"),
        col("count")
      )

    df.printSchema()

    //    val console = df
    //      .writeStream
    //      .format("console")
    //      .trigger(Trigger.ProcessingTime("30 seconds"))
    //      .outputMode("update")
    //      .start()
    //    console.awaitTermination()

    //    val outputStream = df
    //      .writeStream
    //      .queryName("ddos-detector")
    //      .format("parquet")
    //      .option("path", outputDirectory)
    //      .option("checkpointLocation", checkpoint)
    //      .trigger(Trigger.ProcessingTime("15 seconds"))
    //      .start()
    //    outputStream.awaitTermination()

    val writer = new MysqlSink();
    val mysqlSink = df
      .coalesce(5)
      .writeStream
      .foreach(writer)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("1 minutes"))
      .option("checkpointLocation", checkpoint)
      .start()
    mysqlSink.awaitTermination()

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
