package Kafka_SQL
import com.google.gson.JsonDeserializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.json4s.jackson.Json
import org.apache.kafka.common.TopicPartition
import scala.util.{Try, Success, Failure}

object Streaming {


  /** Function that tests if a dataframe has a certain field
    *
    *  @param args: Array[String]   command line input, first argument determines
    *                               whether the intersection of arrays should be
    *                               computed via UDFs or via the internal solution
    *                               If args(0) = "UDF" => UDF solution
    *                               If args(0) = "internal" => internal solution
    *
    */
  def main(args: Array[String]): Unit = {

    // Start Spark session
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Streaming tests")
      .getOrCreate()


    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // Configure log4j to display log messages to console
    BasicConfigurator.configure()

    if (args.length >= 1) {

      // Define UDF to compute intersection of arrays
      spark.udf.register("UDF_INTERSECTION",
        (arr1: Seq[String], arr2: Seq[String]) => (Option(arr1), Option(arr2)) match {
          case (Some(x), Some(y)) => x.intersect(y)
          case _ => Seq()
        })



      // If the fields we want to intersect do no exist, we want to store -1 with every asin entry
      val fields_do_not_exist_query = "SELECT -1 overlap, asin FROM table"

      // If the fields buy_after_viewing and also_viewed exist, we want to compute their intersection
      var fields_exist_query: String = new String
      // args(0) determines, whether we choose our internal solution or UDFs
      if (args(0) == "UDF") {
        fields_exist_query = "SELECT asin, SIZE(UDF_INTERSECTION(related.buy_after_viewing, related.also_viewed)) overlap FROM table"
      } else {
        fields_exist_query = "SELECT asin, SIZE(ARRAY_INTERSECTION(related.buy_after_viewing, related.also_viewed)) overlap FROM table"

      }


      // Starting Kafka connection, subscribing to topic meta
      val preferredHosts = LocationStrategies.PreferConsistent
      val topics = List("meta")
      import org.apache.kafka.common.serialization.StringDeserializer
      val kafkaParams = Map(
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "spark-streaming-notes",
        "auto.offset.reset" -> "earliest"
      )

      // starting Kafka Direct stream
      val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
      val dstream = KafkaUtils.createDirectStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))


      // main logic on how to process the input
      dstream.foreachRDD(
        rdd => {
          val dataFrame = spark.read.json(rdd.map(_.value)) //converts json to DF

          dataFrame.createOrReplaceTempView("table")

          // Choose the query based on whether all relevant fields exist
          (df_has(dataFrame, "asin"), df_has(dataFrame, "related.also_viewed"), df_has(dataFrame, "related.buy_after_viewing")) match{
            case (Success(_), Success(_), Success(_)) => save_to_mysql (spark.sql (fields_exist_query)
            case (Success(_), _, _) => save_to_mysql(spark.sql(fields_do_not_exist_query))
            case _ => println("No asin database!")
          }
        })


      ssc.start()
      ssc.awaitTermination()


    } else {
      println("Missing arguments! Do you want to use UDF or internal solution?")
    }

  }


  /** Function that tests if a dataframe has a certain field
    *
    *  @param df: DataFame    dataframe which we want to test for a field
    *  @param field: String   field name whose existence we want to verify
    *
    *  @return Try[DataFrame]  In case of success, DataFrame containing the field
    */
  def df_has(df: DataFrame, field: String): Try[DataFrame] = Try(df.select(field))


  /** Helper function that saves dataframe to MySQL database
    *
    *  @param df: DataFrame     dataframe to be saved
    *
    */
  def save_to_mysql(df: DataFrame): Unit = {

    //create properties object
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "xxxxxxxxxx")
    prop.setProperty("password", "xxxxxxxxxxxxx")

    //jdbc mysql url
    val url = "xxxxxxxxxxxx"

    //destination database table
    val table = "streaming"


    //write data from spark dataframe to database
    df.write.mode("append").jdbc(url, table, prop)


  }

}


