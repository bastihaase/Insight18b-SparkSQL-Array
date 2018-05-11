package Streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.util.{Failure, Success, Try}

object Streaming {


  /** Runs a SparkStreaming job listening to Kafka topic "meta"
    * The Kafka topic streams in data; this job processes this data
    * and saves the results to MySQL. It computes the intersection of
    * two arrays present in the data.
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

    // Configure log4j to display log messages to console
    BasicConfigurator.configure()

    if (args.length >= 1) {

      register_intersection_udf(spark)


      // If the fields we want to intersect do no exist, we want to store -1 with every asin entry
      val (fields_exist_query, fields_do_not_exist_query)= get_queries(args(0))


      // start new streaming context
      val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

      // Populate it with a Kafka stream
      val dstream = start_Kafka_stream(ssc)


      // main logic on how to process the input
      dstream.foreachRDD(
        rdd => {
          val dataFrame = spark.read.json(rdd.map(_.value)) //converts json to DF

          // Application of transformation
          val results = process_batch(spark, dataFrame, fields_exist_query, fields_do_not_exist_query)

          // If successful, save to MySQL
          results match {
            case Success(df) => save_to_mysql(df)
            case Failure(e) => println(e)
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



  /** Registering UDF to compute intersection of array
    *
    *  @param ss : SparkSession          SparkSession where UDF will be registered at
    *
    */
  def register_intersection_udf(ss: SparkSession): Unit = {
    ss.udf.register("UDF_INTERSECTION",
      (arr1: Seq[String], arr2: Seq[String]) => (Option(arr1), Option(arr2)) match {
        case (Some(x), Some(y)) => x.intersect(y)
        case _ => Seq()
      })
  }


  /** Main transformation for each batch. It computes the intersection of also_viewed and buy_after viewing
    * if those fields exist
    *
    *  @param spark: SparkSession    ambient spark session
    *  @param df: DataFrame          DataFrame containing current batch
    *  @param fields_exist_query: String If the dataframe contains all fields, compute this query
    *  @param fields_do_not_exist_query: String Alternative query if not all fields exist
    *
    *  @return Try[DataFrame]  In case of success, DataFrame containing the transformed DataFrame
    */
  def process_batch(spark: SparkSession, df: DataFrame, fields_exist_query: String, fields_do_not_exist_query: String): Try[DataFrame] = {

    df.createOrReplaceTempView("table")

    // See if all relevant fields are present in current batch
    val also_viewed = df_has(df, "related.also_viewed")
    val buy_after_viewing = df_has(df, "related.buy_after_viewing")


    // Choose the query based on whether all relevant fields exist
    (also_viewed, buy_after_viewing) match{
    case (Success(_), Success(_)) => Try(spark.sql (fields_exist_query))
    case _ => Try(spark.sql(fields_do_not_exist_query))
    }
  }


  /** Get the queries depending on whether we want to use UDF or internal intersection
    *
    *  @param mode: String    "UDF" for UDF version, otherwise internal
    *
    *  @return (String, String)  queries to run: (dataframe has fields, dataframe does not have fields)
    */
  def get_queries(mode: String): (String, String) = {
    // If the fields we want to intersect do no exist, we want to store -1 with every asin entry
    val fields_do_not_exist_query = "SELECT -1 overlap, asin FROM table"

    // If the fields buy_after_viewing and also_viewed exist, we want to compute their intersection
    val fields_exist_query = mode match {
      case "UDF" => "SELECT asin, SIZE(UDF_INTERSECTION(related.buy_after_viewing, related.also_viewed)) overlap FROM table"
      case _ => "SELECT asin, SIZE(ARRAY_INTERSECTION(related.buy_after_viewing, related.also_viewed)) overlap FROM table"
    }

    (fields_exist_query, fields_do_not_exist_query)
  }



  /** Creating DStream that listens to Kafka topic meta
    *
    *  @param ssc: StreamingContext    dataframe which we want to test for a field
    *
    *  @return InputDStream[ConsumerRecord[String, String ] ]   DStream listening to Kafka topic meta
    */
  def start_Kafka_stream(ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    // Setup Kafka configuration and subscribe to topic meta
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

    // Create the Kafka stream
    KafkaUtils.createDirectStream[String, String](
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

  }

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


