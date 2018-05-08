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

      spark.udf.register("UDF_INTERSECTION",
        (arr1: Seq[String], arr2: Seq[String]) => if (arr1 != null && arr2 != null) arr1.intersect(arr2) else Seq(""))

      val failure_query = "SELECT -1 overlap, asin FROM table"
      var success_query: String = new String

      if (args(0) == "UDF") {
        success_query = "SELECT asin, SIZE(UDF_INTERSECTION(related.buy_after_viewing, related.also_viewed)) overlap FROM table"
      } else {
        success_query = "SELECT asin, SIZE(ARRAY_INTERSECTION(related.buy_after_viewing, related.also_viewed)) overlap FROM table"

      }


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

      val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

      val dstream = KafkaUtils.createDirectStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))



      dstream.foreachRDD(
        rdd => {
          val dataFrame = spark.read.json(rdd.map(_.value)) //converts json to DF

          dataFrame.createOrReplaceTempView("table")


          df_has(dataFrame, "asin") match {
            case Success(_) => df_has (dataFrame, "related.also_viewed") match {
              case Success(_) => df_has (dataFrame, "related.buy_after_viewing") match {
                case Success(_) => save_to_mysql (spark.sql (success_query) )
                case Failure(_) => save_to_mysql (spark.sql (failure_query) )
              }
              case Failure(_) => save_to_mysql (spark.sql (failure_query) )
            }
            case Failure(_) => println("No asin in dataframe!")
          }





        })



      ssc.start()
      ssc.awaitTermination()







    } else {
      println("Missing arguments! Do you want to use UDF or internal solution?")
    }

  }


  def df_has(df: DataFrame, field: String): Try[DataFrame] = Try(df.select(field))


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


