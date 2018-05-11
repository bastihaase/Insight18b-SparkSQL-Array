package Performance_Tests
import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}


/** Defines a SparkSQL job that compares performance of
  * using UDFs to compute intersections of arrays and
  * using an internal implementation
  * Requires custom Spark distro available at https://github.com/bastihaase/spark
  */
object SparkSQL_Performance_Show {


  /** Main spark job, expects two command line arguments
    *
    *  @param args expects two elements: 1) name of input file to be processed
    *                                    2) UDF or internal depending on which mode should be used
    */
  def main(args: Array[String]) {

    // Start Spark session
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Performance tests")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames

    // Configure log4j to display log messages to console
    BasicConfigurator.configure()



    if (args.length >= 2) {

      // Define UDF that intersects two sequences of strings in a nullsafe way
      register_intersection_udf(spark)


      // Creates a DataFrame from json file
      val meta_df = spark.read.json("hdfs://10.0.0.10:9000/input/" + args(0))

      // Apply transformation
      val new_df = transform_metadata(spark, meta_df, args(1))

      new_df match {
        case Success(df) => df.show  // an action to make sure execution is executed
        case Failure(e) => println(e)
      }


    } else
      {
        println("Missing arguments!")
      }


  }

  /** Helper function that applies the query to analyze the metadata  from dataframe
    *
    *  @param ss: SparkSession  ambient spark session
    *  @param df: DataFrame   dataframe to be saved
    *  @param mode : String         "UDF" if user wants to use UDF intersection, else internal intersection is used
    *
    *  @return :DataFrame     dataframe returned from query
    */
  def transform_metadata(ss: SparkSession, df: DataFrame, mode: String): Try[DataFrame] = {

    // Create a tempView so we run SQL statements
    df.createOrReplaceTempView("view")

    var query :String = new String

    // Define query based on mode
    if (mode == "UDF") {
      query = "SELECT UDF_INTERSECTION(related.buy_after_viewing, related.also_viewed) overlap FROM view"
    } else {
      query = "SELECT ARRAY_INTERSECTION(related.buy_after_viewing, related.also_viewed) overlap FROM view"
    }

    Try(ss.sql(query))
  }

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


}
