package Performance_Tests
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.BasicConfigurator
import scala.util.{Failure, Success, Try}




/** Defines a SparkSQL job that compares performance of
  * using UDFs to compute intersections of arrays and
  * using an internal implementation
  * Requires custom Spark distro available at https://github.com/bastihaase/spark
  */
object SparkSQL_Performance {


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
    import spark.implicits._

    // Configure log4j to display log messages to console
    BasicConfigurator.configure()

    if (args.length >= 2) {

      // Define UDF that intersects two sequences of strings in a nullsafe way
      register_intersection_udf(spark)


      // Creates a DataFrame from json file
      val meta_df = spark.read.json("hdfs://10.0.0.10:9000/input/" + args(0))


      val new_df = transform_metadata(spark, meta_df, args(1))

      new_df match {
        case Success(df) => df.rdd.count()  // Force evaluation
        case Failure(e) => println(e)
      }

    } else
      {
        println("Missing arguments! Do you want to use UDF or Internal mode?")
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
      query = "SELECT UDF_INTERSECTION(related.buy_after_viewing, related.also_viewed) FROM meta_view"
    } else {
      query = "SELECT ARRAY_INTERSECTION(related.buy_after_viewing, related.also_viewed) FROM meta_view"
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
