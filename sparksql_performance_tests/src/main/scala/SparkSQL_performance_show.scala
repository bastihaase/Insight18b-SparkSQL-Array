package Performance_Tests
import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql.SparkSession


/** Defines a SparkSQL job that compares performance of
  * using UDFs to compute intersections of arrays and
  * using an internal implementation
  * Requires custom Spark distro available at https://github.com/bastihaase/spark
  */
object SparkSQL_Performance_Show {


  /** Main spark job, expects two command line arguments
    *
    *  @param args(0) name of input file to be processed
    *  @param args(1) string indicating whether to test UDF or internal version
    *                 use "UDF" for UDF and "internal" for internal
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
      spark.udf.register("UDF_INTERSECTION",
        (arr1: Seq[String], arr2: Seq[String]) => (Option(arr1), Option(arr2)) match {
          case (Some(x), Some(y)) => x.intersect(y)
          case _ => Seq()
        })


      // Creates a DataFrame from json file
      val meta_df = spark.read.json("hdfs://10.0.0.10:9000/input/" + args(0))


      // Create a tempView so we run SQL statements
      meta_df.createOrReplaceTempView("meta_view")

      // Define the query based based on command line input
      // Either use UDF or the internal solution

      var query :String = new String


      if (args(1) == "UDF") {
        query = "SELECT UDF_INTERSECTION(related.buy_after_viewing, related.also_viewed) FROM meta_view"
      } else {
        query = "SELECT ARRAY_INTERSECTION(related.buy_after_viewing, related.also_viewed) FROM meta_view"
      }

      val new_df = spark.sql(query)

      // To force evaluation
      new_df.show

    } else
      {
        println("Missing arguments!")
      }


  }


}
