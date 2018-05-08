package Performance_Tests
import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql.SparkSession


/** Defines a SparkSQL job that compares performance of
  * using UDFs to compute intersections of arrays and
  * using an internal implementation
  * Requires custom Spark distro available at https://github.com/bastihaase/spark
  */
object SparkSQL_Performance_Join {


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
        (arr1: Seq[String], arr2: Seq[String]) => if (arr1 != null && arr2 != null) arr1.intersect(arr2) else Seq())


      // Creates a DataFrame from json file
      val meta_df = spark.read.json("hdfs://10.0.0.10:9000/input/" + args(0))


      // Create two tempViews so we run SQL join statements on them
      meta_df.createOrReplaceTempView("m")

      val meta2 = meta_df.filter("SIZE(related.buy_after_viewing) > 3")
      meta2.createOrReplaceTempView("m2")

      // Define the query based based on command line input
      // Either use UDF or the internal solution

      var query :String = new String


      if (args(1) == "UDF") {
        query = "SELECT m.asin FROM m LEFT JOIN m2 ON " +
          "UDF_INTERSECTION(m.related.also_viewed, m.related.buy_after_viewing) " +
          "== UDF_INTERSECTION(m2.related.also_viewed, m2.related.buy_after_viewing)"
      } else {
        query = "SELECT m.asin FROM m LEFT JOIN m2 ON " +
          "ARRAY_INTERSECTION(m.related.also_viewed, m.related.buy_after_viewing) " +
          "== ARRAY_INTERSECTION(m2.related.also_viewed, m2.related.buy_after_viewing)"
      }

      val new_df = spark.sql(query)



      new_df.write.format("json").save("s3n://bastian-haase-insight-18b/results")


    } else
      {
        println("Missing arguments!")
      }


  }


}
