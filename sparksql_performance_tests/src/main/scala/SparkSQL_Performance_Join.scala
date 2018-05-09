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
      spark.udf.register("UDF_INTERSECTION",
        (arr1: Seq[String], arr2: Seq[String]) => (Option(arr1), Option(arr2)) match {
          case (Some(x), Some(y)) => x.intersect(y)
          case _ => Seq()
        })


      // Creates a DataFrame from json file
      val meta_df = spark.read.json("hdfs://10.0.0.10:9000/input/" + args(0))


      // Create two tempViews so we run SQL join statements on them
      meta_df.createOrReplaceTempView("m")

      // Only join with subtabel of elements that have at least 4 element in buy_after_viewing
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


      // Save results to S3 in JSON format
      new_df.write.format("json").save("s3n://bastian-haase-insight-18b/results")


    } else
      {
        println("Missing arguments!")
      }


  }


}
