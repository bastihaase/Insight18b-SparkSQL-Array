package Performance_Tests
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.BasicConfigurator
// For implicit conversions like converting RDDs to DataFrames


object SparkSQL_Performance {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Performance tests")
      .getOrCreate()
    import spark.implicits._

    BasicConfigurator.configure()


    if (args.length >= 2) {
      // Define UDF that intersects two sequences of strings in a nullsafe way
      spark.udf.register("UDF_INTERSECTION",
        (arr1: Seq[String], arr2: Seq[String]) => if (arr1 != null && arr2 != null) arr1.intersect(arr2) else Seq())


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
        query = "SELECT array_intersection(related.buy_after_viewing, related.also_viewed) FROM meta_view"

      }

      val new_df = spark.sql(query)

      // To force evaluation
      new_df.rdd.count

    } else
      {
        println("Missing arguments!")
      }


  }


}









