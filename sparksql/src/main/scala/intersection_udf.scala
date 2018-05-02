package UDF_Intersection
import org.apache.spark.sql.{DataFrame, SparkSession}
// For implicit conversions like converting RDDs to DataFrames




object SparkUDFIntersect {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Intersection with UDF")
      .getOrCreate()
    import spark.implicits._


    // Define UDF that intersects two sequences of strings in a nullsafe way
    spark.udf.register("INTERSECTION",
      (arr1: Seq[String], arr2: Seq[String]) => if (arr1 != null && arr2 != null) arr1.intersect(arr2) else Seq())



    // Creates a DataFrame from json file
    val meta_df = spark.read.json("hdfs://10.0.0.10:9000/input/" + args(0))

    // Look at the schema of this DataFrame for debugging.
    meta_df.printSchema()

    // Create a tempView so we run SQL statements
    meta_df.createOrReplaceTempView("meta_view")

    // Select the asin (product_id) and intersection of
    // what was bought and what was looked at
    val query = "SELECT asin, SUBSTRING(description, 0, 20) description, price, SIZE(INTERSECTION(related.buy_after_viewing, related.also_viewed)) overlap FROM meta_view"
    val new_df = spark.sql(query)

    // Save to MySQL database
    save_to_mysql(new_df)

  }

  def save_to_mysql(df: DataFrame): Unit = {

    //create properties object
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")  

    prop.setProperty("user", "xxxxx")
    prop.setProperty("password", "xxxxxxx")

    //jdbc mysql url - destination database is named "processed"
    val url = "xxxxxxx/processed"
    val table = "output"

    //write data from spark dataframe to mysql
    df.write.mode("append").jdbc(url, table, prop)


  }
}




