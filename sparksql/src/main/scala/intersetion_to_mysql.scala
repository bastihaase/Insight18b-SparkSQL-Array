package Intersection
import org.apache.spark.sql.{DataFrame, SparkSession}
// For implicit conversions like converting RDDs to DataFrames




object SparkIntersect {

  /** Main spark job that saves data to MySQL, expects one command line argument
    *
    *  @param args(0) name of input file to be processed
    *
    */
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL to MySQL job")
      .getOrCreate()
    import spark.implicits._


    // Creates a DataFrame from json file
    val meta_df = spark.read.json("hdfs://10.0.0.10:9000/input/" + args(0))

    // Look at the schema of this DataFrame for debugging.
    meta_df.printSchema()

    // Create a tempView so we run SQL statements
    meta_df.createOrReplaceTempView("meta_view")

    // Select the asin (product_id) and intersection of
    // what was bought and what was looked at
    val query = "SELECT asin, SUBSTRING(description, 0, 20) description, price, SIZE(ARRAY_INTERSECTION(related.buy_after_viewing, related.also_viewed)) overlap FROM meta_view"
    val new_df = spark.sql(query)

    // Save to MySQL database
    save_to_mysql(new_df)

  }


  /** Helper function that saves dataframe to MySQL database
    *
    *  @param df dataframe to be saved
    *
    */
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
