package Intersection
import org.apache.spark.sql.{DataFrame, SparkSession}
// For implicit conversions like converting RDDs to DataFrames




object SparkIntersect {

  /** Main spark job that saves data to MySQL, expects one command line argument
    *
    *  @param args(0): String      name of input file to be processed
    *
    */
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL to MySQL job")
      .getOrCreate()

    // For implicit conversions


    // Creates a DataFrame from json file
    val meta_df = spark.read.json("hdfs://10.0.0.10:9000/input/" + args(0))

    // Apply transformations to dataframe
    val transformed_df = transform_metadata(spark, meta_df)

    // Save to MySQL database
    save_to_mysql(transformed_df)

  }

  /** Helper function that applies the query to analyze the metadata  from dataframe
    *
    *  @param df: DataFrame    dataframe to be saved
    *  @param ss: SparkSession current SparkSession
    *  @return :DataFrame      dataframe returned from query
    */
  def transform_metadata(ss: SparkSession, df: DataFrame): DataFrame = {
    // Create a tempView so we run SQL statements
    df.createOrReplaceTempView("view")

    // Select the asin (product_id) and intersection of
    // what was bought and what was looked at
    val query = "SELECT asin, SUBSTRING(description, 0, 20) description, price, SIZE(ARRAY_INTERSECTION(related.buy_after_viewing, related.also_viewed)) overlap FROM view"
    ss.sql(query)
  }



  /** Helper function that saves dataframe to MySQL database
    *
    *  @param df: DataFrame   dataframe to be saved
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
