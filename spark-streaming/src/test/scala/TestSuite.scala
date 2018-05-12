import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Test
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}
import Streaming._

class TestStreaming extends FunSuite with BeforeAndAfterEach {

  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("Transformation testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }


  // Test the function that registers a UDF
  test("Udf_Registration"){
    val df = sparkSession.read.json("src/test/scala/test_data.json")

    Streaming.register_intersection_udf(sparkSession)

    df.createOrReplaceTempView("m")

    val res = Try(sparkSession.sql("SELECT UDF_INTERSECTION(related.also_viewed, related.buy_after_viewing) FROM m"))

    assert(res.isSuccess)

  }


  // Test the transformations used in the spark job
  // General note: We will only test UDF, as we have already tested
  // internal solution within Spark's testsuite. Here, it is
  // about testing the transformation

  test("Process_Batch"){
    val df = sparkSession.read.json("src/test/scala/test_data.json")


    // As we cannot testbuild against the custom SparkVersion, register UDF
    // As we tested ARRAY_INTERSECTION within the Spark Test suite,
    // this is not a problem

    Streaming.register_intersection_udf(sparkSession)

    val fields_exist_query = "SELECT asin, SIZE(UDF_INTERSECTION(related.buy_after_viewing, related.also_viewed)) overlap FROM table"
    val fields_do_not_exist_query = "SELECT -1 overlap, asin FROM table"
    val output = Streaming.process_batch(sparkSession, df, fields_exist_query, fields_do_not_exist_query)


    assert(output.isSuccess)

    val fields = List(
      StructField("overlap",ArrayType(StringType, true), nullable = true)
    )
    val row1 = Row(null)
    val row2 = Row(Array[String]("B0036FO6SI", "B000KL8ODE", "000014357X", "B0037718RC"))
    val row3 = Row(null)
    val result = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(Seq(row1, row2, row3)), StructType(fields))

    // Unfortunately there is no implemented equal function for datafranes
    // There are some external packages that provide this nicely
    // But, for these small tests, testing equality of show suffices
    assert(output.get.show() == result.show())

  }

  test("get_query"){

    val (field_exist_query, field_not_exist_query) = Streaming.get_queries("UDF")
    assert(field_exist_query == "SELECT asin, SIZE(UDF_INTERSECTION(related.buy_after_viewing, related.also_viewed)) overlap FROM table")
    assert(field_not_exist_query == "SELECT -1 overlap, asin FROM table")

    val (field_exist_query2, field_not_exist_query2) = Streaming.get_queries("internal")
    assert(field_exist_query2 == "SELECT asin, SIZE(ARRAY_INTERSECTION(related.buy_after_viewing, related.also_viewed)) overlap FROM table")
    assert(field_not_exist_query2 == "SELECT -1 overlap, asin FROM table")
  }



  override def afterEach() {
    sparkSession.stop()
  }
}
