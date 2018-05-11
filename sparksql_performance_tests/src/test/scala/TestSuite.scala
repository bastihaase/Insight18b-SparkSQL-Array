import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Test
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}
import Performance_Tests._

class TestPerformance extends FunSuite with BeforeAndAfterEach {

  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("Transformation testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }


  // Test the function that registers a UDF
  test("Does UDF work?"){
    val df = sparkSession.read.json("src/test/scala/test_data.json")

    SparkSQL_Performance.register_intersection_udf(sparkSession)

    df.createOrReplaceTempView("m")

    val res = Try(sparkSession.sql("SELECT UDF_INTERSECTION(related.also_viewed, related.buy_after_viewing) FROM m"))

    assert(res.isSuccess)

  }


  // Test the transformations used in the spark job
  // General note: We will only test UDF, as we have already tested
  // internal solution within Spark's testsuite. Here, it is
  // about testing the transformation

  test("Performance transform_metadata"){
    val df = sparkSession.read.json("src/test/scala/test_data.json")


    // As we cannot testbuild against the custom SparkVersion, register UDF
    // As we tested ARRAY_INTERSECTION within the Spark Test suite,
    // this is not a problem

    SparkSQL_Performance.register_intersection_udf(sparkSession)
    val output = SparkSQL_Performance.transform_metadata(sparkSession, df, "UDF")


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

  test("Performance_show transform_metadata"){
    val df = sparkSession.read.json("src/test/scala/test_data.json")


    // As we cannot testbuild against the custom SparkVersion, register UDF
    // As we tested ARRAY_INTERSECTION within the Spark Test suite,
    // this is not a problem

    SparkSQL_Performance_Show.register_intersection_udf(sparkSession)
    val output = SparkSQL_Performance_Show.transform_metadata(sparkSession, df, "UDF")


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

  test("Performance_Group_By transform_metadata"){
    val df = sparkSession.read.json("src/test/scala/test_data.json")


    // As we cannot testbuild against the custom SparkVersion, register UDF
    // As we tested ARRAY_INTERSECTION within the Spark Test suite,
    // this is not a problem

    SparkSQL_Performance_Group_By.register_intersection_udf(sparkSession)
    val output = SparkSQL_Performance_Group_By.transform_metadata(sparkSession, df, "UDF")


    assert(output.isSuccess)

    val fields = List(
      StructField("count(asin)",IntegerType, nullable = true)
    )
    val row1 = Row(2)
    val row2 = Row(1)
    val result = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(Seq(row1, row2)), StructType(fields))

    // Unfortunately there is no implemented equal function for datafranes
    // There are some external packages that provide this nicely
    // But, for these small tests, testing equality of show suffices
    assert(output.get.show() == result.show())

  }

  test("Performance_Concat transform_metadata"){
    val df = sparkSession.read.json("src/test/scala/test_data.json")


    // As we cannot testbuild against the custom SparkVersion, register UDF
    // As we tested ARRAY_INTERSECTION within the Spark Test suite,
    // this is not a problem

    SparkSQL_Performance_Concat.register_concat_udf(sparkSession)
    val output = SparkSQL_Performance_Concat.transform_metadata(sparkSession, df, "UDF")


    assert(output.isSuccess)

    val fields = List(
      StructField("merge", ArrayType(StringType, true), nullable = true)
    )
    val row1 = Row(null)
    val row2 = Row(Array[String]("B0036FO6SI", "B000KL8ODE", "000014357X", "B0037718RC", "B0036FO6SI", "B000KL8ODE", "000014357X", "B0037718RC", "B002I5GNVU", "B000RBU4BM"))
    val row3 = Row(null)
    val result = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(Seq(row1, row2, row3)), StructType(fields))

    // Unfortunately there is no implemented equal function for datafranes
    // There are some external packages that provide this nicely
    // But, for these small tests, testing equality of show suffices
    assert(output.get.show() == result.show())

  }


  test("Performance_Join transform_metadata"){
    val df = sparkSession.read.json("src/test/scala/test_data.json")


    // As we cannot testbuild against the custom SparkVersion, register UDF
    // As we tested ARRAY_INTERSECTION within the Spark Test suite,
    // this is not a problem

    SparkSQL_Performance_Join.register_intersection_udf(sparkSession)
    val output = SparkSQL_Performance_Join.transform_metadata(sparkSession, df, "UDF")


    assert(output.isSuccess)

    val fields = List(
      StructField("asin", StringType, nullable = true)
    )
    val row1 = Row("0001048791")
    val row2 = Row("0000143561")
    val row3 = Row("0000037214")
    val result = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(Seq(row1, row2, row3)), StructType(fields))

    // Unfortunately there is no implemented equal function for datafranes
    // There are some external packages that provide this nicely
    // But, for these small tests, testing equality of show suffices
    assert(output.get.show() == result.show())

  }


  override def afterEach() {
    sparkSession.stop()
  }
}

