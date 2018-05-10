import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Test
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import scala.util.{Failure, Success}

import Intersection.SparkIntersect.transform_metadata

class TestIntersection extends FunSuite with BeforeAndAfterEach {

  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("Transformation testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }


  // Test the transformation used in the spark job
  test("transform_metadata"){
    val df = sparkSession.read.json("src/test/scala/test_data.json")


    // As we cannot testbuild against the custom SparkVersion, register UDF
    // As we tested ARRAY_INTERSECTION within the Spark Test suite,
    // this is not a problem

    sparkSession.udf.register("ARRAY_INTERSECTION",
      (arr1: Seq[String], arr2: Seq[String]) => (Option(arr1), Option(arr2)) match {
        case (Some(x), Some(y)) => x.intersect(y)
        case _ => Seq()
      })
    val output = transform_metadata(sparkSession, df)

    assert(output.isSuccess)

    val fields = List(
      StructField("asin", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("overlap", IntegerType, nullable = true)
    )
    val row1 = Row("0001048791", null, null, 0)
    val row2 = Row("0000143561", "3Pack DVD set - Ital", 12.99, 4)
    val row3 = Row("0000037214", null, 6.99, 0)
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

