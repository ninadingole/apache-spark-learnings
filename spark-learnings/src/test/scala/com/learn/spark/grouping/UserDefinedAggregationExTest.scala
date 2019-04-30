package com.learn.spark.grouping

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class UserDefinedAggregationExTest
    extends WordSpec
    with DataFrameSuiteBase
    with Matchers {

  import UserDefinedAggregationExTest._

  "UserDefinedAggregation" should {
    "aggregate line description based on line number order" in {

      val df = spark.createDataFrame(data, schema)
      val expectedDF = new UserDefinedAggregationEx().process(df)
      expectedDF.show(false)
      val rows = expectedDF.collect()
      val row1 = rows(0)
      val map1 = row1.getMap[String, List[String]](1)
      map1("DE") shouldBe List("Line2 1", "Line2 2")

      val row2 = rows(1)
      val map2 = row2.getMap[String, List[String]](1)
      map2("CD") shouldBe List("Line 1", "Line 2", "Line 3")
      map2("PE") shouldBe List("Line1 1", "Line1 2")
    }
  }
}

object UserDefinedAggregationExTest {
  private val schema = StructType(
    StructField("TRAN_KEY", StringType)
      :: StructField("LINE_NBR", IntegerType)
      :: StructField("TYPE_CD", StringType)
      :: StructField("LINE_DESC", StringType) :: Nil)

  private val data = List(
    Row("123456789", 1, "CD", "Line 1"),
    Row("123456789", 3, "CD", "Line 3"),
    Row("123456789", 2, "CD", "Line 2"),
    Row("123456789", 1, "PE", "Line1 1"),
    Row("123456789", 2, "PE", "Line1 2"),
    Row("112233445", 1, "DE", "Line2 1"),
    Row("112233445", 2, "DE", "Line2 2")
  ).asJava
}
