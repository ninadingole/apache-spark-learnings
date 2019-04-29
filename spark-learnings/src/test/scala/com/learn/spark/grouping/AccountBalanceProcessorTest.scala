package com.learn.spark.grouping

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class AccountBalanceProcessorTest
    extends WordSpec
    with DataFrameSuiteBase
    with Matchers {

  import AccountBalanceProcessorTest._

  "AccountBalanceProcessor" should {
    "only select max date rows" in {
      val df = spark.createDataFrame(list, schema)
      val resultDF = new AccountBalanceProcessor().process(df)
      val rows = resultDF.collect()
      val row1 = rows(0)
      assert(row1.getAs[String](0) == "123456789")
      assert(row1.getAs[String](1) == "1488412800000")

      val row2 = rows(1)
      assert(row2.getAs[String](0) == "123456789")
      assert(row2.getAs[String](1) == "1547251200000")
    }
  }

}

object AccountBalanceProcessorTest {
  private val row1
    : Array[Any] = Array("123456789", "1488412800000") // 2-MAR-17 right
  private val row2: Array[Any] = Array("123456789", "1488326400000") // 1-MAR-17
  private val row3
    : Array[Any] = Array("123456789", "1547251200000") // 12-JAN-19 right
  private val row4
    : Array[Any] = Array("123456789", "1549843200000") // 11-FEB-19
  private val row5
    : Array[Any] = Array("987654321", "1550620800000") // 20-FEB-19 right
  private val row6
    : Array[Any] = Array("987654321", "1550361600000") // 17-FEB-19
  private val row7
    : Array[Any] = Array("987654321", "1546992000000") // 9-JAN-19 right
  private val row8
    : Array[Any] = Array("987654321", "1543622400000") // 1-DEC-18 right

  private val schema = StructType(
    Seq(StructField("acct_key", DataTypes.StringType, nullable = false),
        StructField("posn_dt", DataTypes.StringType, nullable = false)))

  private val list: java.util.List[Row] = List(
    new GenericRowWithSchema(row2, schema).asInstanceOf[Row],
    new GenericRowWithSchema(row1, schema).asInstanceOf[Row],
    new GenericRowWithSchema(row3, schema).asInstanceOf[Row],
    new GenericRowWithSchema(row4, schema).asInstanceOf[Row],
    new GenericRowWithSchema(row5, schema).asInstanceOf[Row],
    new GenericRowWithSchema(row6, schema).asInstanceOf[Row],
    new GenericRowWithSchema(row7, schema).asInstanceOf[Row],
    new GenericRowWithSchema(row8, schema).asInstanceOf[Row]
  ).asJava
}
