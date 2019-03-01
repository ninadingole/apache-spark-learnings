package com.spark.learn.df

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DataframeGenerator}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class DataFrameGenerator
    extends FunSuite
    with DataFrameSuiteBase
    with Checkers {
  test("test  array type generator") {
    val schema = StructType(
      List(
        StructField("name", StringType, true),
        StructField(
          "pandas",
          ArrayType(StructType(List(
            StructField("id", LongType, true),
            StructField("zip", StringType, true),
            StructField("happy", BooleanType, true),
            StructField("attributes", ArrayType(FloatType), true)
          )))
        )
      ))
    val df: Arbitrary[DataFrame] =
      DataframeGenerator.arbitraryDataFrame(spark.sqlContext, schema)
    val property = forAll(df.arbitrary) { dataframe =>
      dataframe.schema === schema && dataframe.count() >= 0
    }

    check(property)
  }
}
