package com.learn.spark.grouping

import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction
}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class UserDefinedAggregationEx {

  val tranKey = "TRAN_KEY"

  def process(df: DataFrame): DataFrame = {
    df.groupBy(tranKey)
      .agg(
        AggregateRow(col("LINE_NBR"), col("TYPE_CD"), col("LINE_DESC"))
          .as("AGG_DATA"))

  }

}

object AggregateRow extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =
    StructType(
      StructField("LINE_NBR", IntegerType)
        :: StructField("TYPE_CD", StringType)
        :: StructField("LINE_DESC", StringType) :: Nil)

  override def bufferSchema: StructType =
    StructType(
      StructField("data",
                  DataTypes.createMapType(
                    StringType,
                    DataTypes.createMapType(IntegerType, StringType))) :: Nil)

  override def dataType: DataType =
    DataTypes.createMapType(StringType, DataTypes.createArrayType(StringType))

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, Map.empty[String, Map[Int, String]])
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val map = Map(
      input.getAs[String]("input1") -> Map(input.getAs[Int]("input0") -> input
        .getAs[String]("input2")))
    buffer.update(0, map)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var parentMap =
      buffer1.getMap[String, Map[Int, String]](0)
    buffer2
      .getMap[String, Map[Int, String]](0)
      .iterator
      .foreach(ele => {
        val option = parentMap.get(ele._1)
        if (option.isEmpty) {
          parentMap = parentMap + (ele._1 -> Map.empty[Int, String])
        }
        parentMap = parentMap + (ele._1 -> (parentMap(ele._1) ++ ele._2))
      })
    buffer1.update(0, parentMap)
  }

  override def evaluate(buffer: Row): Any = {
    val map = buffer.getMap[String, Map[Int, String]](0)
    val sortedMap = map.flatMap(ele => {
      Map(ele._1 -> ele._2.toSeq.sortBy(_._1).map(_._2))
    })
    sortedMap
  }
}
