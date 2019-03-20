package com.learn.spark.grouping

import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

object AccountBalanceProcessor {
  val row1: Array[Any] = Array("123456789", "1550620800000") // right
  val row2: Array[Any] = Array("123456789", "1550361600000")
  val row3: Array[Any] = Array("123456789", "1547251200000") // right
  val row4: Array[Any] = Array("123456789", "1549843200000")
  val row5: Array[Any] = Array("987654321", "1550620800000") // right
  val row6: Array[Any] = Array("987654321", "1550361600000")
  val row7: Array[Any] = Array("987654321", "1546992000000") // right
  val row8: Array[Any] = Array("987654321", "1543622400000") // right

  def main(args: Array[String]): Unit = {

    val schema = StructType(
      Seq(StructField("acct_key", DataTypes.StringType, false),
          StructField("posn_dt", DataTypes.StringType, false)))

    val list: java.util.List[Row] = List(
      new GenericRowWithSchema(row2, schema).asInstanceOf[Row],
      new GenericRowWithSchema(row1, schema).asInstanceOf[Row],
      new GenericRowWithSchema(row3, schema).asInstanceOf[Row],
      new GenericRowWithSchema(row4, schema).asInstanceOf[Row],
      new GenericRowWithSchema(row5, schema).asInstanceOf[Row],
      new GenericRowWithSchema(row6, schema).asInstanceOf[Row],
      new GenericRowWithSchema(row7, schema).asInstanceOf[Row],
      new GenericRowWithSchema(row8, schema).asInstanceOf[Row]
    ).asJava

    val session = SparkSession
      .builder()
      .appName("accountBalance")
      .master("local[2]")
      .getOrCreate()

    val df = session.createDataFrame(list, schema)
    val newDf =
      df.withColumn("key", concat(col("acct_key"), lit("_"), col("posn_dt")))

    newDf
      .map(row => {
        val acctKey = row.getAs[String]("acct_key")
        val posnDt = row.getAs[String]("posn_dt").toLong
        val key = new SimpleDateFormat("yyyy-MM").format(posnDt)
        Row(row(0), row(1), key)
      })(RowEncoder.apply(newDf.schema))
      .orderBy(desc("posn_dt"))
      .groupBy(col("key"), col("acct_key"))
      .agg(first("posn_dt").as("posn_dt"))
      .selectExpr("concat(acct_key,'_',posn_dt) as key")
      .show(false)
    session.close()
  }
}
