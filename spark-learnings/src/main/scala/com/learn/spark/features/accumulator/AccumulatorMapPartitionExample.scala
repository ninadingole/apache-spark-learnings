package com.learn.spark.features.accumulator

import java.util.logging.Logger

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AccumulatorMapPartitionExample {

  val ColumnSeq = Array(
    "States",
    "District",
    "Electricity office",
    "NA",
    "GP Office",
    "Post Office",
    "Franchise",
    "Total",
    "Electricity Office (%)",
    "NA (%)",
    "GP Office (%)",
    "Post Office (%)",
    "Franchise (%)"
  )

  def main(args: Array[String]): Unit = {
    @transient val logger =
      Logger.getLogger(AccumulatorMapPartitionExample.getClass.getName)
    val session =
      SparkSession
        .builder()
        .appName("AccumulatorExample")
        .master("local[2]")
        .getOrCreate()
    val accumulator = session.sparkContext.longAccumulator("exampleAcc")
    val count = session.read
      .option("header", true)
      .option("treatEmptyValuesAsNulls", true)
      .csv("spark-learnings/src/main/resources/datafile.csv")
      .repartition(20)
      .toDF(
        ColumnSeq: _*
      )
      .withColumn("partition_number", spark_partition_id())
    count

    println(s"Total Accumulator Count is ${accumulator.value}")
    session.close()
  }
}
