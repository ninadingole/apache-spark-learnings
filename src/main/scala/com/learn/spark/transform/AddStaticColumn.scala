package com.learn.spark.transform

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

object AddStaticColumn {

  def main(args: Array[String]): Unit = {
    val session =
      SparkSession
        .builder()
        .appName("Hello World SparkDF")
        .master("local[2]")
        .getOrCreate()
    val count = session.read
      .option("header", true)
      .option("treatEmptyValuesAsNulls", true)
      .csv("src/main/resources/datafile.csv")
      .toDF(
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
      .transform(addStaticColumn())
    count.foreach(println(_))
  }

  def addStaticColumn()(df: DataFrame) =
    df.withColumn("CustomCol", lit("N"))
}
