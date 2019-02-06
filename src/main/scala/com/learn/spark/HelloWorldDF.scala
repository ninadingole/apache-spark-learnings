package com.learn.spark

import org.apache.spark.sql.SparkSession

object HelloWorldDF {
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
    count.foreach(println(_))
  }
}
