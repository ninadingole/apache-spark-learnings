package com.learn.spark

import org.apache.spark.sql.SparkSession

abstract class SparkBase {
  protected def getSession(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master("local[2]")
      .getOrCreate()
  }

}
