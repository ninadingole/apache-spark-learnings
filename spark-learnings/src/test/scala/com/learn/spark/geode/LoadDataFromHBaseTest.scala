package com.learn.spark.geode

import com.learn.spark.testcore.HBaseSparkDFTest
import org.scalatest.{BeforeAndAfterAll, Tag}

class LoadDataFromHBaseTest extends HBaseSparkDFTest with BeforeAndAfterAll {
  test("should load data from hbase and form a Dataframe", Tag("unit")) {
    val config = HBaseSparkDFTest.util.getConfiguration
    implicit val sparkSession = spark

    val loader = new LoadDataFromHBase(config)

    val dataFrame = loader.load()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTable("position", "cf")
  }
}
