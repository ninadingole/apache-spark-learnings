package com.learn.spark.parallel

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.hbase.{HBaseTestingUtility, MiniHBaseCluster}
import org.scalatest._

class HBaseSparkDFTest
    extends FunSuite
    with DataFrameSuiteBase
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    super.beforeAll()
    HBaseSparkDFTest.start()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    HBaseSparkDFTest.clear()
  }

  override protected implicit def reuseContextIfPossible: Boolean = true
}

object HBaseSparkDFTest {
  @transient var util: HBaseTestingUtility = _
  @transient var cluster: MiniHBaseCluster = _

  def start() = {
    if (util == null) {

      util = new HBaseTestingUtility()
      cluster = util.startMiniCluster(1)
    }
  }

  def clear(): Unit = {
    val admin = util.getHBaseAdmin
    admin
      .listTableNames()
      .foreach(
        table => admin.deleteTable(table)
      )
  }
}
