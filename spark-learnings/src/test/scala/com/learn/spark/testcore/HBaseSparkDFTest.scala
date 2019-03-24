package com.learn.spark.testcore

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.hbase.client.{
  ColumnFamilyDescriptorBuilder,
  TableDescriptorBuilder
}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{
  HBaseTestingUtility,
  MiniHBaseCluster,
  TableName
}
import org.scalatest._

class HBaseSparkDFTest
    extends FunSuite
    with DataFrameSuiteBase
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  override protected implicit def enableHiveSupport: Boolean = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    HBaseSparkDFTest.start()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    HBaseSparkDFTest.clear()
  }

  override protected implicit def reuseContextIfPossible: Boolean = true

  def createTable(tableName: String, colFamily: String) = {
    val connection = HBaseSparkDFTest.util.getConnection
    val admin = HBaseSparkDFTest.util.getAdmin
    admin.createTable(
      TableDescriptorBuilder
        .newBuilder(TableName.valueOf(tableName))
        .setColumnFamily(
          ColumnFamilyDescriptorBuilder
            .newBuilder(Bytes.toBytes(colFamily))
            .build()
        )
        .build())
  }
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
    val admin = util.getAdmin
    admin
      .listTableNames()
      .foreach(
        table => {
          admin.disableTable(table)
          admin.deleteTable(table)
        }
      )
  }
}
