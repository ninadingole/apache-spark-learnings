package com.learn.spark.hbase

import org.apache.spark.sql.DataFrame

object HBaseWriteRepository {
  implicit def writeDF(dataFrame: DataFrame): Unit = {

    dataFrame
    //      .toHBaseTable("tableName")
    //      .toColumns("", "")
    //      .inColumnFamily("cf")
    //      .save()
  }

}
