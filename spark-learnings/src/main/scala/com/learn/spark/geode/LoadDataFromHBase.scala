package com.learn.spark.geode

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadDataFromHBase {}

class LoadDataFromHBase(config: Configuration)(
    implicit sparkSession: SparkSession) {

  private val hbaseConfig = {
    val c = HBaseConfiguration.create(config)
    c.set(TableInputFormat.INPUT_TABLE, "position")
    c
  }

  def load(): DataFrame = {
    val rdd: NewHadoopRDD[ImmutableBytesWritable, Result] =
      sparkSession.sparkContext
        .newAPIHadoopRDD(hbaseConfig,
                         classOf[TableInputFormat],
                         classOf[ImmutableBytesWritable],
                         classOf[Result])
        .asInstanceOf[NewHadoopRDD[ImmutableBytesWritable, Result]]
    //    rdd.map(f => f._2).map(result => result.getRow)

    sparkSession.emptyDataFrame
  }
}
