package com.learn.spark.hbase

import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

class HBaseWriterBuildable[R: ClassTag](dataFrame: DataFrame)
    extends Serializable {
  implicit def toHBaseTable(tableName: String) =
    new HBaseWriterBuilder[R](dataFrame, tableName)
}

case class HBaseWriterBuilder[R: ClassTag](
    val rdd: DataFrame,
    val tableName: String,
    val columnFamily: Option[String] = None,
    val columns: Iterable[String] = Seq.empty)
    extends Serializable {
  def toColumns(columns: String*) = {
    this
  }

  def inColumnFamily(columnFamily: String) = {
    this
  }
}

class HBaseWriter[R: ClassTag](builder: HBaseWriterBuilder[R])
    extends Serializable {
  def save() = {

    builder

  }
}

trait HBaseWriterBuilderConversions extends Serializable {

  implicit def rddToHBaseBuilder[R: ClassTag](
      rdd: DataFrame): HBaseWriterBuildable[R] =
    new HBaseWriterBuildable[R](rdd)

  implicit def hbaseBuilderToWriter[R: ClassTag](
      buildable: HBaseWriterBuilder[R]) = new HBaseWriter[R](buildable)
}
