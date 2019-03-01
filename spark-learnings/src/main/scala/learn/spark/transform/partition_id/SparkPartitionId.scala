package learn.spark.transform.partition_id

import org.apache.spark.sql.functions.{col, spark_partition_id}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkPartitionId {
  def main(args: Array[String]): Unit = {
    val session =
      SparkSession
        .builder()
        .appName("Incrementing Number SparkDF")
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
      ) // To divide data into multiple partition, so that there will be more than one partition
      .repartition(3, col("Electricity Office (%)"))
      .transform(addStaticColumn())
    count.foreach(println(_))
  }

  def addStaticColumn()(df: DataFrame) =
    df.withColumn("Incrementing Number", spark_partition_id())

}
