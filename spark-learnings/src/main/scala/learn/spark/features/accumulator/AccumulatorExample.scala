package learn.spark.features.accumulator

import org.apache.spark.sql.SparkSession

object AccumulatorExample {
  val ColumnSeq = Array(
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

  def main(args: Array[String]): Unit = {
    val session =
      SparkSession
        .builder()
        .appName("AccumulatorExample")
        .master("local[2]")
        .getOrCreate()
    val accumulator = session.sparkContext.longAccumulator("exampleAcc")
    val count = session.read
      .option("header", true)
      .option("treatEmptyValuesAsNulls", true)
      .csv("spark-learnings/src/main/resources/datafile.csv")
      .toDF(
        ColumnSeq: _*
      )
    count.foreach(_ => accumulator.add(1))
    println(s"Total Accumulator Count is ${accumulator.value}")
    session.close()
  }
}
