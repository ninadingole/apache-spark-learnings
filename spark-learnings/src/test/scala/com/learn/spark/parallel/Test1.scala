package com.learn.spark.parallel

class Test1 extends HBaseSparkDFTest {
  test("Hello") {
    val rdd = spark.read
      .option("header", true)
      .option("treatEmptyValuesAsNulls", true)
      .csv("src/main/resources/datafile.csv")
    import spark.implicits._
    val dataRDD = rdd
      .repartition(2)
      .map(row => row.getString(0))
      .map(_.toUpperCase())
    dataRDD.explain(false)
    dataRDD.count() shouldBe 38
  }
}
