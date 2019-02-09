package com.spark.learn.parallel

class Test2 extends HBaseSparkDFTest {
  test("Hello") {
    val rdd = spark.sqlContext.read
      .option("header", true)
      .option("treatEmptyValuesAsNulls", true)
      .csv("src/main/resources/datafile.csv")
    import spark.sqlContext.implicits._
    val count = rdd
      .repartition(2)
      .map(row => row.getString(0))
      .map(_.toLowerCase())
      .count()
    count shouldBe 38
  }
}
