package learn.spark

import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Hello World").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val file = sc.textFile("README.md")
    file.foreach(println)
  }
}
