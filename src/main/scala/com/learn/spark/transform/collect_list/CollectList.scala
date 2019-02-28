package com.learn.spark.transform.collect_list

import com.learn.spark.SparkBase
import org.apache.spark.sql.Row

object CollectList extends SparkBase {
  def main(args: Array[String]): Unit = {
    val session = getSession("Collect Example")
    import session.implicits._
    session.read
      .option("header", true)
      .option("delimiter", "\t")
      .csv("src/main/resources/name.basics.tsv")
      .toDF("nconst",
            "primaryName",
            "birthYear",
            "deathYear",
            "primaryProfession",
            "knownForTitles")
      .map(
        row =>
          (row.getString(0),
           row.getString(1),
           row.getString(2),
           row.getString(3),
           row.getString(4).split(",").toSeq,
           row.getString(5).split(",").toSeq))
      .toDF()
      .show()
  }

}
