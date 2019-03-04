package com.learn.spark.geode

import com.iamninad.geode.model.ActorModel
import org.apache.geode.spark.connector.GeodeConnectionConf
import org.apache.spark.sql.{Encoders, SparkSession}

object LoadActorsToGeode {

  def main(args: Array[String]): Unit = {
    val conf = GeodeConnectionConf("localhost[10334]")
    val session = SparkSession
      .builder()
      .appName("spark-geode")
      .master("local[2]")
      .getOrCreate()

    val data = session.read
      .option("header", true)
      .option("delimiter", "\t")
      .csv("src/main/resources/name.basics.tsv")
      .toDF("nconst",
            "primaryName",
            "birthYear",
            "deathYear",
            "primaryProfession",
            "knownForTitles")
    val geodeRdd = data
      .map(row => {
        val actor = new ActorModel()
        actor.setId(row.getAs[String]("nconst"))
        actor.setName(row.getAs[String]("primaryName"))
        Tuple2(row.getAs[String]("nconst"), actor)
      })(Encoders.tuple(Encoders.STRING,
                        com.learn.spark.encod3r.Encoders.ActorModelEncoder))
      .rdd
    import org.apache.geode.spark.connector._

    geodeRdd.saveToGeode("actors", conf)
  }

}
