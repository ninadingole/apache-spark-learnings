import sbt._

object Libs {
  lazy val SparkVersion = "2.4.0"
  lazy val ScalaTestVersion = "3.0.5"

  lazy val `Spark` = "org.apache.spark" %% "spark-core" % SparkVersion
  lazy val `Spark-Sql` = "org.apache.spark" %% "spark-sql" % SparkVersion
  lazy val `Scalactic` = "org.scalactic" %% "scalactic" % ScalaTestVersion
  lazy val `ScalaTest` = "org.scalatest" %% "scalatest" % ScalaTestVersion % "test"
}
