import sbt._

object Libs {
  lazy val SparkVersion = "2.4.0"
  lazy val ScalaTestVersion = "3.0.5"

  lazy val `Spark` = "org.apache.spark" %% "spark-core" % SparkVersion
  lazy val `Spark-Sql` = "org.apache.spark" %% "spark-sql" % SparkVersion
  lazy val `Spark-MlLib` = "org.apache.spark" %% "spark-mllib" % SparkVersion
  lazy val `Spark-Hive` = "org.apache.spark" %% "spark-hive" % SparkVersion
  lazy val `Scalactic` = "org.scalactic" %% "scalactic" % ScalaTestVersion
  lazy val `HbaseTestingUtil` =
    "org.apache.hbase" % "hbase-testing-util" % "1.4.9" % Test
  lazy val `GoogleGuava` = "com.google.guava" % "guava" % "12.0.1" force ()

  lazy val `Hadoop-Common-Test` = "org.apache.hadoop" % "hadoop-common" % "2.8.3" % "test" classifier ("test")
  lazy val `HadoopHdfs-Test` = "org.apache.hadoop" % "hadoop-hdfs" % "2.8.3" % "test" classifier ("test")
  lazy val `HadoopHdfs-Test1` = "org.apache.hadoop" % "hadoop-hdfs" % "2.8.3" % "test"
  lazy val `HBase` = "org.apache.hbase" % "hbase" % "2.1.0" % "test" classifier ("test")
  lazy val `ScalaTest` = "org.scalatest" %% "scalatest" % ScalaTestVersion % "test"

  lazy val `SparkTestingBase` = "com.holdenkarau" %% "spark-testing-base" % "2.4.0_0.11.0" % Test

}
