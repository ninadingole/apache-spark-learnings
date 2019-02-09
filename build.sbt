name := "apache-spark-learnings"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint")
parallelExecution in Test := false
fork := true
unmanagedResourceDirectories in Compile += baseDirectory.value / "conf"
unmanagedResourceDirectories in Test += baseDirectory.value / "conf"

initialCommands +=
  """
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.SparkContext
  val spark = SparkSession.builder.
    master("local[*]").
    appName("Console").
    config("spark.app.id", "Console").   // To silence Metrics warning.
    getOrCreate()
  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._    // for min, max, etc.
  """

cleanupCommands +=
  """
  println("Closing the SparkSession:")
  spark.stop()
  """

excludeDependencies ++= Seq(ExclusionRule("com.google.guava", "guava"))

libraryDependencies ++= Seq(Libs.`Spark`,
  Libs.`Spark-Sql`,
  Libs.`Spark-Hive`,
  Libs.`Scalactic`,
  Libs.`ScalaTest`,
  Libs.`HbaseTestingUtil`,
  Libs.`GoogleGuava`,
  Libs.`Spark-Hive`,
  Libs.`HadoopHdfs-Test`, Libs.`HadoopHdfs-Test1`,
  Libs.`Hadoop-Common-Test`,
  Libs.`SparkTestingBase`)