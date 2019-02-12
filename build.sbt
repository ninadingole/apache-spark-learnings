name := "apache-spark-learnings"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint")
parallelExecution in Test := false
fork := true
unmanagedResourceDirectories in Compile += baseDirectory.value / "conf"
unmanagedResourceDirectories in Test += baseDirectory.value / "conf"

excludeDependencies ++= Seq(ExclusionRule("com.google.guava", "guava"))

libraryDependencies ++= Seq(Libs.`Spark`,
  Libs.`Spark-Sql`,
  Libs.`Spark-Hive`,
  Libs.`Spark-MlLib`,
  Libs.`Scalactic`,
  Libs.`ScalaTest`,
  Libs.`HbaseTestingUtil`,
  Libs.`GoogleGuava`,
  Libs.`Spark-Hive`,
  Libs.`HadoopHdfs-Test`, Libs.`HadoopHdfs-Test1`,
  Libs.`Hadoop-Common-Test`,
  Libs.`SparkTestingBase`)