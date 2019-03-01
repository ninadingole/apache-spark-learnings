package com.learn.spark.listener

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import learn.spark.listener.SparkListenerDemo
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SparkListenerDemoTest
    extends FunSuite
    with SharedSparkContext
    with Matchers
    with BeforeAndAfterAll {

  lazy val URLPrefix = "hdfs://localhost"
  lazy val hadoopPath = new Path(HdfsFile)
  lazy val sourceFile = new Path(
    "spark-learnings/src/main/resources/datafile.csv")
  lazy val HdfsFile = "/data/datafile.csv"
  var cluster: MiniDFSCluster = _

  test("delete file from HDFS after job ends") {
    println(s"hdfs url => $URLPrefix:${cluster.getNameNodePort}")
    SparkListenerDemo
      .runJob(s"$URLPrefix:${cluster.getNameNodePort}${HdfsFile}", true)

  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val baseDir: File = new File(
      "target/hdfs/" + classOf[SparkListenerDemoTest].getName).getAbsoluteFile()
    FileUtils.deleteDirectory(baseDir)
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    cluster = new MiniDFSCluster.Builder(conf).build()
    copyTestFileToHdfs
  }

  private def copyTestFileToHdfs(): Unit = {
    cluster.getFileSystem.copyFromLocalFile(sourceFile, hadoopPath)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    val fileExists = cluster.getFileSystem.exists(hadoopPath)
    cluster.shutdown(true)
    fileExists shouldBe false
  }
}
