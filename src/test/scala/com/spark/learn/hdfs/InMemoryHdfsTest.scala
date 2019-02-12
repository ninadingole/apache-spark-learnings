package com.spark.learn.hdfs

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.log4j.Logger
import org.scalatest.{Matchers, WordSpec}

class InMemoryHdfsTest extends WordSpec with Matchers {
  private val log: Logger = Logger.getLogger(classOf[InMemoryHdfsTest].getName)

  "test file copy to in-memory hdfs" in {
    val baseDir: File = new File("target/hdfs/1").getAbsoluteFile()
    FileUtils.deleteDirectory(baseDir)
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val cluster = new MiniDFSCluster.Builder(conf).build()
    cluster.getFileSystem.copyFromLocalFile(
      new Path("src/main/resources/datafile.csv"),
      new Path("/data/datafile.csv"))
    log.warn(s"URL for HDFS:=> hdfs://localhost:${cluster.getNameNodePort}")
    val accessTime = cluster.getFileSystem
      .listFiles(new Path("/data/datafile.csv"), false)
      .next()
      .asInstanceOf[LocatedFileStatus]
      .getAccessTime
    cluster.shutdown(true)

    assert(accessTime > 0)
  }
}
