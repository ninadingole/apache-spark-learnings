package com.learn.spark.listener

import java.net.URI
import java.util.logging.Logger

import com.learn.spark.HelloWorldDF
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession

object SparkListenerDemo {
  val log =
    Logger.getLogger("SparkListenerDemo")

  def main(args: Array[String]): Unit = {
    SparkListenerDemo.runJob("src/main/resources/datafile.csv", false)
  }

  def runJob(path: String, deleteFile: Boolean) = {
    val session =
      SparkSession
        .builder()
        .appName("Spark Listener Demo")
        .master("local[2]")
        .getOrCreate()

    session.sparkContext
      .addSparkListener(
        new SparkDemoListener(path, session.sparkContext, deleteFile))

    val df = session.read
      .option("header", true)
      .option("treatEmptyValuesAsNulls", true)
      .csv(path)
      .toDF(
        HelloWorldDF.ColumnSeq: _*
      )
    df.foreach(row => log.severe(row.toString()))
    log.info("Count is " + df.count())
  }
}

class SparkDemoListener(val fileName: String,
                        sc: SparkContext,
                        deleteFile: Boolean)
    extends SparkListener {
  val logger = Logger.getLogger("SparkDemoListener")

  override def onApplicationStart(
      applicationStart: SparkListenerApplicationStart): Unit = {
    println(s"Application Start ${applicationStart.appId}")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println(s"Job Start.... ${jobStart.jobId}, ${jobStart.stageIds}")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println("Job End" + jobEnd.jobId + " " + jobEnd.jobResult.toString)
  }

  override def onApplicationEnd(
      applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("Application end..." + applicationEnd.time)
    deleteHdfsFile()
  }

  private def deleteHdfsFile(): Unit = {
    val hdfsPath = fileName.substring(0, fileName.lastIndexOf(":") + 6)
    val filePath = fileName.substring(fileName.lastIndexOf(":") + 6)
    val fs = FileSystem
      .get(new URI(hdfsPath), sc.hadoopConfiguration)
    val path = new Path(filePath)
    if (deleteFile) {
      fs.delete(path, true)
    }
    logger.warning(s"File Exists : ${fs.exists(path)}")
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {}
}
