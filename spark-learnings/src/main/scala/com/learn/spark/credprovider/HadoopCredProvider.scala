package com.learn.spark.credprovider

import org.apache.spark.sql.SparkSession

/**
  * Allows Credential to be accessed from jceks file provided by Hadoop Credential Provider.
  * To Create jceks run command:
  * > hadoop credential create [alias] -v [value] -provider jceks://file/[fileNameForLocal].jceks
  *
  * Paste the jceks location to the config variable @spark.hadoop.hadoop.security.credential.provider.path
  * Note: While creating jceks make sure hadoop HDFS is running on localmachine.
  *
  * More information about Credential Provider is here:
  * https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html
  *
  * @author ninad
  *
  */
object HadoopCredProvider {
  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .appName("accountBalance")
      .master("local[2]")
      .config("password", "password@123")
      .config("spark.logconf", "true")
      .config(
        "spark.hadoop.hadoop.security.credential.provider.path",
        "<path to jceks>"
      )
      .getOrCreate()
    val hadoopConfig = session.sparkContext.hadoopConfiguration

    val password: Array[Char] =
      hadoopConfig.getPassword("<password alias used in the create command>")

    println(new String(password))
    session.close()
  }
}
