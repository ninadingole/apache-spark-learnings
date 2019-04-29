package com.learn.spark.grouping

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class AccountBalanceProcessor {

  def process(df: DataFrame): DataFrame = {

    df.repartition(col("acct_key"))
      .withColumn("yearandmonth",
                  from_unixtime(col("posn_dt").divide(1000), "yyyy-MM"))
      .groupBy(col("yearandmonth"), col("acct_key"))
      .agg(max("posn_dt").as("posn_dt"))
      .select(col("acct_key"), col("posn_dt"))
  }

}
