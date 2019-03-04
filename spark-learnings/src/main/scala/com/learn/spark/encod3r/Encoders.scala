package com.learn.spark.encod3r

import com.iamninad.geode.model.ActorModel
import org.apache.spark.sql

object Encoders {

  val ActorModelEncoder = sql.Encoders.kryo(classOf[ActorModel])

}
