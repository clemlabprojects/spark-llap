package com.hortonworks.spark.sql.rule

import org.apache.spark.sql.SparkSessionExtensions

class Extensions extends (SparkSessionExtensions => Unit) with Serializable {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(_ => new HWCSwitchRule())
  }
}
