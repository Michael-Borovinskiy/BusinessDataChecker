package com.boro.apps.joinatool.checkimpl

import com.boro.apps.joinatool.dfservice.DfService
import com.boro.apps.sqlops.AnalysisChecks._
import org.apache.spark.sql.{DataFrame, SparkSession}

class CheckEqualOnVal extends CheckHolder {

  override def getCalculationMap(spark: SparkSession, dfService: DfService): Map[String, (Long, Long)] = {
    checkEqualColumns(dfService.joinResult).mapResult.asInstanceOf[Map[String, (Long, Long)]]
  }

  override def getCalculationDf(spark: SparkSession, dfService: DfService): DataFrame = {
    checkEqualColumns(dfService.joinResult).df
  }

  override def getCheckBool(map: Map[_, _]): Boolean = {
    !map.asInstanceOf[Map[String, (Long, Long)]].exists(kv => kv._2._2 != 100)
  }

}
