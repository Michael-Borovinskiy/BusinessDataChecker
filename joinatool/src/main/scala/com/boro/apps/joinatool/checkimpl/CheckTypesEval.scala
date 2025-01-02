package com.boro.apps.joinatool.checkimpl

import com.boro.apps.joinatool.dfservice.DfService
import com.boro.apps.sqlops.AnalysisChecks._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CheckTypesEval extends CheckHolder {

  override def getCalculationMap(spark: SparkSession, dfService: DfService): Map[String, (String, String)] = {
    checkEqualColumnTypes(spark, dfService.joinResult).mapResult.asInstanceOf[Map[String, (String, String)]]
  }

  override def getCalculationDf(spark: SparkSession, dfService: DfService): DataFrame = {
    checkEqualColumnTypes(spark, dfService.joinResult).df
  }

  override def getCheckBool(map: Map[_, _]): Boolean = {
    !map.asInstanceOf[Map[String, (String, String)]].exists(kv => kv._2._1 != kv._2._2)
  }


}
