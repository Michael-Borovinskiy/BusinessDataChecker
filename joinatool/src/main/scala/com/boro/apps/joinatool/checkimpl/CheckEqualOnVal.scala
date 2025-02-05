package com.boro.apps.joinatool.checkimpl

import com.boro.apps.joinatool.dfservice.DfService
import com.boro.apps.joinatool.domain.{CheckStatistics, TableStatistics}
import com.boro.apps.sqlops.AnalysisChecks._
import org.apache.spark.sql.SparkSession

class CheckEqualOnVal extends CheckHolder {

  override def getCalculationMap(spark: SparkSession, dfService: DfService): CheckStatistics = {
    val joinResult =  dfService.joinResult
    CheckStatistics(joinResult.tableName, checkEqualColumns(joinResult.dfResult).mapResult.asInstanceOf[Map[String, (Long, Long)]])
  }

  override def getCalculationDf(spark: SparkSession, dfService: DfService): TableStatistics = {
    val joinResult =  dfService.joinResult
    TableStatistics(joinResult.tableName, checkEqualColumns(joinResult.dfResult).df)
  }

  override def getCheckBool(map: Map[_, _]): Boolean = {
    !map.asInstanceOf[Map[String, (Long, Long)]].exists(kv => kv._2._2 != 100)
  }

}
