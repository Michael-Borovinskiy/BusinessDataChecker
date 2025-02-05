package com.boro.apps.joinatool.checkimpl

import com.boro.apps.joinatool.dfservice.DfService
import com.boro.apps.joinatool.domain.{CheckStatistics, TableStatistics}
import org.apache.spark.sql.SparkSession

trait CheckHolder  {

  def getCalculationMap(spark: SparkSession, dfService: DfService): CheckStatistics
  def getCalculationDf(spark: SparkSession, dfService: DfService): TableStatistics
  def getCheckBool(map: Map[_,_]): Boolean

}
