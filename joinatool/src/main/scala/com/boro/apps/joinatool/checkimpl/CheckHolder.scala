package com.boro.apps.joinatool.checkimpl

import com.boro.apps.joinatool.dfservice.DfService
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CheckHolder  {

  def getCalculationMap(spark: SparkSession, dfService: DfService): Map[_,_]
  def getCalculationDf(spark: SparkSession, dfService: DfService): DataFrame
  def getCheckBool(map: Map[_,_]): Boolean

}
