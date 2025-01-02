package com.boro.apps.joinatool.checkimpl

import com.boro.apps.joinatool.dfservice.DfService
import org.apache.spark.sql.{DataFrame, SparkSession}


class CheckResult extends CheckHolder {

  override def getCalculationMap(spark: SparkSession, dfService: DfService): Map[_,_] =
    this.getCalculationMap(spark, dfService)

  override def getCalculationDf(spark: SparkSession, dfService: DfService): DataFrame =
    this.getCalculationDf(spark, dfService)

  override def getCheckBool(map: Map[_, _]): Boolean = {
    this.getCheckBool(map)
  }

}
