package com.boro.apps.joinatool.checkservice

import com.boro.apps.joinatool.checkimpl.CheckHolder
import org.apache.spark.sql.SparkSession
import com.boro.apps.joinatool.domain._
import com.boro.apps.joinatool.{CheckStatus, Codes}
import com.boro.apps.joinatool.dfservice.DfService
import com.boro.apps.joinatool.factory.CheckServiceFactory

import java.sql.Timestamp

class CheckService(spark: SparkSession, dfService: DfService) {


  /**
   * Creates map with check results where k is check code name and value is Check object with check results
   *
   * @param code - Codes enum value to identify service for checks
   * @return spark.sql.DataFrame with check results columns
   */
  def getCheckDetails(code: Codes): Map[String, Check] = {
    val checkImpl: CheckHolder = CheckServiceFactory.getCheckImpl(code)

    val mapResult: CheckStatistics = checkImpl.getCalculationMap(spark, dfService)
    val res: Boolean = checkImpl.getCheckBool(mapResult.mapCheckStatistics)

    Map(code.name() -> Check(code, mapResult, CheckStatus.NEW, Result(res, new Timestamp(System.currentTimeMillis()))
    ))
  }

  /**
   * Creates checked DF with check results
   *
   * @param code - Codes enum value to identify service for checks
   * @return spark.sql.DataFrame with check results columns
   */
  def getCheckDF(code: Codes): TableStatistics = {
    val checkImpl: CheckHolder = CheckServiceFactory.getCheckImpl(code)

    checkImpl.getCalculationDf(spark, dfService)
  }

}
