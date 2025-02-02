package com.boro.apps.joinatool.checkservice

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr
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
    val checkService = CheckServiceFactory.getCheckService(code)

    val mapResult: Map[_, _] = checkService.getCalculationMap(spark, dfService)
    val res: Boolean = checkService.getCheckBool(mapResult)

    Map(code.name() -> Check(code, mapResult, CheckStatus.NEW, new Result(res, new Timestamp(System.currentTimeMillis()))
    ))
  }

  /**
   * Creates checked DF with check results
   *
   * @param code - Codes enum value to identify service for checks
   * @return spark.sql.DataFrame with check results columns
   */
  def getCheckDF(code: Codes): DataFrame = {
    val checkService = CheckServiceFactory.getCheckService(code)

    checkService.getCalculationDf(spark, dfService)
  }


  private def getUserStats(dfLeft: DataFrame, dfRight: DataFrame, seqCols: Seq[(String, String)]): Map[String, Check] = {

    val exprJoinKeys: Column = expr(seqCols.map(tuple => s"dfLeft.${tuple._1.trim} = dfRight.${tuple._2.trim}").mkString(" AND "))
    val dfPrep = dfLeft.as("dfLeft").join(dfRight.as("dfRight"), exprJoinKeys, "full")
    val res_prep = checkCountRows(dfLeft, dfRight, dfPrep)

    Map("firstCheck" -> Check(Codes.COUNT_ROWS, res_prep.calculation.map, CheckStatus.NEW, res_prep.result))
  }

  private def checkCountRows(dfLeft: DataFrame, dfRight: DataFrame, dfPostJn: DataFrame): ResultSet = {

    val ch: Boolean = (dfLeft.count == dfRight.count) && (dfLeft.count == dfPostJn.count)
    new ResultSet(new Calculation(Map("dfLeft" -> dfLeft.count, "dfRight" -> dfRight.count)), new Result(ch, new Timestamp(System.currentTimeMillis())))
  }

}
