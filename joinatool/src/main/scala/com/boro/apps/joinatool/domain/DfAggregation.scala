package com.boro.apps.joinatool.domain

import org.apache.spark.sql.DataFrame

import com.boro.apps.joinatool.{CheckStatus, Codes}

case class DfAggregation(tableName: String, dfLeft: DataFrame, dfRight: DataFrame, seqColumns: Seq[String]){}
case class TableResult(tableName: String, dfResult: DataFrame){}
case class TableResultWithPrepDF(tableName: String, dfLeft: DataFrame, dfRight: DataFrame, dfResult: DataFrame){}
case class CheckStatistics(tableName: String, mapCheckStatistics: Map[_,_]){}
case class TableStatistics(tableName: String, dfWithStatistics: DataFrame){}

case class Check (code: Codes, stat: Any, status: CheckStatus, result: Result) {
  require(result != null, "Check result couldn't be null")
  require(status != null, "Check status couldn't be null")
}

case class Result(res: Boolean, tmstp: java.sql.Timestamp) {
}
case class Calculation(map: Map[String, Any]) {
}
case class ResultSet(calculation: Calculation, result: Result) {
}

