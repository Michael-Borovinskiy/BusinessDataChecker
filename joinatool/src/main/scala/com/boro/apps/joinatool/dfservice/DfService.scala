package com.boro.apps.joinatool.dfservice

import com.boro.apps.joinatool.domain.{DfAggregation, TableResult}
import com.boro.apps.sqlops.AnalysisChecks.prepareDf


class DfService(dfAggregator: DfAggregation) { // TODO obtain tableName if compare tables

  /**
   * Creates one spark.sql.DataFrame from two compared in DfAggregation
   * @return spark.sql.DataFrame
   */
  def joinResult:TableResult = {
    TableResult(dfAggregator.tableName, prepareDf(dfAggregator.dfLeft, dfAggregator.dfRight, dfAggregator.seqColumns))
  }

}
