package com.boro.apps.joinatool.dfservice

import com.boro.apps.joinatool.domain.{DfAggregation, TableResult, TableResultWithPrepDF}
import com.boro.apps.sqlops.AnalysisChecks.prepareDf


class DfService(dfAggregator: DfAggregation) {

  /**
   * Creates one spark.sql.DataFrame from two compared in DfAggregation
   * @return spark.sql.DataFrame
   */
  def joinResult:TableResult = {
    TableResult(dfAggregator.tableName, prepareDf(dfAggregator.dfLeft, dfAggregator.dfRight, dfAggregator.seqColumns))
  }

  /**
   * Returns one spark.sql.DataFrame from two compared in DfAggregation with predefined Df from DfAggregation
   *
   * @return spark.sql.DataFrame
   */
  def joinResultWithPrepDF: TableResultWithPrepDF = {
    TableResultWithPrepDF(dfAggregator.tableName, dfAggregator.dfLeft, dfAggregator.dfRight, prepareDf(dfAggregator.dfLeft, dfAggregator.dfRight, dfAggregator.seqColumns))
  }
}
