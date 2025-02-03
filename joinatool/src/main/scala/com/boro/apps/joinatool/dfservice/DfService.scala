package com.boro.apps.joinatool.dfservice

import com.boro.apps.joinatool.domain.DfAggregation
import com.boro.apps.sqlops.AnalysisChecks.prepareDf
import org.apache.spark.sql.DataFrame


class DfService(dfAggregator: DfAggregation) { // TODO obtain tableName if compare tables

  /**
   * Creates one spark.sql.DataFrame from two compared in DfAggregation
   * @return spark.sql.DataFrame
   */
  def joinResult:DataFrame = {
    prepareDf(dfAggregator.dfLeft, dfAggregator.dfRight, dfAggregator.seqColumns)
  }

}
