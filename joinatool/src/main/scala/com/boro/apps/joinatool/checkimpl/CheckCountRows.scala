package com.boro.apps.joinatool.checkimpl
import com.boro.apps.joinatool.dfservice.DfService
import com.boro.apps.joinatool.domain.{CheckStatistics, TableStatistics}
import org.apache.spark.sql.SparkSession


/**
 * @author Michael-Borovinskiy
 *         09.03.2025
 */
class CheckCountRows extends CheckHolder {

  override def getCalculationMap(spark: SparkSession, dfService: DfService): CheckStatistics = {
    val joinResult = dfService.joinResultWithPrepDF

    CheckStatistics(joinResult.tableName, Map[String, Long]("dfLeft" -> joinResult.dfLeft.count(), "dfRight" -> joinResult.dfRight.count(), "dfResult" -> joinResult.dfResult.count()))
  }

  override def getCalculationDf(spark: SparkSession, dfService: DfService): TableStatistics = {
    val joinResult = dfService.joinResult
    TableStatistics(joinResult.tableName, spark.emptyDataFrame)
  }

  override def getCheckBool(map: Map[_, _]): Boolean = {
    map.asInstanceOf[Map[String, Long]].values.toList.distinct.size == 1
  }
}
