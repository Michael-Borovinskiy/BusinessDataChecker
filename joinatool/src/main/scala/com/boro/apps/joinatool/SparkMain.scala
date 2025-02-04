import com.boro.apps.joinatool.Codes
import com.boro.apps.joinatool.checkservice.CheckService
import com.boro.apps.joinatool.dfservice.DfService
import org.apache.spark.sql.SparkSession
import com.boro.apps.joinatool.domain._
import scala.reflect.io.Path

object SparkMain {

  val spark: SparkSession = SparkSession.builder()
    .appName("spark_sql_operations")
    .master("local[6]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  {
    if (!Path("spark-warehouse").exists) {
      val sqLeft = spark.sql(
        """
          |SELECT 1 NUM, 'ANDREW' NAME, 33 PRCNT, 900000 SALARY, 'ENGLAND' COUNTRY UNION ALL
          |SELECT 2 NUM, 'MARY' NAME, 76 PRCNT, 350000 SALARY, 'USA' COUNTRY  UNION ALL
          |SELECT 3 NUM, 'ARNOLD' NAME, 23 PRCNT, 400000 SALARY, 'USA' COUNTRY  UNION ALL
          |SELECT 4 NUM, 'HELEN' NAME, 87 PRCNT, 500000 SALARY, 'USA' COUNTRY  UNION ALL
          |SELECT 5 NUM, 'WANE' NAME, 15 PRCNT, 600000 SALARY, 'ITALY' COUNTRY  UNION ALL
          |SELECT 6 NUM, 'EDWARD' NAME, 33 PRCNT, 900000 SALARY, 'FRANCE' COUNTRY
          |""".stripMargin)

      sqLeft.write.saveAsTable("table1")

      val sqRight = spark.sql(
        """
          |SELECT 1 NUM, 'ANDREW' NAME, 100 PRCNT, 900000 SALARY, 'ENGLAND' COUNTRY, 124000 LIMIT UNION ALL
          |SELECT 2 NUM, 'MARY' NAME, 76 PRCNT, 350000 SALARY, 'USA' COUNTRY, 33000 LIMIT  UNION ALL
          |SELECT 3 NUM, 'ARNOLD' NAME, 23 PRCNT, 40000 SALARY, 'USA' COUNTRY, 9000 LIMIT  UNION ALL
          |SELECT 4 NUM, 'HELEN' NAME, 33 PRCNT, 500000 SALARY, 'USA' COUNTRY, 12000 LIMIT  UNION ALL
          |SELECT 5 NUM, 'WANE' NAME, 15 PRCNT, 600000 SALARY, 'ITALY' COUNTRY, 4000 LIMIT  UNION ALL
          |SELECT 6 NUM, 'EDWARD' NAME, 33 PRCNT, 900000 SALARY, 'FRANCE' COUNTRY, 1500 LIMIT
          |""".stripMargin)

      sqRight.write.saveAsTable("table2")


      val sqLeftTestEq = spark.sql(
        """
          |SELECT 1 NUM, 'ANDREW' NAME, 33 PRCNT, 900000 SALARY, 'ENGLAND' COUNTRY UNION ALL
          |SELECT 2 NUM, 'MARY' NAME, 76 PRCNT, 350000 SALARY, 'USA' COUNTRY  UNION ALL
          |SELECT 3 NUM, 'ARNOLD' NAME, 23 PRCNT, 400000 SALARY, 'USA' COUNTRY  UNION ALL
          |SELECT 4 NUM, 'HELEN' NAME, 87 PRCNT, 500000 SALARY, 'USA' COUNTRY  UNION ALL
          |SELECT 5 NUM, 'WANE' NAME, 15 PRCNT, 600000 SALARY, 'ITALY' COUNTRY  UNION ALL
          |SELECT 6 NUM, 'EDWARD' NAME, 33 PRCNT, 900000 SALARY, 'FRANCE' COUNTRY
          |""".stripMargin)

      sqLeftTestEq.write.saveAsTable("table3")

    } else {
      Seq("table1", "table2", "table3").foreach(tableName => {
        spark.sql("CREATE TABLE " + tableName + " USING parquet OPTIONS (path \"spark-warehouse/" + tableName + "\")")
      })
    }
  }

  def main(args: Array[String]): Unit = {


    val checkService = new CheckService(spark, new DfService(DfAggregation(spark.table("table1")
      , spark.table("table2"), Seq("NUM"))))

    println(checkService.getCheckDetails(Codes.TYPES_EVAL))
    println(checkService.getCheckDetails(Codes.EQUAL_ON_VAL))

    checkService.getCheckDF(Codes.TYPES_EVAL).show(false)
    checkService.getCheckDF(Codes.EQUAL_ON_VAL).show(false)

    val checkService2 = new CheckService(spark, new DfService(DfAggregation(spark.table("table1")
      , spark.table("table3"), Seq("NUM"))))

    println(checkService2.getCheckDetails(Codes.TYPES_EVAL))
    println(checkService2.getCheckDetails(Codes.EQUAL_ON_VAL))

    checkService2.getCheckDF(Codes.TYPES_EVAL).show(false)
    checkService2.getCheckDF(Codes.EQUAL_ON_VAL).show(false)


  }


}
