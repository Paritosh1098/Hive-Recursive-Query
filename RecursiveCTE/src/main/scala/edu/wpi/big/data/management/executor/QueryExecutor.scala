package edu.wpi.big.data.management.executor

import edu.wpi.big.data.management.utils.SparkUtil
import org.apache.spark.sql.{DataFrame, SQLContext}

class QueryExecutor(cteTable: String, columns: String, outputTable: String, anchorQuery: String,
                    recursiveQuery: String) {

  private val sqlContext: SQLContext = SparkUtil.sqlContext

  private[this] def executeAnchorQuery(): Unit = {
    val anchorResult: DataFrame = sqlContext.sql(anchorQuery).repartition(2)
    anchorResult.createTempView(cteTable)
    sqlContext.sql("create table " + outputTable + " as select " + columns + " from " + cteTable)
  }

  private[this] def executeRecursiveQueries(): Unit = {
    var resultEmpty: Boolean = false

    while (!resultEmpty) {
      val queryResult: DataFrame = sqlContext.sql(recursiveQuery).repartition(2)

      if (queryResult.head(1).isEmpty) {
        resultEmpty = true
      }
      else {
        queryResult.createOrReplaceTempView(cteTable)
        sqlContext.sql("insert into table " + outputTable + " select " + columns + " from " + cteTable)
      }
    }
  }

  private[this] def cleanup(): Unit = {
    sqlContext.sql("drop view " + cteTable)
  }

  def execute(): Unit = {
    executeAnchorQuery()
    executeRecursiveQueries()
    cleanup()
  }
}