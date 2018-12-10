package edu.wpi.big.data.management.controller

import edu.wpi.big.data.management.executor.QueryExecutor
import edu.wpi.big.data.management.parser.SQLParser
import edu.wpi.big.data.management.utils.SparkUtil
import org.apache.spark.SparkConf

import scala.io.Source

object Controller {

  def main(args: Array[String]): Unit = {
    val timestamp: Long = System.currentTimeMillis / 1000

    if (args.length != 1)
      throw new Exception("Invalid number of arguments\n Usage: spark-submit class_name [configs] jar_name query_file")

    // read the query from file
    // sample query - "with cte(x) as (select 1 as n from dummy union all select x + 1 from cte where x < 10) select x from cte"
    val query: String = Source.fromFile(args(0)).getLines.mkString(" ").toLowerCase()

    println("Started parsing the recursive query.....")

    // parse the given recursive query
    val sqlParser: SQLParser = new SQLParser()
    sqlParser.parseQuery(query)

    // extract the different parts of the recursive query
    val cteTable: String = sqlParser.getCTETableName
    val columns: String = sqlParser.getColumnNames
    val anchorQuery: String = sqlParser.getAnchorQuery
    val recursiveQuery: String = sqlParser.getRecursiveQuery.replace(cteTable, cteTable + "_" + timestamp)
    val outputTable: String = "cte_output_" + timestamp

    println("CTE table: " + cteTable + "_" + timestamp)
    println("Columns: " + columns)
    println("Anchor query: " + anchorQuery)
    println("Recursive query: " + recursiveQuery)
    println("Output table: " + outputTable)

    println("Finished parsing the recursive query!!!")

    println("Started query execution.....")
    // create Spark conf
     SparkUtil.conf_=(new SparkConf().setAppName("SparkTest").setMaster("local[2]"))

    // execute recursive query
     new QueryExecutor(cteTable + "_" + timestamp, columns, outputTable, anchorQuery, recursiveQuery).execute()

    println("Finished query execution!!!")

    // query output available as table
    println("Query output available in table '" + outputTable + "'")
  }
}