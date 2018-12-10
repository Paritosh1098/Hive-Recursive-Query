package edu.wpi.big.data.management.parser

import java.io.StringReader

import edu.wpi.big.data.management.exception.SQLParserException

import scala.collection.JavaConverters._
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select._

import scala.collection.mutable.ListBuffer

class SQLParser {
  private[this] var parsedStatement: Select = _
  private[this] var withItem: WithItem = _

  private[this] def getColumns(plainSelect: PlainSelect): List[String] = {
    plainSelect.getSelectItems.asScala.toList
      .map(column => {
        if (column.isInstanceOf[SelectExpressionItem])
          column.asInstanceOf[SelectExpressionItem].getExpression.toString
        else
          column.asInstanceOf[AllColumns].toString
      })
  }

  private[this] def getCTEColumnNames: List[String] = {
    withItem.getWithItemList.asScala.toList
      .map(column => column.asInstanceOf[SelectExpressionItem].getExpression.toString)
  }

  private[this] def getTableName(plainSelect: PlainSelect): List[String] = {
    val tableName: scala.collection.mutable.ListBuffer[String] = new scala.collection.mutable.ListBuffer()
    tableName.append(plainSelect.getFromItem.toString.split(" ")(0))
    if(plainSelect.getJoins != null)
      tableName.append(plainSelect.getJoins.get(0).getRightItem.toString.split(" ")(0))

    tableName.toList
  }

  def parseQuery(query: String): Unit = {
    try {
      parsedStatement = CCJSqlParserUtil.parse(new StringReader(query)).asInstanceOf[Select]
      if (parsedStatement.getWithItemsList.size() != 1)
        throw new SQLParserException("Invalid number of with clauses - " + parsedStatement.getWithItemsList.size())
      withItem = parsedStatement.getWithItemsList.get(0)
    } catch {
      case ex: Exception => {
        throw new SQLParserException(ex.getMessage, ex.getCause)
      }
    }
  }

  def getAnchorQuery: String = {
    val anchorQuery: PlainSelect = withItem.getSelectBody.asInstanceOf[SetOperationList].getPlainSelects.get(0)
    val anchorColumns: List[String] = getColumns(anchorQuery)
    val cteColumns: List[String] = getCTEColumnNames

    if (cteColumns.size != anchorColumns.size)
      throw new SQLParserException("Column numbers differ in CTE and anchor query")

    anchorQuery.toString
  }

  def getRecursiveQuery: String = {
    val recursiveQuery: PlainSelect = withItem.getSelectBody.asInstanceOf[SetOperationList].getPlainSelects.get(1)
    val recursiveColumns: List[String] = getColumns(recursiveQuery)
    val recursiveTableNames: List[String] = getTableName(recursiveQuery)
    val cteColumns: List[String] = getCTEColumnNames

    if (cteColumns.size != recursiveColumns.size)
      throw new SQLParserException("Column numbers differ in CTE and recursive query")

    if (!recursiveTableNames.contains(getCTETableName))
      throw new SQLParserException("CTE name doesn't match in the recursive query - " + recursiveTableNames)

    recursiveQuery.toString
  }

  def getColumnNames: String = {
    val tableColumns: List[String] = getColumns(parsedStatement.getSelectBody.asInstanceOf[PlainSelect])
    val cteColumns: List[String] = getCTEColumnNames

    tableColumns.foreach(column =>
      if (!cteColumns.contains(column) && !column.equals("*")) throw new SQLParserException("Column not found in CTE alias - " + column)
    )

    tableColumns.mkString(", ")
  }

  def getCTETableName: String = {
    val cteName = withItem.getName
    val tableName: String = parsedStatement.getSelectBody.asInstanceOf[PlainSelect].getFromItem.toString
    if (!cteName.equalsIgnoreCase(tableName))
      throw new SQLParserException("CTE name doesn't match in the select clause - " + tableName)
    cteName
  }

}