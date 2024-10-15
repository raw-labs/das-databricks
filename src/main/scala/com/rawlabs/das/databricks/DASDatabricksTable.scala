/*
 * Copyright 2024 RAW Labs S.A.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

package com.rawlabs.das.databricks

import com.databricks.sdk.service.catalog.{ColumnInfo, ColumnTypeName, TableInfo}
import com.databricks.sdk.service.sql._
import com.rawlabs.das.sdk.{DASExecuteResult, DASTable}
import com.rawlabs.protocol.das._
import com.rawlabs.protocol.raw.{Type, Value}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable

class DASDatabricksTable(databricksUtils: DASDatabricksUtils, databricksTable: TableInfo)
    extends DASTable
    with StrictLogging {

  override def getRelSize(quals: Seq[Qual], columns: Seq[String]): (Int, Int) = REL_SIZE

  override def execute(
      quals: Seq[Qual],
      columns: Seq[String],
      maybeSortKeys: Option[Seq[SortKey]],
      maybeLimit: Option[Long]
  ): DASExecuteResult = {
    val databricksColumns = if (columns.isEmpty) Seq("NULL") else columns.map(databricksColumnName)
    var query = s"SELECT ${databricksColumns.mkString(",")} FROM " + databricksTable.getName
    val parameters = mutable.ArrayBuffer[StatementParameterListItem]()
    if (quals.nonEmpty) {
      val predicates = quals.zipWithIndex.map {
        case (qual, idx) =>
          if (qual.hasSimpleQual) {
            val operator = databricksOperator(qual.getSimpleQual.getOperator)
            val parameter = rawValueToParameter(qual.getSimpleQual.getValue)
            val column = databricksColumnName(qual.getFieldName)
            val arg = "arg" + idx
            parameter.setName(arg)
            parameters.append(parameter)
            s"$column $operator :$arg"
          } else if (qual.hasListQual) {
            val listQual = qual.getListQual
            val isAny = listQual.getIsAny
            val op = listQual.getOperator
            val values = listQual.getValuesList.asScala.map(rawValueToDatabricksQueryString).map('(' + _ + ')')
            val column = databricksColumnName(qual.getFieldName)
            val valuesTable = databricksColumnName("vs_" + qual.getFieldName)
            val v = databricksColumnName("v_" + qual.getFieldName)
            val operator = databricksOperator(op)
            if (isAny) {
              // We generate an EXISTS clause: WHERE x > ANY (1,2,3) => WHERE EXISTS (SELECT * FROM VALUES (1),(2),(3) AS values(v) WHERE x > v)
              val subquery =
                s"SELECT * FROM VALUES ${values.mkString(",")} AS $valuesTable($v) WHERE $column $operator $v"
              s"EXISTS ($subquery)"
            } else {
              // We use NOT EXIST with the NOT operation:
              // WHERE x > ALL (1,2,3) => WHERE NOT EXISTS (SELECT * FROM VALUES (1),(2),(3) AS values(v) WHERE NOT x > v)
              val subquery =
                s"SELECT * FROM VALUES ${values.mkString(",")} AS $valuesTable($v) WHERE NOT $column $operator $v"
              s"NOT EXISTS ($subquery)"
            }
          }
      }
      query += " WHERE " + predicates.mkString(" AND ")
    }

    if (maybeSortKeys.nonEmpty) {
      query += " ORDER BY " + maybeSortKeys.get
        .map { sk =>
          val order = if (sk.getIsReversed) "DESC" else "ASC"
          val nulls = if (sk.getNullsFirst) "NULLS FIRST" else "NULLS LAST"
          databricksColumnName(sk.getName) + " " + order + " " + nulls
        }
        .mkString(", ")
    }
    if (maybeLimit.nonEmpty) {
      query += " LIMIT " + maybeLimit.get
    }

    databricksUtils.execute(query, parameters)
  }

  private def databricksColumnName(name: String): String = {
    '`' + name + '`'
  }

  private def databricksOperator(op: Operator): String = {

    op.getType match {
      case OperatorType.EQUALS => "="
      case OperatorType.GREATER_THAN => ">"
      case OperatorType.GREATER_THAN_OR_EQUAL => ">="
      case OperatorType.LESS_THAN => "<"
      case OperatorType.LESS_THAN_OR_EQUAL => "<="
      case OperatorType.NOT_EQUALS => "<>"
      case _ => throw new IllegalArgumentException(s"Unsupported operator: ${op.getType}")
    }
  }

  override def canSort(sortKeys: Seq[SortKey]): Seq[SortKey] = sortKeys

  private val STARTUP_COST = 3000
  private val REL_SIZE = (100, 100)

  val tableDefinition: TableDefinition = {
    val definition = TableDefinition.newBuilder().setTableId(TableId.newBuilder().setName(databricksTable.getName))
    if (databricksTable.getComment != null) definition.setDescription(databricksTable.getComment)
    databricksTable.getColumns.asScala.foreach {
      case databricksColumn => columnType(databricksColumn) match {
          case Some(tipe) =>
            val columnDef = ColumnDefinition.newBuilder()
            columnDef.setName(databricksColumn.getName)
            columnDef.setType(tipe)
            if (databricksColumn.getComment != null) columnDef.setDescription(databricksColumn.getComment)
            definition.addColumns(columnDef)
          case None =>
            // We ignore columns of unsupported types
            logger.warn(s"Unsupported column type: ${databricksColumn.getTypeJson}")
        }
    }
    definition.setStartupCost(STARTUP_COST)
    definition.build()
  }

  private def columnType(info: ColumnInfo): Option[Type] = {
    val builder = Type.newBuilder()
    val columnType = info.getTypeName
    val isNullable = info.getNullable
    columnType match {
      case ColumnTypeName.BYTE =>
        builder.setByte(com.rawlabs.protocol.raw.ByteType.newBuilder().setTriable(false).setNullable(isNullable))
      case ColumnTypeName.SHORT =>
        builder.setShort(com.rawlabs.protocol.raw.ShortType.newBuilder().setTriable(false).setNullable(isNullable))
      case ColumnTypeName.INT =>
        builder.setInt(com.rawlabs.protocol.raw.IntType.newBuilder().setTriable(false).setNullable(isNullable))
      case ColumnTypeName.LONG =>
        builder.setLong(com.rawlabs.protocol.raw.LongType.newBuilder().setTriable(false).setNullable(isNullable))
      case ColumnTypeName.FLOAT =>
        builder.setFloat(com.rawlabs.protocol.raw.FloatType.newBuilder().setTriable(false).setNullable(isNullable))
      case ColumnTypeName.DOUBLE =>
        builder.setDouble(com.rawlabs.protocol.raw.DoubleType.newBuilder().setTriable(false).setNullable(isNullable))
      case ColumnTypeName.DECIMAL =>
        builder.setDecimal(com.rawlabs.protocol.raw.DecimalType.newBuilder().setTriable(false).setNullable(isNullable))
      case ColumnTypeName.STRING =>
        builder.setString(com.rawlabs.protocol.raw.StringType.newBuilder().setTriable(false).setNullable(isNullable))
      case ColumnTypeName.BOOLEAN =>
        builder.setBool(com.rawlabs.protocol.raw.BoolType.newBuilder().setTriable(false).setNullable(isNullable))
      case ColumnTypeName.DATE =>
        builder.setDate(com.rawlabs.protocol.raw.DateType.newBuilder().setTriable(false).setNullable(isNullable))
      case ColumnTypeName.TIMESTAMP => builder.setTimestamp(
          com.rawlabs.protocol.raw.TimestampType.newBuilder().setTriable(false).setNullable(isNullable)
        )
      case ColumnTypeName.STRUCT => return None // TODO needs to extract the type info from JSON
      case ColumnTypeName.ARRAY => return None // TODO needs to extract the type info from JSON
      case _ => throw new IllegalArgumentException(s"Unsupported column type: $columnType")
    }
    Some(builder.build())
  }

  private def rawValueToDatabricksQueryString(v: Value): String = {
    logger.debug(s"Converting value to query string: $v")
    if (v.hasByte) v.getByte.getV.toString
    else if (v.hasShort) v.getShort.getV.toString
    else if (v.hasInt) v.getInt.getV.toString
    else if (v.hasLong) v.getLong.getV.toString
    else if (v.hasFloat) v.getFloat.getV.toString
    else if (v.hasDouble) v.getDouble.getV.toString
    else if (v.hasDecimal) v.getDecimal.getV
    else if (v.hasString) {
      // This needs to be escaped for SQL queries
      val str = v.getString.getV
      '\'' + str.replace("'", "''") + '\''
    } else if (v.hasBool) v.getBool.getV.toString
    else if (v.hasNull) "NULL"
    else if (v.hasDate) {
      val year = v.getDate.getYear
      val month = v.getDate.getMonth
      val day = v.getDate.getDay
      f"DATE '$year%04d-$month%02d-$day%02d'"
    } else if (v.hasTimestamp) {
      val year = v.getTimestamp.getYear
      val month = v.getTimestamp.getMonth
      val day = v.getTimestamp.getDay
      val hour = v.getTimestamp.getHour
      val minute = v.getTimestamp.getMinute
      val second = v.getTimestamp.getSecond
      val nano = v.getTimestamp.getNano
      f"TIMESTAMP '$year%04d-$month%02d-$day%02dT$hour%02d:$minute%02d:$second%02d.$nano%09dZ'"
    } else {
      throw new IllegalArgumentException(s"Unsupported value: $v")
    }
  }

  private def rawValueToParameter(v: Value): StatementParameterListItem = {
    logger.debug(s"Converting value to parameter: $v")
    val parameter = new StatementParameterListItem()
    if (v.hasByte) {
      parameter.setValue(v.getByte.getV.toString)
      parameter.setType("BYTE")
    } else if (v.hasShort) {
      parameter.setValue(v.getShort.getV.toString)
      parameter.setType("SHORT")
    } else if (v.hasInt) {
      parameter.setValue(v.getInt.getV.toString)
      parameter.setType("INT")
    } else if (v.hasLong) {
      parameter.setValue(v.getLong.getV.toString)
      parameter.setType("LONG")
    } else if (v.hasFloat) {
      parameter.setValue(v.getFloat.getV.toString)
      parameter.setType("FLOAT")
    } else if (v.hasDouble) {
      parameter.setValue(v.getDouble.getV.toString)
      parameter.setType("DOUBLE")
    } else if (v.hasDecimal) {
      parameter.setValue(v.getDecimal.getV)
      parameter.setType("DECIMAL")
    } else if (v.hasString) {
      parameter.setValue(v.getString.getV)
      parameter.setType("STRING")
    } else if (v.hasBool) {
      parameter.setValue(v.getBool.getV.toString)
      parameter.setType("BOOLEAN")
    } else if (v.hasNull) {
      parameter.setValue(null)
      parameter.setType("NULL")
    } else if (v.hasDate) {
      val year = v.getDate.getYear
      val month = v.getDate.getMonth
      val day = v.getDate.getDay
      val formatted = f"$year%04d-$month%02d-$day%02d"
      parameter.setValue(formatted)
      parameter.setType("DATE")
    } else if (v.hasTimestamp) {
      val year = v.getTimestamp.getYear
      val month = v.getTimestamp.getMonth
      val day = v.getTimestamp.getDay
      val hour = v.getTimestamp.getHour
      val minute = v.getTimestamp.getMinute
      val second = v.getTimestamp.getSecond
      val nano = v.getTimestamp.getNano
      val formatted = f"$year%04d-$month%02d-$day%02dT$hour%02d:$minute%02d:$second%02d.$nano%09dZ"
      parameter.setValue(formatted)
      parameter.setType("TIMESTAMP")
    } else {
      throw new IllegalArgumentException(s"Unsupported value: $v")
    }
  }

  override def uniqueColumn: String = {
    "l_orderkey"
  }

  override def insert(row: Row): Row = {
    logger.info("Received insert request for row: " + row)
    ???
  }

  override def bulkInsert(rows: Seq[Row]): Seq[Row] = {
    logger.info("Received bulk insert request for rows: " + rows)
    ???
  }

  override def update(rowId: Value, newValues: Row): Row = {
    logger.info("Received update request for row: " + rowId + " with new values: " + newValues)
    ???
  }

  override def delete(rowId: Value): Unit = {
    logger.info("Received delete request for row: " + rowId)
    ???
  }

}
