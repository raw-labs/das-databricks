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

import java.time.format.DateTimeFormatter

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.CollectionHasAsScala

import com.databricks.sdk.service.sql._
import com.google.protobuf.ByteString
import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.protocol.das.v1.tables.{Column, Row}
import com.rawlabs.protocol.das.v1.types.{
  Value,
  ValueBinary,
  ValueBool,
  ValueByte,
  ValueDate,
  ValueDouble,
  ValueFloat,
  ValueInt,
  ValueLong,
  ValueNull,
  ValueShort,
  ValueString,
  ValueTimestamp
}
import com.typesafe.scalalogging.StrictLogging

class DASDatabricksExecuteResult(statementExecutionAPI: StatementExecutionAPI, response: StatementResponse)
    extends DASExecuteResult
    with StrictLogging {

  private val id = response.getStatementId
  private val columns = response.getManifest.getSchema.getColumns.asScala.toList
  private val databricksChunks = response.getManifest.getChunks
  private val chunksIterator: java.util.Iterator[BaseChunkInfo] = {
    // If there are no rows, therefore no chunks, databricksChunks is null
    if (databricksChunks == null) null else databricksChunks.iterator()
  }
  private var rowCount = response.getManifest.getTotalRowCount
  private var rowIterator: java.util.Iterator[java.util.Collection[String]] = null

  final override def hasNext: Boolean = {
    rowCount > 0
  }

  @tailrec
  final override def next(): Row = {
    if (rowIterator != null && rowIterator.hasNext) {
      val items = rowIterator.next()
      rowCount -= 1
      val row = Row.newBuilder()
      columns.zip(items.asScala).foreach { case (columnInfo, item) =>
        row
          .addColumns(Column.newBuilder().setName(columnInfo.getName).setData(databricksToRawValue(columnInfo, item)))
      }
      row.build()
    } else {
      if (chunksIterator.hasNext) {
        val chunk = chunksIterator.next()
        val req = new GetStatementResultChunkNRequest().setStatementId(id).setChunkIndex(chunk.getChunkIndex)
        rowIterator = statementExecutionAPI.getStatementResultChunkN(req).getDataArray.iterator()
        next()
      } else {
        throw new NoSuchElementException("No more rows")
      }
    }
  }

  private def databricksToRawValue(columnInfo: ColumnInfo, item: String): Value = {
    val builder = Value.newBuilder()
    if (item == null) {
      builder.setNull(ValueNull.newBuilder().build())
    } else
      columnInfo.getTypeName match {
        case ColumnInfoTypeName.BYTE    => builder.setByte(ValueByte.newBuilder().setV(item.toByte).build())
        case ColumnInfoTypeName.SHORT   => builder.setShort(ValueShort.newBuilder().setV(item.toShort).build())
        case ColumnInfoTypeName.INT     => builder.setInt(ValueInt.newBuilder().setV(item.toInt).build())
        case ColumnInfoTypeName.LONG    => builder.setLong(ValueLong.newBuilder().setV(item.toLong).build())
        case ColumnInfoTypeName.FLOAT   => builder.setFloat(ValueFloat.newBuilder().setV(item.toFloat).build())
        case ColumnInfoTypeName.DOUBLE  => builder.setDouble(ValueDouble.newBuilder().setV(item.toDouble).build())
        case ColumnInfoTypeName.DECIMAL =>
          // TODO to be tested with a parquet file, extract the value from json
          ???
        case ColumnInfoTypeName.STRING  => builder.setString(ValueString.newBuilder().setV(item).build())
        case ColumnInfoTypeName.BOOLEAN => builder.setBool(ValueBool.newBuilder().setV(item.toBoolean).build())
        case ColumnInfoTypeName.BINARY  =>
          // TODO to be tested with a parquet file
          builder.setBinary(ValueBinary.newBuilder().setV(ByteString.copyFrom(item.getBytes)).build())
        case ColumnInfoTypeName.STRUCT =>
          // TODO extract the value from json
          ???
        case ColumnInfoTypeName.ARRAY =>
          // TODO extract the value from json
          ???
        case ColumnInfoTypeName.DATE =>
          val date = java.time.LocalDate.parse(item)
          builder.setDate(
            ValueDate
              .newBuilder()
              .setYear(date.getYear)
              .setMonth(date.getMonthValue)
              .setDay(date.getDayOfMonth)
              .build())
        case ColumnInfoTypeName.TIMESTAMP =>
          val timestamp = java.time.LocalDateTime.parse(item, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          builder.setTimestamp(
            ValueTimestamp
              .newBuilder()
              .setYear(timestamp.getYear)
              .setMonth(timestamp.getMonthValue)
              .setDay(timestamp.getDayOfMonth)
              .setHour(timestamp.getHour)
              .setMinute(timestamp.getMinute)
              .setSecond(timestamp.getSecond)
              .setNano(timestamp.getNano)
              .build())
        case ColumnInfoTypeName.MAP =>
          // TODO extract the value from json
          ??? // Any
        case t =>
          logger.error(s"Unsupported type: $t")
          throw new IllegalArgumentException(s"Unsupported type: $t")
      }
    builder.build()
  }

  override def close(): Unit = {}

}
