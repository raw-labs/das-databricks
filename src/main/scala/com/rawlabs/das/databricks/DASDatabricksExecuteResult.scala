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

import com.databricks.sdk.service.sql._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.protobuf.ByteString
import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.protocol.das.Row
import com.rawlabs.protocol.raw._
import com.typesafe.scalalogging.StrictLogging

import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

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

  val objectMapper = new ObjectMapper()

  @tailrec
  final override def next(): Row = {
    if (rowIterator != null && rowIterator.hasNext) {
      val items = rowIterator.next()
      rowCount -= 1
      val row = Row.newBuilder()
      columns.zip(items.asScala).foreach {
        case (columnInfo, item) =>
          val colType = new DatabricksTextTypeParser(columnInfo.getTypeText).tipe
          val jsonNode = colType match {
            case _: DatabricksArrayType | _: DatabricksMapType | _: DatabricksStructType => objectMapper.readTree(item)
            case _ => objectMapper.valueToTree[JsonNode](item)
          }
          row.putData(columnInfo.getName, databricksToRawValue(colType, jsonNode))
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

  private def databricksToRawValue(colType: DatabricksDataType, item: JsonNode): Value = {
    val builder = Value.newBuilder()
    logger.debug(item.toString)
    if (item.isNull) {
      builder.setNull(ValueNull.newBuilder().build())
    } else colType match {
      case DatabricksByteType => builder.setByte(ValueByte.newBuilder().setV(item.asText.toByte).build())
      case DatabricksShortType => builder.setShort(ValueShort.newBuilder().setV(item.asText.toShort).build())
      case DatabricksIntType => builder.setInt(ValueInt.newBuilder().setV(item.asText.toInt).build())
      case DatabricksLongType => builder.setLong(ValueLong.newBuilder().setV(item.asText.toLong).build())
      case DatabricksFloatType => builder.setFloat(ValueFloat.newBuilder().setV(item.asText.toFloat).build())
      case DatabricksDoubleType => builder.setDouble(ValueDouble.newBuilder().setV(item.asText.toDouble).build())
      case DatabricksDecimalType =>
        // TODO to be tested with a parquet file, extract the value from json
        ???
      case DatabricksStringType => builder.setString(ValueString.newBuilder().setV(item.asText).build())
      case DatabricksBooleanType => builder.setBool(ValueBool.newBuilder().setV(item.asText.toBoolean).build())
      case DatabricksBinaryType =>
        // TODO to be tested with a parquet file
        builder.setBinary(ValueBinary.newBuilder().setV(ByteString.copyFrom(item.asText.getBytes)).build())
      case DatabricksStructType(fields) =>
        val struct = ValueRecord.newBuilder()
        fields.foreach {
          case (name, fieldType) =>
            val field = ValueRecordField.newBuilder()
            field.setName(name)
            field.setValue(databricksToRawValue(fieldType, item.get(name)))
            struct.addFields(field)
        }
        builder.setRecord(struct)
      case DatabricksArrayType(innerType) =>
        val list = ValueList.newBuilder()
        val items = item.elements()
        while (items.hasNext) {
          val i = items.next()
          list.addValues(databricksToRawValue(innerType, i))
        }
        builder.setList(list)
      case DatabricksDateType =>
        val date = java.time.LocalDate.parse(item.asText)
        builder.setDate(
          ValueDate.newBuilder().setYear(date.getYear).setMonth(date.getMonthValue).setDay(date.getDayOfMonth).build()
        )
      case DatabricksTimestampType =>
        val timestamp = java.time.LocalDateTime.parse(item.asText, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
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
            .build()
        )
      case DatabricksMapType(_, _) =>
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
