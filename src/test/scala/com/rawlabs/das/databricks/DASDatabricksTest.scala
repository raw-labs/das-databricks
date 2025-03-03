/*
 * Copyright 2025 RAW Labs S.A.
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

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import com.rawlabs.protocol.das.v1.query.{Operator, Qual, SimpleQual}
import com.rawlabs.protocol.das.v1.tables.{Row => ProtoRow}
import com.rawlabs.protocol.das.v1.types.{Value, ValueString}
import com.typesafe.scalalogging.StrictLogging

class DASDatabricksTest extends AnyFunSuite with StrictLogging {

  // --------------------------------------------------------------------------
  // Configuration for the test
  // --------------------------------------------------------------------------

  // The options for the DAS
  private val options: Map[String, String] = Map(
    "host" -> sys.env("DATABRICKS_HOST"),
    "token" -> sys.env("DATABRICKS_TOKEN"),
    "catalog" -> sys.env("DATABRICKS_CATALOG"),
    "schema" -> sys.env("DATABRICKS_SCHEMA"),
    "warehouse" -> sys.env("DATABRICKS_WAREHOUSE"))

  // --------------------------------------------------------------------------
  // 1) Registration
  // --------------------------------------------------------------------------

  test("Should register Databricks") {
    new DASDatabricks(options)
  }

  test("Should fail to register Databricks with missing options") {
    val missingOptions = options - "host"
    assertThrows[IllegalArgumentException] {
      new DASDatabricks(missingOptions)
    }
  }

  // --------------------------------------------------------------------------
  // 2) Definitions
  // --------------------------------------------------------------------------

  test("Should have some tables") {
    val das = new DASDatabricks(options)
    das.tableDefinitions.nonEmpty
  }

  test("alltpes table definition should exist with expected columns") {
    val das = new DASDatabricks(options)
    val tableAllTypesDef = das.tableDefinitions.find(_.getTableId.getName == "alltypes")
    assert(tableAllTypesDef.isDefined, "alltypes must be defined")
    val colNames = tableAllTypesDef.get.getColumnsList
    val actualNames = colNames.asScala.map(_.getName)
    assert(
      actualNames == Seq(
        "byteCol",
        "shortCol",
        "intCol",
        "longCol",
        "floatCol",
        "doubleCol",
        "decimalCol",
        "stringCol",
        "boolCol",
        "nullBoolCol",
        "dateCol",
        "timeCol",
        "timestampCol"),
      s"Expected columns, got $actualNames")
  }

  // --------------------------------------------------------------------------
  // 3) Execution
  // --------------------------------------------------------------------------

  test("alltypes table project + filter + limit test") {
    val das = new DASDatabricks(options)
    val tableAllTypes = das.getTable("alltypes")
    assert(tableAllTypes.isDefined)

    val dt = tableAllTypes.get
    val execResult = dt.execute(
      quals = Seq(
        Qual
          .newBuilder()
          .setName("stringCol")
          .setSimpleQual(
            SimpleQual
              .newBuilder()
              .setOperator(Operator.EQUALS)
              .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("tralala1"))))
          .build()),
      columns = Seq("intCol"),
      sortKeys = Seq.empty,
      maybeLimit = Some(1L))

    val rowsBuffer = scala.collection.mutable.ArrayBuffer.empty[ProtoRow]
    while (execResult.hasNext) {
      rowsBuffer += execResult.next()
    }
    execResult.close()

    assert(rowsBuffer.size == 1)
    logger.info(s"Rows: $rowsBuffer")
    assert(rowsBuffer(0).getColumnsList.asScala.map(_.getData.getLong.getV) == Seq(100L)) // It shows as long
  }

}
