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

import scala.collection.mutable

import com.rawlabs.das.sdk.scala.{DASFunction, DASSdk, DASTable}
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition
import com.typesafe.scalalogging.StrictLogging

class DASDatabricks(options: Map[String, String]) extends DASSdk with StrictLogging {

  private val databricksClient = new DASDatabricksConnection(options)
  private val tables = fetchTables()

  private def fetchTables(): Map[String, DASDatabricksTable] = {
    val databricksTables = databricksClient.dasTables()
    val tables = mutable.Map.empty[String, DASDatabricksTable]
    databricksTables.foreach { databricksTable =>
      tables.put(databricksTable.tableDefinition.getTableId.getName, databricksTable)
    }
    tables.toMap
  }

  override def getTable(name: String): Option[DASTable] = tables.get(name)

  override def getFunction(name: String): Option[DASFunction] = None

  override def tableDefinitions: Seq[TableDefinition] = tables.map(_._2.tableDefinition).toSeq

  override def functionDefinitions: Seq[FunctionDefinition] = Seq.empty
}
