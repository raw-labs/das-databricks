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

import com.databricks.sdk.WorkspaceClient
import com.databricks.sdk.core.DatabricksConfig
import com.databricks.sdk.service.catalog.ListTablesRequest
import com.databricks.sdk.service.sql.ListWarehousesRequest
import com.rawlabs.das.sdk.scala.{DASFunction, DASSdk, DASTable}
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition

class DASDatabricks(options: Map[String, String]) extends DASSdk {

  private val host: String = options.getOrElse("host", throw new IllegalArgumentException("Host is required"))
  private val token: String = options.getOrElse("token", throw new IllegalArgumentException("Token is required"))
  private val catalog: String = options.getOrElse("catalog", throw new IllegalArgumentException("Catalog is required"))
  private val schema: String = options.getOrElse("schema", throw new IllegalArgumentException("Schema is required"))
  private val warehouse: String =
    options.getOrElse("warehouse", throw new IllegalArgumentException("Warehouse ID is required"))
  private val config = new DatabricksConfig().setHost(host).setToken(token)
  private val databricksClient = new WorkspaceClient(config)

  private val tables = fetchTables()

  databricksClient.warehouses().list(new ListWarehousesRequest()).forEach(println)

  private def fetchTables(): Map[String, DASDatabricksTable] = {
    val req = new ListTablesRequest().setCatalogName(catalog).setSchemaName(schema)
    val databricksTables = databricksClient.tables().list(req)
    val tables = mutable.Map.empty[String, DASDatabricksTable]
    databricksTables.forEach { databricksTable =>
      tables.put(databricksTable.getName, new DASDatabricksTable(databricksClient, warehouse, databricksTable))
    }
    tables.toMap
  }

  override def getTable(name: String): Option[DASTable] = tables.get(name)

  override def getFunction(name: String): Option[DASFunction] = None

  override def tableDefinitions: Seq[TableDefinition] = tables.map(_._2.tableDefinition).toSeq

  override def functionDefinitions: Seq[FunctionDefinition] = Seq.empty
}
