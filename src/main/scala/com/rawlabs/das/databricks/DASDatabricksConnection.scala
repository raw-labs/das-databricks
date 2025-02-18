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

import scala.jdk.CollectionConverters.IterableHasAsScala

import com.databricks.sdk.WorkspaceClient
import com.databricks.sdk.core.DatabricksConfig
import com.databricks.sdk.service.catalog.{ListTablesRequest, TableInfo}

class DASDatabricksConnection(options: Map[String, String]) {

  private val host: String = options.getOrElse("host", throw new IllegalArgumentException("Host is required"))
  private val token: String = options.getOrElse("token", throw new IllegalArgumentException("Token is required"))
  private val catalog: String = options.getOrElse("catalog", throw new IllegalArgumentException("Catalog is required"))
  private val schema: String = options.getOrElse("schema", throw new IllegalArgumentException("Schema is required"))
  private val config = new DatabricksConfig().setHost(host).setToken(token)
  private val w = new WorkspaceClient(config)

  def tableDefinitions(): Seq[TableInfo] = {
    val req = new ListTablesRequest().setCatalogName(catalog).setSchemaName(schema)
    val tables = w.tables().list(req).asScala
    tables.toList
  }
}
