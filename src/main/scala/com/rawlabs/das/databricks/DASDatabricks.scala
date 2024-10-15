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

import com.databricks.sdk.WorkspaceClient
import com.databricks.sdk.core.DatabricksConfig
import com.databricks.sdk.service.catalog.ListTablesRequest
import com.databricks.sdk.service.sql.ListWarehousesRequest
import com.rawlabs.das.sdk.{DASExecuteResult, DASFunction, DASSdk, DASTable}
import com.rawlabs.protocol.das.services.{FunctionSupported, OperationsSupportedResponse, OperatorSupported}
import com.rawlabs.protocol.das.{FunctionDefinition, Operator, OperatorType, TableDefinition}
import com.rawlabs.protocol.raw.{IntType, LongType, Type}

import scala.collection.mutable

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

  private val databricksUtils = new DASDatabricksUtils(databricksClient, warehouse)

  private def fetchTables(): Map[String, DASDatabricksTable] = {
    val req = new ListTablesRequest().setCatalogName(catalog).setSchemaName(schema)
    val databricksTables = databricksClient.tables().list(req)
    val tables = mutable.Map.empty[String, DASDatabricksTable]
    databricksTables.forEach { databricksTable =>
      tables.put(databricksTable.getName, new DASDatabricksTable(databricksUtils, databricksTable))
    }
    tables.toMap
  }

  override def tableDefinitions: Seq[TableDefinition] = tables.values.map(_.tableDefinition).toList

  override def functionDefinitions: Seq[FunctionDefinition] = Seq.empty

  override def getTable(name: String): Option[DASTable] = tables.get(name)

  override def getFunction(name: String): Option[DASFunction] = None

  override def operationsSupported: OperationsSupportedResponse = {
    OperationsSupportedResponse
      .newBuilder()
      .setOrderBySupported(false)
      .setJoinSupported(true)
      .setAggregationSupported(true)
      .addFunctionsSupported(
        FunctionSupported
          .newBuilder()
          .setName("count")
          .build()
      )
      .addFunctionsSupported(
        FunctionSupported
          .newBuilder()
          .setName("avg")
          .addParameters(Type.newBuilder().setInt(IntType.newBuilder().setTriable(false).setNullable(true)).build())
          .build()
      )
      .addFunctionsSupported(
        FunctionSupported
          .newBuilder()
          .setName("max")
          .addParameters(Type.newBuilder().setInt(IntType.newBuilder().setTriable(false).setNullable(true)).build())
          .build()
      )
      .addFunctionsSupported(
        FunctionSupported
          .newBuilder()
          .setName("max")
          .addParameters(Type.newBuilder().setLong(LongType.newBuilder().setTriable(false).setNullable(true)).build())
          .build()
      )
      .addFunctionsSupported(
        FunctionSupported
          .newBuilder()
          .setName("min")
          .addParameters(Type.newBuilder().setInt(IntType.newBuilder().setTriable(false).setNullable(true)).build())
          .build()
      )
      .addFunctionsSupported(
        FunctionSupported
          .newBuilder()
          .setName("min")
          .addParameters(Type.newBuilder().setLong(LongType.newBuilder().setTriable(false).setNullable(true)).build())
          .build()
      )
      .addOperatorsSupported(
        OperatorSupported
          .newBuilder()
          .setOperator(Operator.newBuilder().setType(OperatorType.EQUALS).build())
          .setLhs(
            Type
              .newBuilder()
              .setInt(IntType.newBuilder().setTriable(false).setNullable(true))
              .build()
          )
          .setRhs(
            Type
              .newBuilder()
              .setInt(IntType.newBuilder().setTriable(false).setNullable(true))
              .build()
          )
          .build()
      )
      .addOperatorsSupported(
        OperatorSupported
          .newBuilder()
          .setOperator(Operator.newBuilder().setType(OperatorType.LESS_THAN).build())
          .setLhs(
            Type
              .newBuilder()
              .setInt(IntType.newBuilder().setTriable(false).setNullable(true))
              .build()
          )
          .setRhs(
            Type
              .newBuilder()
              .setInt(IntType.newBuilder().setTriable(false).setNullable(true))
              .build()
          )
          .build()
      )
      .addOperatorsSupported(
        OperatorSupported
          .newBuilder()
          .setOperator(Operator.newBuilder().setType(OperatorType.GREATER_THAN).build())
          .setLhs(
            Type
              .newBuilder()
              .setInt(IntType.newBuilder().setTriable(false).setNullable(true))
              .build()
          )
          .setRhs(
            Type
              .newBuilder()
              .setInt(IntType.newBuilder().setTriable(false).setNullable(true))
              .build()
          )
          .build()
      )
      .build()
  }

  override def sqlQuery(sql: String): DASExecuteResult = {
    // HACK: Convert Databricks quotes to PostgreSQL quotes
    val fixedSql = sql.replace("\"", "`").replace("`public`.", "")
    databricksUtils.execute(fixedSql)
  }

}
