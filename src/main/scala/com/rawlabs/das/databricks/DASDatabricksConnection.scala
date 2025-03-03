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

import java.net.UnknownHostException

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IterableHasAsScala

import com.databricks.sdk.WorkspaceClient
import com.databricks.sdk.core.error.platform.{BadRequest, NotFound, PermissionDenied, Unauthenticated}
import com.databricks.sdk.core.{DatabricksConfig, DatabricksError, DatabricksException}
import com.databricks.sdk.service.catalog.ListTablesRequest
import com.databricks.sdk.service.sql.{ExecuteStatementRequest, StatementResponse, StatementState}
import com.rawlabs.das.sdk.{
  DASSdkInvalidArgumentException,
  DASSdkPermissionDeniedException,
  DASSdkUnauthenticatedException
}
import com.typesafe.scalalogging.StrictLogging

class DASDatabricksConnection(options: Map[String, String]) extends StrictLogging {

  private val host: String = options.getOrElse("host", throw new DASSdkInvalidArgumentException("host is required"))
  private val token: String = options.getOrElse("token", throw new DASSdkInvalidArgumentException("token is required"))
  private val catalog: String =
    options.getOrElse("catalog", throw new DASSdkInvalidArgumentException("catalog is required"))
  private val schema: String =
    options.getOrElse("schema", throw new DASSdkInvalidArgumentException("schema is required"))
  private val warehouse: String =
    options.getOrElse("warehouse", throw new DASSdkInvalidArgumentException("warehouse ID is required"))
  private val config = new DatabricksConfig().setHost(host).setToken(token)
  private val databricksClient = new WorkspaceClient(config)

  def dasTables(): Iterable[DASDatabricksTable] = {
    val req = new ListTablesRequest().setCatalogName(catalog).setSchemaName(schema)
    withDatabricksException {
      databricksClient
        .tables()
        .list(req)
        .asScala
        .map(table => new DASDatabricksTable(this, warehouse, table))
    }
  }

  def execute(stmt: ExecuteStatementRequest): DASDatabricksExecuteResult = {
    val executeAPI = databricksClient.statementExecution()
    val response1 = executeAPI.executeStatement(stmt)
    val response = getResult(response1)
    new DASDatabricksExecuteResult(executeAPI, response)
  }

  private val POLLING_TIME = 1000

  @tailrec
  private def getResult(response: StatementResponse): StatementResponse = {
    val state = response.getStatus.getState
    state match {
      case StatementState.PENDING | StatementState.RUNNING =>
        Thread.sleep(POLLING_TIME)
        val response2 = databricksClient.statementExecution().getStatement(response.getStatementId)
        getResult(response2)
      case StatementState.SUCCEEDED => response
      case StatementState.FAILED =>
        throw new RuntimeException(s"Query failed: ${response.getStatus.getError.getMessage}")
      case StatementState.CLOSED =>
        throw new RuntimeException(s"Query closed: ${response.getStatus.getError.getMessage}")
      case StatementState.CANCELED =>
        throw new RuntimeException(s"Query canceled: ${response.getStatus.getError.getMessage}")
    }
  }

  private def mapDatabricksException(t: Throwable): Option[RuntimeException] = {
    t match {
      case e: DatabricksError =>
        e match {
          case e: BadRequest =>
            Some(new DASSdkInvalidArgumentException(e.getErrorInfo.asScala.map(_.getReason).mkString(", "), e))
          case e: NotFound =>
            Some(new DASSdkInvalidArgumentException(e.getErrorInfo.asScala.map(_.getReason).mkString(", "), e))
          case e: PermissionDenied => Some(new DASSdkPermissionDeniedException(e.getMessage, e))
          case e: Unauthenticated  => Some(new DASSdkUnauthenticatedException(e.getMessage, e))
          case e =>
            logger.warn("Unhandled Databricks error", e)
            None
        }
      case e: DatabricksException =>
        e.getCause match {
          case _: UnknownHostException => Some(new DASSdkInvalidArgumentException("invalid host", e))
          case _ =>
            logger.warn("Unhandled Databricks exception", e)
            None
        }
      case _ =>
        logger.warn("Unhandled exception", t)
        None
    }
  }

  private def withDatabricksException[T](block: => T): T = {
    try {
      block
    } catch {
      case t: Throwable =>
        throw mapDatabricksException(t).getOrElse(t)
    }
  }

}
