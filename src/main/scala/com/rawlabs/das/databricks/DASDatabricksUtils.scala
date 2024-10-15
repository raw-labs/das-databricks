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
import com.databricks.sdk.service.sql.{
  Disposition,
  ExecuteStatementRequest,
  Format,
  StatementParameterListItem,
  StatementResponse,
  StatementState
}
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec

import scala.collection.JavaConverters._

object DASDatabricksUtils {
  private val POLLING_TIME = 1000
}

class DASDatabricksUtils(client: WorkspaceClient, warehouseID: String) extends StrictLogging {

  import DASDatabricksUtils._

  def execute(query: String, parameters: Seq[StatementParameterListItem] = Seq.empty): DASDatabricksExecuteResult = {
    val stmt = new ExecuteStatementRequest()
    if (parameters.nonEmpty) {
      stmt.setParameters(parameters.asJava)
    }
    stmt.setStatement(query).setWarehouseId(warehouseID).setDisposition(Disposition.INLINE).setFormat(Format.JSON_ARRAY)
    val executeAPI = client.statementExecution()
    val response1 = executeAPI.executeStatement(stmt)
    val response = getResult(response1)
    new DASDatabricksExecuteResult(executeAPI, response)
  }

  @tailrec
  private def getResult(response: StatementResponse): StatementResponse = {
    val state = response.getStatus.getState
    logger.info(s"Query ${response.getStatementId} state: $state")
    state match {
      case StatementState.PENDING | StatementState.RUNNING =>
        Thread.sleep(POLLING_TIME)
        val response2 = client.statementExecution().getStatement(response.getStatementId)
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

}
