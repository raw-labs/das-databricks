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

trait DatabricksDataType

case object DatabricksByteType extends DatabricksDataType
case object DatabricksShortType extends DatabricksDataType
case object DatabricksIntType extends DatabricksDataType
case object DatabricksLongType extends DatabricksDataType
case object DatabricksStringType extends DatabricksDataType
case object DatabricksFloatType extends DatabricksDataType
case object DatabricksDoubleType extends DatabricksDataType
case object DatabricksDecimalType extends DatabricksDataType
case object DatabricksBooleanType extends DatabricksDataType
case object DatabricksTimestampType extends DatabricksDataType
case object DatabricksDateType extends DatabricksDataType
case object DatabricksBinaryType extends DatabricksDataType
case class DatabricksArrayType(itemType: DatabricksDataType) extends DatabricksDataType
case class DatabricksStructType(fields: Seq[(String, DatabricksDataType)]) extends DatabricksDataType
case class DatabricksMapType(keyType: DatabricksDataType, valueType: DatabricksDataType) extends DatabricksDataType
