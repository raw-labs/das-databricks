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

import com.rawlabs.das.sdk.{DASSdk, DASSdkBuilder}
import com.rawlabs.utils.core.RawSettings

class DASDatabricksBuilder extends DASSdkBuilder {

  override def dasType: String = "databricks"

  override def build(options: Map[String, String])(implicit settings: RawSettings): DASSdk = new DASDatabricks(options)

}
