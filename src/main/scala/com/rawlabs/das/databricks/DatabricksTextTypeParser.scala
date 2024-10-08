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

class DatabricksTextTypeParser(input: String) {

  var offset = 0

  private def parseType(): Either[String, DatabricksDataType] = {
    if (tryToConsume("BYTE")) {
      Right(DatabricksByteType)
    } else if (tryToConsume("SHORT")) {
      Right(DatabricksShortType)
    } else if (tryToConsume("INTEGER")) {
      Right(DatabricksIntType)
    } else if (tryToConsume("BIGINT")) {
      Right(DatabricksLongType)
    } else if (tryToConsume("STRING")) {
      Right(DatabricksStringType)
    } else if (tryToConsume("FLOAT")) {
      Right(DatabricksFloatType)
    } else if (tryToConsume("DOUBLE")) {
      Right(DatabricksDoubleType)
    } else if (tryToConsume("DECIMAL")) {
      Right(DatabricksDecimalType)
    } else if (tryToConsume("BOOLEAN")) {
      Right(DatabricksBooleanType)
    } else if (tryToConsume("TIMESTAMP")) {
      Right(DatabricksTimestampType)
    } else if (tryToConsume("DATE")) {
      Right(DatabricksDateType)
    } else if (tryToConsume("BINARY")) {
      Right(DatabricksBinaryType)
    } else if (tryToConsume("ARRAY<")) {
      val tipe = for {
        itemType <- parseType()
      } yield DatabricksArrayType(itemType)
      consume(">")
      tipe
    } else if (tryToConsume("STRUCT<")) {
      val fields = parseStructFields()
      Right(DatabricksStructType(fields))
    } else if (tryToConsume("MAP<")) {
      val keyType =
        parseType().getOrElse(throw new IllegalArgumentException(s"Expected key type at position $offset: $input"))
      consume(",")
      val valueType =
        parseType().getOrElse(throw new IllegalArgumentException(s"Expected value type at position $offset: $input"))
      consume(">")
      Right(DatabricksMapType(keyType, valueType))
    } else {
      Left(s"Unknown type at position $offset: $input")
    }
  }

  private def parseStructFields(): Seq[(String, DatabricksDataType)] = {
    val fields = collection.mutable.ArrayBuffer.empty[(String, DatabricksDataType)]
    while (!lookAhead(">")) {
      val name = parseName()
      consume(":")
      skipSpaces()
      val tipe = parseType()
      fields += (
        (
          name,
          tipe.getOrElse(throw new IllegalArgumentException(s"Expected type at position $offset: $input"))
        )
      )
      if (lookAhead(",")) {
        consume(",")
      }
      skipSpaces()
    }
    consume(">")
    fields
  }

  private def skipSpaces(): Unit = {
    while (input(offset).isWhitespace) {
      offset += 1
    }
  }

  private def parseName(): String = {
    val start = offset
    while (input(offset) != ':') {
      offset += 1
    }
    input.substring(start, offset)
  }

  private def lookAhead(expected: String): Boolean = {
    input.substring(offset).startsWith(expected)
  }

  private def tryToConsume(expected: String): Boolean = {
    if (lookAhead(expected)) {
      consume(expected)
      true
    } else {
      false
    }
  }

  private def consume(expected: String): Unit = {
    if (lookAhead(expected)) {
      offset += expected.length
    } else {
      throw new IllegalArgumentException(s"Expected $expected at position $offset")
    }
  }

  val tipe = parseType().right.get

}
