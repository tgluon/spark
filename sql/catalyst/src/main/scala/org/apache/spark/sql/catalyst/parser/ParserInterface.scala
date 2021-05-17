/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Interface for a parser.
 */
@DeveloperApi
trait ParserInterface {
  /**
   * Parse a string to a [[LogicalPlan]].
   * 字符串解析为逻辑计划
   */
  @throws[ParseException]("Text cannot be parsed to a LogicalPlan")
  def parsePlan(sqlText: String): LogicalPlan

  /**
   * Parse a string to an [[Expression]].
   * 字符串解析为Expression
   */
  @throws[ParseException]("Text cannot be parsed to an Expression")
  def parseExpression(sqlText: String): Expression

  /**
   * Parse a string to a [[TableIdentifier]].
   * 字符串解析为表语句
   */
  @throws[ParseException]("Text cannot be parsed to a TableIdentifier")
  def parseTableIdentifier(sqlText: String): TableIdentifier

  /**
   * Parse a string to a [[FunctionIdentifier]].
   * 字符串解析为函数
   */
  @throws[ParseException]("Text cannot be parsed to a FunctionIdentifier")
  def parseFunctionIdentifier(sqlText: String): FunctionIdentifier

  /**
   * Parse a string to a multi-part identifier.
   * 字符串解析为多表达式
   */
  @throws[ParseException]("Text cannot be parsed to a multi-part identifier")
  def parseMultipartIdentifier(sqlText: String): Seq[String]

  /**
   * Parse a string to a [[StructType]]. The passed SQL string should be a comma separated list
   * of field definitions which will preserve the correct Hive metadata.
   * 字符串解析为结构化类型，传递过来的sql字符串碧玺都是逗号隔开的，同事保留完整的hive元数据
   */
  @throws[ParseException]("Text cannot be parsed to a schema")
  def parseTableSchema(sqlText: String): StructType

  /**
   * Parse a string to a [[DataType]].
   * 字符串解析为DataType
   */
  @throws[ParseException]("Text cannot be parsed to a DataType")
  def parseDataType(sqlText: String): DataType

  /**
   * Parse a string to a raw [[DataType]] without CHAR/VARCHAR replacement.
   */
  @throws[ParseException]("Text cannot be parsed to a DataType")
  def parseRawDataType(sqlText: String): DataType
}
