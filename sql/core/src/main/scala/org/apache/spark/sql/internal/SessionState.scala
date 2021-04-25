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

package org.apache.spark.sql.internal

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution._
import org.apache.spark.sql.streaming.StreamingQueryManager
import org.apache.spark.sql.util.{ExecutionListenerManager, QueryExecutionListener}

/**
 * A class that holds all session-specific state in a given [[SparkSession]].
 * 负责存储所有SparkSession的状态。
 *
 * @param sharedState The state shared across sessions, e.g. global view manager, external catalog.
 * @param conf SQL-specific key-value configurations.
 * @param experimentalMethods Interface to add custom planning strategies and optimizers.
 * @param functionRegistry Internal catalog for managing functions registered by the user.
 * @param udfRegistration Interface exposed to the user for registering user-defined functions.
 * @param catalogBuilder a function to create an internal catalog for managing table and database
 *                       states.
 * @param sqlParser Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
 * @param analyzerBuilder A function to create the logical query plan analyzer for resolving
 *                        unresolved attributes and relations.
 * @param optimizerBuilder a function to create the logical query plan optimizer.
 * @param planner Planner that converts optimized logical plans to physical plans.
 * @param streamingQueryManagerBuilder A function to create a streaming query manager to
 *                                     start and stop streaming queries.
 * @param listenerManager Interface to register custom [[QueryExecutionListener]]s.
 * @param resourceLoaderBuilder a function to create a session shared resource loader to load JARs,
 *                              files, etc.
 * @param createQueryExecution Function used to create QueryExecution objects.
 * @param createClone Function used to create clones of the session state.
 */
private[sql] class SessionState(
    sharedState: SharedState,
    val conf: SQLConf, // Spark SQL配置
    val experimentalMethods: ExperimentalMethods,
    val functionRegistry: FunctionRegistry, // 函数注册
    val udfRegistration: UDFRegistration, // UDF注册
    catalogBuilder: () => SessionCatalog, // 构建Catalog
    val sqlParser: ParserInterface, // SQL解析器
    analyzerBuilder: () => Analyzer, // 分析后的执行计划构建器
    optimizerBuilder: () => Optimizer, // 优化后的执行计划构建器
    val planner: SparkPlanner,
    val streamingQueryManagerBuilder: () => StreamingQueryManager, // Streaming查询管理构建器
    val listenerManager: ExecutionListenerManager, // 监听器管理器
    resourceLoaderBuilder: () => SessionResourceLoader, // 资源加载构建器
    createQueryExecution: LogicalPlan => QueryExecution, // create语句执行器
    createClone: (SparkSession, SessionState) => SessionState,
    val columnarRules: Seq[ColumnarRule], // 自定义规则实现，在执行计划中实现运算符实现
    val queryStagePrepRules: Seq[Rule[SparkPlan]]) { // 查询阶段预处理规则

  // The following fields are lazy to avoid creating the Hive client when creating SessionState.
  lazy val catalog: SessionCatalog = catalogBuilder()
 // 分析器
  lazy val analyzer: Analyzer = analyzerBuilder()
 // 优化器
  lazy val optimizer: Optimizer = optimizerBuilder()
 // 资源加载器
  lazy val resourceLoader: SessionResourceLoader = resourceLoaderBuilder()

  // The streamingQueryManager is lazy to avoid creating a StreamingQueryManager for each session
  // when connecting to ThriftServer.
  // 流查询管理器
  lazy val streamingQueryManager: StreamingQueryManager = streamingQueryManagerBuilder()
  // Catalog管理器
  def catalogManager: CatalogManager = analyzer.catalogManager

  def newHadoopConf(): Configuration = SessionState.newHadoopConf(
    sharedState.sparkContext.hadoopConfiguration,
    conf)

  def newHadoopConfWithOptions(options: Map[String, String]): Configuration = {
    val hadoopConf = newHadoopConf()
    options.foreach { case (k, v) =>
      if ((v ne null) && k != "path" && k != "paths") {
        hadoopConf.set(k, v)
      }
    }
    hadoopConf
  }

  /**
   * Get an identical copy of the `SessionState` and associate it with the given `SparkSession`
   */
  def clone(newSparkSession: SparkSession): SessionState = createClone(newSparkSession, this)

  // ------------------------------------------------------
  //  Helper methods, partially leftover from pre-2.0 days
  // ------------------------------------------------------

  def executePlan(plan: LogicalPlan): QueryExecution = createQueryExecution(plan)

  def refreshTable(tableName: String): Unit = {
    catalog.refreshTable(sqlParser.parseTableIdentifier(tableName))
  }
}

private[sql] object SessionState {
  def newHadoopConf(hadoopConf: Configuration, sqlConf: SQLConf): Configuration = {
    val newHadoopConf = new Configuration(hadoopConf)
    sqlConf.getAllConfs.foreach { case (k, v) => if (v ne null) newHadoopConf.set(k, v) }
    newHadoopConf
  }
}

/**
 * Concrete implementation of a [[BaseSessionStateBuilder]].
 */
@Unstable
class SessionStateBuilder(
    session: SparkSession,
    parentState: Option[SessionState],
    options: Map[String, String])
  extends BaseSessionStateBuilder(session, parentState, options) {
  override protected def newBuilder: NewBuilder = new SessionStateBuilder(_, _, Map.empty)
}

/**
 * Session shared [[FunctionResourceLoader]].
 */
@Unstable
class SessionResourceLoader(session: SparkSession) extends FunctionResourceLoader {
  override def loadResource(resource: FunctionResource): Unit = {
    resource.resourceType match {
      case JarResource => addJar(resource.uri)
      case FileResource => session.sparkContext.addFile(resource.uri)
      case ArchiveResource =>
        throw new AnalysisException(
          "Archive is not allowed to be loaded. If YARN mode is used, " +
            "please use --archives options while calling spark-submit.")
    }
  }

  /**
   * Add a jar path to [[SparkContext]] and the classloader.
   *
   * Note: this method seems not access any session state, but a Hive based `SessionState` needs
   * to add the jar to its hive client for the current session. Hence, it still needs to be in
   * [[SessionState]].
   */
  def addJar(path: String): Unit = {
    session.sparkContext.addJar(path)
    val uri = new Path(path).toUri
    val jarURL = if (uri.getScheme == null) {
      // `path` is a local file path without a URL scheme
      new File(path).toURI.toURL
    } else {
      // `path` is a URL with a scheme
      uri.toURL
    }
    session.sharedState.jarClassLoader.addURL(jarURL)
    Thread.currentThread().setContextClassLoader(session.sharedState.jarClassLoader)
  }
}
