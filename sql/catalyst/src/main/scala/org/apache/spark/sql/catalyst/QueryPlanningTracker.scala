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

package org.apache.spark.sql.catalyst

import scala.collection.JavaConverters._

import org.apache.spark.util.BoundedPriorityQueue


/**
 * A simple utility for tracking runtime and associated stats in query planning.
 *
 * There are two separate concepts we track:
 *
 * 1. Phases: These are broad scope phases in query planning, as listed below, i.e. analysis,
 * optimization and physical planning (just planning).
 *
 * 2. Rules: These are the individual Catalyst rules that we track. In addition to time, we also
 * track the number of invocations and effective invocations.
 */
object QueryPlanningTracker {

  // Define a list of common phases here.
  val PARSING = "parsing"
  val ANALYSIS = "analysis"
  val OPTIMIZATION = "optimization"
  val PLANNING = "planning"

  /**
   * Summary for a rule.
   * @param totalTimeNs total amount of time, in nanosecs, spent in this rule.
   * @param numInvocations number of times the rule has been invoked.
   * @param numEffectiveInvocations number of times the rule has been invoked and
   *                                resulted in a plan change.
   */
  class RuleSummary(
    var totalTimeNs: Long, var numInvocations: Long, var numEffectiveInvocations: Long) {

    def this() = this(totalTimeNs = 0, numInvocations = 0, numEffectiveInvocations = 0)

    override def toString: String = {
      s"RuleSummary($totalTimeNs, $numInvocations, $numEffectiveInvocations)"
    }
  }

  /**
   * Summary of a phase, with start time and end time so we can construct a timeline.
   */
  class PhaseSummary(val startTimeMs: Long, val endTimeMs: Long) {

    def durationMs: Long = endTimeMs - startTimeMs

    override def toString: String = {
      s"PhaseSummary($startTimeMs, $endTimeMs)"
    }
  }

  /**
   * A thread local variable to implicitly pass the tracker around. This assumes the query planner
   * is single-threaded, and avoids passing the same tracker context in every function call.
   */
  private val localTracker = new ThreadLocal[QueryPlanningTracker]() {
    override def initialValue: QueryPlanningTracker = null
  }

  /** Returns the current tracker in scope, based on the thread local variable. */
  def get: Option[QueryPlanningTracker] = Option(localTracker.get())

  /** Sets the current tracker for the execution of function f. We assume f is single-threaded. */
  def withTracker[T](tracker: QueryPlanningTracker)(f: => T): T = {
    val originalTracker = localTracker.get()
    localTracker.set(tracker)
    try f finally { localTracker.set(originalTracker) }
  }
}


class QueryPlanningTracker {

  import QueryPlanningTracker._

  // Mapping from the name of a rule to a rule's summary.
  // Use a Java HashMap for less overhead.
  // key是规则名称，value是规则详细概要
  private val rulesMap = new java.util.HashMap[String, RuleSummary]

  // From a phase to its start time and end time, in ms.
  private val phasesMap = new java.util.HashMap[String, PhaseSummary]

  /**
   * 柯里化(Currying)指的是将原来接受两个参数的函数变成新的接受一个参数的函数的过程。新的函数返回一个以原有第二个参数为参数的函数。
   * 原始：
   * def add(x:Int,y:Int)=x+y
   * add(1,2)
   * 柯里化
   * def add(x:Int)(y:Int) = x + y
   * add(1)(2)
   * Measure the start and end time of a phase. Note that if this function is called multiple
   * times for the same phase, the recorded start time will be the start time of the first call,
   * and the recorded end time will be the end time of the last call.
   */
  def measurePhase[T](phase: String)(f: => T): T = {
    // 开始时间
    val startTime = System.currentTimeMillis()
   // 调用第二个函数
    val ret = f
    // 结束时间
    val endTime = System.currentTimeMillis

    if (phasesMap.containsKey(phase)) { // 判断是否包含这个阶段
      // 如果包含,获取旧的阶段概要
      val oldSummary = phasesMap.get(phase)
      // 将旧的概要写入阶段概要map种
      phasesMap.put(phase, new PhaseSummary(oldSummary.startTimeMs, endTime))
    } else {
      // 不包含,直接往概要map种插入
      phasesMap.put(phase, new PhaseSummary(startTime, endTime))
    }
    // 返回逻辑计划LogicalPlan
    ret
  }

  /**
   * Record a specific invocation of a rule.
   *
   * @param rule name of the rule
   * @param timeNs time taken to run this invocation
   * @param effective whether the invocation has resulted in a plan change
   */
  def recordRuleInvocation(rule: String, timeNs: Long, effective: Boolean): Unit = {
    var s = rulesMap.get(rule)
    if (s eq null) {
      s = new RuleSummary
      rulesMap.put(rule, s)
    }

    s.totalTimeNs += timeNs
    s.numInvocations += 1
    s.numEffectiveInvocations += (if (effective) 1 else 0)
  }

  // ------------ reporting functions below ------------

  def rules: Map[String, RuleSummary] = rulesMap.asScala.toMap

  def phases: Map[String, PhaseSummary] = phasesMap.asScala.toMap

  /**
   * Returns the top k most expensive rules (as measured by time). If k is larger than the rules
   * seen so far, return all the rules. If there is no rule seen so far or k <= 0, return empty seq.
   */
  def topRulesByTime(k: Int): Seq[(String, RuleSummary)] = {
    if (k <= 0) {
      Seq.empty
    } else {
      val orderingByTime: Ordering[(String, RuleSummary)] = Ordering.by(e => e._2.totalTimeNs)
      val q = new BoundedPriorityQueue(k)(orderingByTime)
      rulesMap.asScala.foreach(q.+=)
      q.toSeq.sortBy(r => -r._2.totalTimeNs)
    }
  }

}
