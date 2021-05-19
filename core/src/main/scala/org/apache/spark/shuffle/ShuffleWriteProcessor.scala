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

package org.apache.spark.shuffle

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus

/**
 * The interface for customizing shuffle write process. The driver create a ShuffleWriteProcessor
 * and put it into [[ShuffleDependency]], and executors use it in each ShuffleMapTask.
 */
private[spark] class ShuffleWriteProcessor extends Serializable with Logging {

  /**
   * Create a [[ShuffleWriteMetricsReporter]] from the task context. As the reporter is a
   * per-row operator, here need a careful consideration on performance.
   */
  protected def createMetricsReporter(context: TaskContext): ShuffleWriteMetricsReporter = {
    context.taskMetrics().shuffleWriteMetrics
  }

  /**
   * The write process for particular partition, it controls the life circle of [[ShuffleWriter]]
   * get from [[ShuffleManager]] and triggers rdd compute, finally return the [[MapStatus]] for
   * this task.
   */
  def write(
             rdd: RDD[_],
             dep: ShuffleDependency[_, _, _],
             mapId: Long,
             context: TaskContext,
             partition: Partition): MapStatus = {
    var writer: ShuffleWriter[Any, Any] = null
    try {
      // shuffle管理器
      val manager = SparkEnv.get.shuffleManager
      // 获取一个shuffle写入器
      writer = manager.getWriter[Any, Any](
        dep.shuffleHandle,
        mapId,
        context,
        createMetricsReporter(context))

      /**
       * 这里可以看到rdd计算的核心方法就是iterator方法
       * SortShuffleWriter的write方法可以分为几个步骤：
       * 将上游rdd计算出的数据(通过调用rdd.iterator方法)写入内存缓冲区，
       * 在写的过程中如果超过 内存阈值就会溢写磁盘文件，可能会写多个文件
       * 最后将溢写的文件和内存中剩余的数据一起进行归并排序后写入到磁盘中形成一个大的数据文件
       * 这个排序是先按分区排序，在按key排序
       * 在最后归并排序后写的过程中，没写一个分区就会手动刷写一遍，并记录下这个分区数据在文件中的位移
       * 所以实际上最后写完一个task的数据后，磁盘上会有两个文件：数据文件和记录每个reduce端partition数据位移的索引文件
       */
      writer.write(
        rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      // 主要是删除中间过程的溢写文件，向内存管理器释放申请的内存
      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }
}
