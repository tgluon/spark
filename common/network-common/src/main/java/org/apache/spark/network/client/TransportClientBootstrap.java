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

package org.apache.spark.network.client;

import io.netty.channel.Channel;

/**
 * A bootstrap which is executed on a TransportClient before it is returned to the user.
 * This enables an initial exchange of information (e.g., SASL authentication tokens) on a once-per-
 * connection basis.
 *
 * Since connections (and TransportClients) are reused as much as possible, it is generally
 * reasonable to perform an expensive bootstrapping operation, as they often share a lifespan with
 * the JVM itself.
 * 当服务端响应客户端连接时在客户端执行一次的引导程序
 *
 * 是在TransportClient上执行的客户端引导程序，主要对连接建立时进行一些初始化的准备（例如验证、加密）。TransportClientBootstrap所作的操作往往是昂贵的，好在建立的连接可以重用。用于初始化TransportClient
 */
public interface TransportClientBootstrap {
  /** Performs the bootstrapping operation, throwing an exception on failure. */
  void doBootstrap(TransportClient client, Channel channel) throws RuntimeException;
}
