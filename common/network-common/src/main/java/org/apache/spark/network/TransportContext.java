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

package org.apache.spark.network;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.Counter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.server.ChunkFetchRequestHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * Contains the context to create a {@link TransportServer}, {@link TransportClientFactory}, and to
 * setup Netty Channel pipelines with a
 * {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 * TransportContext内部包含传输上下文的配置信息TransportConf和对客户端请求消息进行处理的RpcHandler，
 * TransportConf在创建TransportClientFactory和TransportServer时都是必须的，而RpcHandler只用于创建TransportServer。TransportClientFactory是RPC客户端的工厂类。TransportServer是RPC服务端的实现
 */
public class TransportContext implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

  private final TransportConf conf;
  private final RpcHandler rpcHandler;
  private final boolean closeIdleConnections;
  // Number of registered connections to the shuffle service
  private Counter registeredConnections = new Counter();

  /**
   * Force to create MessageEncoder and MessageDecoder so that we can make sure they will be created
   * before switching the current context class loader to ExecutorClassLoader.
   *
   * Netty's MessageToMessageEncoder uses Javassist to generate a matcher class and the
   * implementation calls "Class.forName" to check if this calls is already generated. If the
   * following two objects are created in "ExecutorClassLoader.findClass", it will cause
   * "ClassCircularityError". This is because loading this Netty generated class will call
   * "ExecutorClassLoader.findClass" to search this class, and "ExecutorClassLoader" will try to use
   * RPC to load it and cause to load the non-exist matcher class again. JVM will report
   * `ClassCircularityError` to prevent such infinite recursion. (See SPARK-17714)
   */
  private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
  private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

  // Separate thread pool for handling ChunkFetchRequest. This helps to enable throttling
  // max number of TransportServer worker threads that are blocked on writing response
  // of ChunkFetchRequest message back to the client via the underlying channel.
  private final EventLoopGroup chunkFetchWorkers;

  public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
    this(conf, rpcHandler, false, false);
  }

  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections) {
    this(conf, rpcHandler, closeIdleConnections, false);
  }

  /**
   * Enables TransportContext initialization for underlying client and server.
   *
   * @param conf TransportConf
   * @param rpcHandler RpcHandler responsible for handling requests and responses.
   * @param closeIdleConnections Close idle connections if it is set to true.
   * @param isClientOnly This config indicates the TransportContext is only used by a client.
   *                     This config is more important when external shuffle is enabled.
   *                     It stops creating extra event loop and subsequent thread pool
   *                     for shuffle clients to handle chunked fetch requests.
   */
  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections,
      boolean isClientOnly) {
    this.conf = conf;
    this.rpcHandler = rpcHandler;
    this.closeIdleConnections = closeIdleConnections;

    if (conf.getModuleName() != null &&
        conf.getModuleName().equalsIgnoreCase("shuffle") &&
        !isClientOnly && conf.separateChunkFetchRequest()) {
      // 创建netty 事件
      chunkFetchWorkers = NettyUtils.createEventLoop(
          IOMode.valueOf(conf.ioMode()),
          conf.chunkFetchHandlerThreads(),
          "shuffle-chunk-fetch-handler");
    } else {
      chunkFetchWorkers = null;
    }
  }

  /**
   * Initializes a ClientFactory which runs the given TransportClientBootstraps prior to returning
   * a new Client. Bootstraps will be executed synchronously, and must run successfully in order
   * to create a Client.
   */
  public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
    return new TransportClientFactory(this, bootstraps);
  }

  public TransportClientFactory createClientFactory() {
    return createClientFactory(new ArrayList<>());
  }

  /** Create a server which will attempt to bind to a specific port. */
  public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, null, port, rpcHandler, bootstraps);
  }

  /** Create a server which will attempt to bind to a specific host and port. */
  public TransportServer createServer(
      String host, int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, host, port, rpcHandler, bootstraps);
  }

  /** Creates a new server, binding to any available ephemeral port. */
  public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
    return createServer(0, bootstraps);
  }

  /**
   * 表示通过调用TransportContext的createServer方法创建传输服务端TransportServer的实例。
   * 在构造TransportServer的实例时，需要传递TransportContext、host、port、RpcHandler以及服务端引导程序TransportServerBootstrap的列表
   * @return
   */
  public TransportServer createServer() {
    return createServer(0, new ArrayList<>());
  }
   // 建立netty pipeline
  public TransportChannelHandler initializePipeline(SocketChannel channel) {
    return initializePipeline(channel, rpcHandler);
  }

  /**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * @param channel The channel to initialize.
   * @param channelRpcHandler The RPC handler to use for the channel.
   *
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   * 1、调用createChannelHandler方法创建TransportChannelHandler，从createChannelHandler的实现（见代码清单2）中可以看到真正创建TransportClient是在这里发生的。
   * 从TransportClient的构造过程看到RpcHandler 与TransportClient毫无关系，TransportClient只使用了TransportResponseHandler。
   * TransportChannelHandler在服务端将代理TransportRequestHandler对请求消息进行处理，并在客户端代理TransportResponseHandler对响应消息进行处理。
   *
   * 2、对管道进行设置，这里的ENCODER（即MessageEncoder）派生自Netty的ChannelOutboundHandler接口；DECODER（即MessageDecoder）、
   * TransportChannelHandler以及TransportFrameDecoder（由工具类NettyUtils的静态方法createFrameDecoder创建）派生自Netty的ChannelInboundHandler接口；
   * IdleStateHandler同时实现了ChannelOutboundHandler和ChannelInboundHandler接口。根据Netty的API行为，通过addLast方法注册多个handler时，
   * ChannelInboundHandler按照注册的先后顺序执行；ChannelOutboundHandler按照注册的先后顺序逆序执行，因此在管道两端（无论是服务端还是客户端）处理请求和响应的流程如图1所示。
   */
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler) {
    try {
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      ChannelPipeline pipeline = channel.pipeline()
        .addLast("encoder", ENCODER)
        .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
        .addLast("decoder", DECODER)
        .addLast("idleStateHandler",
          new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler);
      // Use a separate EventLoopGroup to handle ChunkFetchRequest messages for shuffle rpcs.
      if (chunkFetchWorkers != null) {
        ChunkFetchRequestHandler chunkFetchHandler = new ChunkFetchRequestHandler(
          channelHandler.getClient(), rpcHandler.getStreamManager(),
          conf.maxChunksBeingTransferred(), true /* syncModeEnabled */);
        pipeline.addLast(chunkFetchWorkers, "chunkFetchHandler", chunkFetchHandler);
      }
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    TransportClient client = new TransportClient(channel, responseHandler);
    boolean separateChunkFetchRequest = conf.separateChunkFetchRequest();
    ChunkFetchRequestHandler chunkFetchRequestHandler = null;
    if (!separateChunkFetchRequest) {
      chunkFetchRequestHandler = new ChunkFetchRequestHandler(
        client, rpcHandler.getStreamManager(),
        conf.maxChunksBeingTransferred(), false /* syncModeEnabled */);
    }
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
      rpcHandler, conf.maxChunksBeingTransferred(), chunkFetchRequestHandler);
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      conf.connectionTimeoutMs(), separateChunkFetchRequest, closeIdleConnections, this);
  }

  public TransportConf getConf() { return conf; }

  public Counter getRegisteredConnections() {
    return registeredConnections;
  }

  @Override
  public void close() {
    if (chunkFetchWorkers != null) {
      chunkFetchWorkers.shutdownGracefully();
    }
  }
}
