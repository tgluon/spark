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

package org.apache.spark.network.server;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.util.*;

/**
 * Server for the efficient, low-level streaming service.
 * RPC框架的服务端，提供高效的、低级别的流服务。
 */
public class TransportServer implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportServer.class);

    private final TransportContext context;
    private final TransportConf conf;
    private final RpcHandler appRpcHandler;
    private final List<TransportServerBootstrap> bootstraps;

    private ServerBootstrap bootstrap;
    private ChannelFuture channelFuture;
    private int port = -1;
    private final PooledByteBufAllocator pooledAllocator;
    private NettyMemoryMetrics metrics;

    /**
     * Creates a TransportServer that binds to the given host and the given port, or to any available
     * if 0. If you don't want to bind to any special host, set "hostToBind" to null.
     */
    public TransportServer(
            TransportContext context,
            String hostToBind,
            int portToBind,
            RpcHandler appRpcHandler,
            List<TransportServerBootstrap> bootstraps) {
        // 即参数传递的TransportContext的引用；
        this.context = context;
        // 即TransportConf，这里通过调用TransportContext的getConf获取；
        this.conf = context.getConf();
        // 即RPC请求处理器RpcHandler；
        this.appRpcHandler = appRpcHandler;
        // 创建一个汇集ByteBuf但对本地线程缓存禁用的分配器
        if (conf.sharedByteBufAllocators()) {
            this.pooledAllocator = NettyUtils.getSharedPooledByteBufAllocator(
                    conf.preferDirectBufsForSharedByteBufAllocators(), true /* allowCache */);
        } else {
            this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
                    conf.preferDirectBufs(), true /* allowCache */, conf.serverThreads());
        }
        // 即参数传递的TransportServerBootstrap列表；
        this.bootstraps = Lists.newArrayList(Preconditions.checkNotNull(bootstraps));

        boolean shouldClose = true;
        try {
            init(hostToBind, portToBind);
            shouldClose = false;
        } finally {
            if (shouldClose) {
                JavaUtils.closeQuietly(this);
            }
        }
    }

    public int getPort() {
        if (port == -1) {
            throw new IllegalStateException("Server not initialized");
        }
        return port;
    }

    private void init(String hostToBind, int portToBind) {
        // 创建bossGroup和workerGroup（根据Netty的API文档，Netty服务端需同时创建bossGroup和workerGroup。）；
        IOMode ioMode = IOMode.valueOf(conf.ioMode());
        EventLoopGroup bossGroup = NettyUtils.createEventLoop(ioMode, 1,
                conf.getModuleName() + "-boss");
        EventLoopGroup workerGroup = NettyUtils.createEventLoop(ioMode, conf.serverThreads(),
                conf.getModuleName() + "-server");

        // 创建Netty的服务端根引导程序并对其进行配置
        bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NettyUtils.getServerChannelClass(ioMode))
                // 创建一个汇集ByteBuf但对本地线程缓存禁用的分配器；
                .option(ChannelOption.ALLOCATOR, pooledAllocator)
                .option(ChannelOption.SO_REUSEADDR, !SystemUtils.IS_OS_WINDOWS)
                .childOption(ChannelOption.ALLOCATOR, pooledAllocator);
        // 创建shuffle内存监控
        this.metrics = new NettyMemoryMetrics(
                pooledAllocator, conf.getModuleName() + "-server", conf);

        // 调用Netty的API创建Netty的服务端根引导程序并对其进行配置；
        if (conf.backLog() > 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, conf.backLog());
        }

        if (conf.receiveBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_RCVBUF, conf.receiveBuf());
        }

        if (conf.sendBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, conf.sendBuf());
        }

        if (conf.enableTcpKeepAlive()) {
            bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        }

        // 为根引导程序设置管道初始化回调函数，此回调函数首先设置TransportServerBootstrap到根引导程序中，然后调用TransportContext的initializePipeline方法初始化Channel的pipeline；
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                logger.debug("New connection accepted for remote address {}.", ch.remoteAddress());

                RpcHandler rpcHandler = appRpcHandler;
                for (TransportServerBootstrap bootstrap : bootstraps) {
                    rpcHandler = bootstrap.doBootstrap(ch, rpcHandler);
                }
                context.initializePipeline(ch, rpcHandler);
            }
        });
        // 给根引导程序绑定Socket的监听端口，最后返回监听的端口。
        InetSocketAddress address = hostToBind == null ?
                new InetSocketAddress(portToBind) : new InetSocketAddress(hostToBind, portToBind);

        channelFuture = bootstrap.bind(address);
        // shutdownGracefully()方法将释放，所有的资源，并且关闭所有的当前正在使用中的 Channel
        channelFuture.syncUninterruptibly();

        port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        logger.debug("Shuffle server started on port: {}", port);
    }

    public MetricSet getAllMetrics() {
        return metrics;
    }

    @Override
    public void close() {
        if (channelFuture != null) {
            // close is a local operation and should finish within milliseconds; timeout just to be safe
            channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
            channelFuture = null;
        }
        if (bootstrap != null && bootstrap.config().group() != null) {
            bootstrap.config().group().shutdownGracefully();
        }
        if (bootstrap != null && bootstrap.config().childGroup() != null) {
            bootstrap.config().childGroup().shutdownGracefully();
        }
        bootstrap = null;
    }

    public Counter getRegisteredConnections() {
        return context.getRegisteredConnections();
    }
}
