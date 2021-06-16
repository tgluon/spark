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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.MetricSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.util.*;

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 * <p>
 * The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for
 * all TransportClients.
 * <p>
 * TransportClients will be reused whenever possible. Prior to completing the creation of a new
 * TransportClient, all given {@link TransportClientBootstrap}s will be run.
 * 创建传输客户端（TransportClient）的传输客户端工厂类。
 */
public class TransportClientFactory implements Closeable {

    /** A simple data structure to track the pool of clients between two peer nodes. */
    /**
     * ClientPool实际是由TransportClient的数组构成，而locks数组中的Object与clients数组中的TransportClient按照数组索引一一对应，
     * 通过对每个TransportClient分别采用不同的锁，降低并发情况下线程间对锁的争用，进而减少阻塞，提高并发度
     */
    private static class ClientPool {
        TransportClient[] clients;
        Object[] locks;
        volatile long lastConnectionFailed;

        ClientPool(int size) {
            clients = new TransportClient[size];
            locks = new Object[size];
            for (int i = 0; i < size; i++) {
                locks[i] = new Object();
            }
            lastConnectionFailed = 0;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

    private final TransportContext context;
    private final TransportConf conf;
    //  TransportClientFactory的clientBootstraps属性是TransportClientBootstrap的列表
    private final List<TransportClientBootstrap> clientBootstraps;
    private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

    /**
     * Random number generator for picking connections between peers.
     */
    private final Random rand;
    private final int numConnectionsPerPeer;

    private final Class<? extends Channel> socketChannelClass;
    private EventLoopGroup workerGroup;
    private final PooledByteBufAllocator pooledAllocator;
    private final NettyMemoryMetrics metrics;
    private final int fastFailTimeWindow;

    public TransportClientFactory(
            TransportContext context, // 即参数传递的TransportContext的引用；
            List<TransportClientBootstrap> clientBootstraps) {
        this.context = Preconditions.checkNotNull(context);
        // 即TransportConf，这里通过调用TransportContext的getConf获取；
        this.conf = context.getConf();
        // 参数传递的TransportClientBootstrap列表；
        this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
        // 针对每个Socket地址的连接池ClientPool的缓存；connectionPool的数据结构较为复杂
        this.connectionPool = new ConcurrentHashMap<>();
        // numConnectionsPerPeer：即从TransportConf获取的key为”spark.+模块名+.io.numConnectionsPerPeer”的属性值。此属性值用于指定对等节点间的连接数。
        // 这里的模块名实际为TransportConf的module字段，Spark的很多组件都利用RPC框架构建，它们之间按照模块名区分，例如RPC模块的key为“spark.rpc.io.numConnectionsPerPeer”；
        this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
        //  对Socket地址对应的连接池ClientPool中缓存的TransportClient进行随机选择，对每个连接做负载均衡；
        this.rand = new Random();
        // ioMode：IO模式，即从TransportConf获取key为”spark.+模块名+.io.mode”的属性值。默认值为NIO，Spark还支持EPOLL
        IOMode ioMode = IOMode.valueOf(conf.ioMode());
        // 客户端Channel被创建时使用的类，通过ioMode来匹配，默认为NioSocketChannel，Spark还支持EpollEventLoopGroup
        this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
        //根据Netty的规范，客户端只有worker组，所以此处创建workerGroup。workerGroup的实际类型是NioEventLoopGroup
        this.workerGroup = NettyUtils.createEventLoop(
                ioMode,
                conf.clientThreads(),
                conf.getModuleName() + "-client");
        if (conf.sharedByteBufAllocators()) {
            this.pooledAllocator = NettyUtils.getSharedPooledByteBufAllocator(
                    conf.preferDirectBufsForSharedByteBufAllocators(), false /* allowCache */);
        } else {
            // 汇集ByteBuf但对本地线程缓存禁用的分配器。
            this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
                    conf.preferDirectBufs(), false /* allowCache */, conf.clientThreads());
        }
        this.metrics = new NettyMemoryMetrics(
                this.pooledAllocator, conf.getModuleName() + "-client", conf);
        fastFailTimeWindow = (int) (conf.ioRetryWaitTimeMs() * 0.95);
    }

    public MetricSet getAllMetrics() {
        return metrics;
    }

    /**
     * Create a {@link TransportClient} connecting to the given remote host / port.
     * <p>
     * We maintain an array of clients (size determined by spark.shuffle.io.numConnectionsPerPeer)
     * and randomly picks one to use. If no client was previously created in the randomly selected
     * spot, this function creates a new client and places it there.
     * <p>
     * If the fastFail parameter is true, fail immediately when the last attempt to the same address
     * failed within the fast fail time window (95 percent of the io wait retry timeout). The
     * assumption is the caller will handle retrying.
     * <p>
     * Prior to the creation of a new TransportClient, we will execute all
     * {@link TransportClientBootstrap}s that are registered with this factory.
     * <p>
     * This blocks until a connection is successfully established and fully bootstrapped.
     * <p>
     * Concurrency: This method is safe to call from multiple threads.
     *
     * @param remoteHost remote address host
     * @param remotePort remote address port
     * @param fastFail   whether this call should fail immediately when the last attempt to the same
     *                   address failed with in the last fast fail time window.
     */
    public TransportClient createClient(String remoteHost, int remotePort, boolean fastFail)
            throws IOException, InterruptedException {
        // Get connection from the connection pool first.
        // If it is not found or not active, create a new one.
        // Use unresolved address here to avoid DNS resolution each time we creates a client.

        // 1、调用InetSocketAddress的静态方法createUnresolved构建InetSocketAddress（这种方式创建InetSocketAddress，可以在缓存中已经有TransportClient时避免不必要的域名解析），
        // 然后从connectionPool中获取与此地址对应的ClientPool，如果没有则需要新建ClientPool，并放入缓存connectionPool中；
        final InetSocketAddress unresolvedAddress = InetSocketAddress.createUnresolved(remoteHost, remotePort);

        // Create the ClientPool if we don't have it yet.
        // 2、根据numConnectionsPerPeer的大小（使用“spark.+模块名+.io.numConnectionsPerPeer”属性配置），从ClientPool中随机选择一个TransportClient；
        ClientPool clientPool = connectionPool.get(unresolvedAddress);
        if (clientPool == null) {
            connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer));
            clientPool = connectionPool.get(unresolvedAddress);
        }
        //  3、如果ClientPool的clients中在随机产生索引位置不存在TransportClient或者TransportClient没有激活，则进入第5)步，否则对此TransportClient进行第4)步的检查；
        int clientIndex = rand.nextInt(numConnectionsPerPeer);
        TransportClient cachedClient = clientPool.clients[clientIndex];

        // 4、更新TransportClient的channel中配置的TransportChannelHandler的最后一次使用时间，确保channel没有超时，然后检查TransportClient是否是激活状态，最后返回此TransportClient给调用方；
        if (cachedClient != null && cachedClient.isActive()) {
            // Make sure that the channel will not timeout by updating the last use time of the
            // handler. Then check that the client is still alive, in case it timed out before
            // this code was able to update things.
            TransportChannelHandler handler = cachedClient.getChannel().pipeline()
                    .get(TransportChannelHandler.class);
            synchronized (handler) {
                handler.getResponseHandler().updateTimeOfLastRequest();
            }

            if (cachedClient.isActive()) {
                logger.trace("Returning cached connection to {}: {}",
                        cachedClient.getSocketAddress(), cachedClient);
                return cachedClient;
            }
        }

        // If we reach here, we don't have an existing connection open. Let's create a new one.
        // Multiple threads might race here to create new connections. Keep only one of them active.
        // 5、由于缓存中没有TransportClient可用，于是调用InetSocketAddress的构造器创建InetSocketAddress对象（直接使用InetSocketAddress的构造器创建InetSocketAddress，会进行域名解析），
         // 在这一步骤多个线程可能会产生竞态条件（由于没有同步处理，所以多个线程极有可能同时执行到此处，都发现缓存中没有TransportClient可用，于是都使用InetSocketAddress的构造器创建InetSocketAddress）；
        final long preResolveHost = System.nanoTime();
        final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
        final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
        final String resolvMsg = resolvedAddress.isUnresolved() ? "failed" : "succeed";
        if (hostResolveTimeMs > 2000) {
            logger.warn("DNS resolution {} for {} took {} ms",
                    resolvMsg, resolvedAddress, hostResolveTimeMs);
        } else {
            logger.trace("DNS resolution {} for {} took {} ms",
                    resolvMsg, resolvedAddress, hostResolveTimeMs);
        }

        // 6、第5步中创建InetSocketAddress的过程中产生的竞态条件如果不妥善处理，会产生线程安全问题，所以到了ClientPool的locks数组发挥作用的时候了。按照随机产生的数组索引，
        // locks数组中的锁对象可以对clients数组中的TransportClient一对一进行同步。即便之前产生了竞态条件，但是在这一步只能有一个线程进入临界区。在临界区内，
        // 先进入的线程调用重载的createClient方法创建TransportClient对象并放入ClientPool的clients数组中。当率先进入临界区的线程退出临界区后，其他线程才能进入，
        // 此时发现ClientPool的clients数组中已经存在了TransportClient对象，那么将不再创建TransportClient，而是直接使用它。
        // 创建并返回TransportClient对象
        synchronized (clientPool.locks[clientIndex]) {
            cachedClient = clientPool.clients[clientIndex];

            if (cachedClient != null) {
                if (cachedClient.isActive()) {
                    logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
                    return cachedClient;
                } else {
                    logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
                }
            }
            // If this connection should fast fail when last connection failed in last fast fail time
            // window and it did, fail this connection directly.
            if (fastFail && System.currentTimeMillis() - clientPool.lastConnectionFailed <
                    fastFailTimeWindow) {
                throw new IOException(
                        String.format("Connecting to %s failed in the last %s ms, fail this connection directly",
                                resolvedAddress, fastFailTimeWindow));
            }
            try {
                clientPool.clients[clientIndex] = createClient(resolvedAddress);
                clientPool.lastConnectionFailed = 0;
            } catch (IOException e) {
                clientPool.lastConnectionFailed = System.currentTimeMillis();
                throw e;
            }
            return clientPool.clients[clientIndex];
        }
    }

    public TransportClient createClient(String remoteHost, int remotePort)
            throws IOException, InterruptedException {
        return createClient(remoteHost, remotePort, false);
    }

    /**
     * Create a completely new {@link TransportClient} to the given remote host / port.
     * This connection is not pooled.
     * <p>
     * As with {@link #createClient(String, int)}, this method is blocking.
     */
    public TransportClient createUnmanagedClient(String remoteHost, int remotePort)
            throws IOException, InterruptedException {
        final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
        return createClient(address);
    }

    /**
     * Create a completely new {@link TransportClient} to the remote address.
     * 1、构建根引导器Bootstrap并对其进行配置；
     * <p>
     * 2、为根引导程序设置管道初始化回调函数，此回调函数将调用TransportContext的initializePipeline方法初始化Channel的pipeline；
     * <p>
     * 使用根引导程序连接远程服务器，当连接成功对管道初始化时会回调初始化回调函数，将TransportClient和Channel对象分别设置到原子引用clientRef与channelRef中；
     * <p>
     * 3、给TransportClient设置客户端引导程序，即设置TransportClientFactory中的TransportClientBootstrap列表；
     * <p>
     * 4、最后返回此TransportClient对象。
     */
    @VisibleForTesting
    TransportClient createClient(InetSocketAddress address)
            throws IOException, InterruptedException {
        logger.debug("Creating new connection to {}", address);

        // 构建根引导器Bootstrap并对其进行配置
        Bootstrap bootstrap = new Bootstrap();

        // 为根引导程序设置管道初始化回调函数
        bootstrap.group(workerGroup)
                .channel(socketChannelClass)
                // Disable Nagle's Algorithm since we don't want packets to wait
                //  在TCP/IP协议中，无论发送多少数据，总是要在数据前面加上协议头，同时，对方接收到数据，
                //  也需要发送ACK表示确认。为了尽可能的利用网络带宽，TCP总是希望尽可能的发送足够大的数据。
                //  这里就涉及到一个名为Nagle的算法，该算法的目的就是为了尽可能发送大块数据，避免网络中充斥着许多小数据块。
                .option(ChannelOption.TCP_NODELAY, true)
                // Channeloption.SO_KEEPALIVE参数对应于套接字选项中的SO_KEEPALIVE，该参数用于设置TCP连接，当设置该选项以后，连接会测试链接的状态，
                // 这个选项用于可能长时间没有数据交流的连接。当设置该选项以后，如果在两小时内没有数据的通信时，TCP会自动发送一个活动探测数据报文。
                .option(ChannelOption.SO_KEEPALIVE, true)
                // 连接超时时间
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
                .option(ChannelOption.ALLOCATOR, pooledAllocator);

        if (conf.receiveBuf() > 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, conf.receiveBuf());
        }

        if (conf.sendBuf() > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, conf.sendBuf());
        }

        final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
        final AtomicReference<Channel> channelRef = new AtomicReference<>();

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                TransportChannelHandler clientHandler = context.initializePipeline(ch);
                clientRef.set(clientHandler.getClient());
                channelRef.set(ch);
            }
        });

        // Connect to the remote server
        long preConnect = System.nanoTime();
        // 使用根引导程序连接远程服务器
        ChannelFuture cf = bootstrap.connect(address);
        if (!cf.await(conf.connectionTimeoutMs())) {
            throw new IOException(
                    String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
        } else if (cf.cause() != null) {
            throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
        }

        TransportClient client = clientRef.get();
        Channel channel = channelRef.get();
        assert client != null : "Channel future completed successfully with null client";

        // Execute any client bootstraps synchronously before marking the Client as successful.
        long preBootstrap = System.nanoTime();
        logger.debug("Connection to {} successful, running bootstraps...", address);
        try {
            // 给TransportClient设置客户端引导程序
            for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
                clientBootstrap.doBootstrap(client, channel);
            }
        } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
            long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
            logger.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e);
            client.close();
            throw Throwables.propagate(e);
        }
        long postBootstrap = System.nanoTime();

        logger.info("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
                address, (postBootstrap - preConnect) / 1000000, (postBootstrap - preBootstrap) / 1000000);

        return client;
    }

    /**
     * Close all connections in the connection pool, and shutdown the worker thread pool.
     */
    @Override
    public void close() {
        // Go through all clients and close them if they are active.
        for (ClientPool clientPool : connectionPool.values()) {
            for (int i = 0; i < clientPool.clients.length; i++) {
                TransportClient client = clientPool.clients[i];
                if (client != null) {
                    clientPool.clients[i] = null;
                    JavaUtils.closeQuietly(client);
                }
            }
        }
        connectionPool.clear();

        if (workerGroup != null && !workerGroup.isShuttingDown()) {
            workerGroup.shutdownGracefully();
        }
    }
}
