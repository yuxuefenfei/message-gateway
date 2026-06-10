package com.gateway.push.server;

import com.gateway.push.auth.TokenAuthenticator;
import com.gateway.push.config.GatewayConfig;
import com.gateway.push.handler.BizReportSink;
import com.gateway.push.metrics.GatewayMetrics;
import com.gateway.push.session.SessionRegistry;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Netty 推送网关服务启动器。
 *
 * <p>负责创建 boss/worker 事件循环、业务线程池、服务端 bootstrap 以及停服资源释放。
 * 该类不直接处理业务协议，具体协议处理交给 {@link GatewayChannelInitializer}
 * 装配出的 pipeline。</p>
 */
@Slf4j
public final class GatewayServer {
    private final GatewayConfig config;
    private final SessionRegistry sessionRegistry;
    private final TokenAuthenticator authenticator;
    private final BizReportSink bizReportSink;
    private final GatewayMetrics metrics;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private EventExecutorGroup businessExecutorGroup;
    private EventExecutorGroup authExecutorGroup;
    private Channel serverChannel;

    public GatewayServer(GatewayConfig config, SessionRegistry sessionRegistry, TokenAuthenticator authenticator) {
        this(config, sessionRegistry, authenticator, BizReportSink.noop());
    }

    public GatewayServer(
            GatewayConfig config,
            SessionRegistry sessionRegistry,
            TokenAuthenticator authenticator,
            BizReportSink bizReportSink
    ) {
        this(config, sessionRegistry, authenticator, bizReportSink, GatewayMetrics.noop());
    }

    public GatewayServer(
            GatewayConfig config,
            SessionRegistry sessionRegistry,
            TokenAuthenticator authenticator,
            BizReportSink bizReportSink,
            GatewayMetrics metrics
    ) {
        this.config = config;
        this.sessionRegistry = sessionRegistry;
        this.authenticator = authenticator;
        this.bizReportSink = bizReportSink;
        this.metrics = metrics;
    }

    /**
     * 创建线程组、装配 Netty ServerBootstrap 并同步完成端口绑定。
     *
     * @return 已完成 bind 的 ChannelFuture，可通过其 channel 获取实际监听端口和 closeFuture
     * @throws InterruptedException 当前线程在等待 bind 完成时被中断
     */
    public synchronized ChannelFuture start() throws InterruptedException {
        if (serverChannel != null && serverChannel.isActive()) {
            throw new IllegalStateException("Gateway server is already started");
        }

        try {
            // 所有线程组都放在异常保护内创建，任一步失败都能由 stop() 回收已创建资源。
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();
            businessExecutorGroup = new DefaultEventExecutorGroup(
                    config.getBusinessExecutorThreads(),
                    new DefaultThreadFactory("gateway-business"),
                    config.getBusinessExecutorMaxPendingTasks(),
                    (task, executor) -> {
                        throw new RejectedExecutionException("Business executor queue is full");
                    });
            authExecutorGroup = new DefaultEventExecutorGroup(
                    config.getAuthExecutorThreads(),
                    new DefaultThreadFactory("gateway-auth"),
                    config.getAuthExecutorMaxPendingTasks(),
                    (task, executor) -> {
                        throw new RejectedExecutionException("Auth executor queue is full");
                    });

            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new GatewayChannelInitializer(
                            config,
                            sessionRegistry,
                            authenticator,
                            bizReportSink,
                            metrics,
                            businessExecutorGroup,
                            authExecutorGroup))
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                            config.getWriteBufferLowWaterMark(),
                            config.getWriteBufferHighWaterMark()));

            ChannelFuture future = bootstrap.bind(config.getPort()).sync();
            serverChannel = future.channel();
            log.info("Gateway started: ws://0.0.0.0:{}{}", getPort(), config.getWebsocketPath());
            return future;
        } catch (InterruptedException e) {
            stop();
            Thread.currentThread().interrupt();
            throw e;
        } catch (RuntimeException e) {
            stop();
            throw e;
        }
    }

    public int getPort() {
        if (serverChannel == null) {
            return config.getPort();
        }
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    /**
     * 幂等停止网关。
     *
     * <p>关闭顺序为：监听 Channel -> 已认证客户端 Channel -> 鉴权池 -> 业务池 ->
     * Worker EventLoop -> Boss EventLoop。该顺序先阻止新连接，再允许已有异步任务在
     * IO EventLoop 尚存活时完成必要回调。</p>
     */
    public synchronized void stop() {
        // 先关闭监听端口，阻止停机期间再接入新连接；随后关闭现有会话和后台执行器。
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
            serverChannel = null;
        }
        sessionRegistry.closeAll();
        if (authExecutorGroup != null) {
            authExecutorGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            authExecutorGroup = null;
        }
        if (businessExecutorGroup != null) {
            businessExecutorGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            businessExecutorGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            workerGroup = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            bossGroup = null;
        }
    }
}
