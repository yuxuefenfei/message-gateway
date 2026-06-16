package com.gateway.push.server;

import com.gateway.push.auth.TokenAuthenticator;
import com.gateway.push.codec.WebSocketProtobufDecoder;
import com.gateway.push.codec.WebSocketProtobufEncoder;
import com.gateway.push.config.GatewayConfig;
import com.gateway.push.handler.AuthTimeoutHandler;
import com.gateway.push.handler.BizReportHandler;
import com.gateway.push.handler.BizReportSink;
import com.gateway.push.handler.GatewayPushHandler;
import com.gateway.push.metrics.GatewayMetrics;
import com.gateway.push.session.SessionRegistry;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executor;

/**
 * 单个客户端连接的 Netty pipeline 装配器。
 *
 * <p>pipeline 顺序遵循：HTTP 握手编解码 -> WebSocket 协议升级与分片聚合 -> CONNECT 超时保护 ->
 * Protobuf 编解码 -> 网关快路径处理 -> 业务上报分发。鉴权和业务落地任务分别投递到独立有界线程池。</p>
 */
public final class GatewayChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final GatewayConfig config;
    private final SessionRegistry sessionRegistry;
    private final TokenAuthenticator authenticator;
    private final BizReportSink bizReportSink;
    private final GatewayMetrics metrics;
    private final EventExecutorGroup businessExecutorGroup;
    private final Executor authExecutor;

    public GatewayChannelInitializer(
            GatewayConfig config,
            SessionRegistry sessionRegistry,
            TokenAuthenticator authenticator
    ) {
        this(config, sessionRegistry, authenticator, BizReportSink.noop());
    }

    public GatewayChannelInitializer(
            GatewayConfig config,
            SessionRegistry sessionRegistry,
            TokenAuthenticator authenticator,
            BizReportSink bizReportSink
    ) {
        this(config, sessionRegistry, authenticator, bizReportSink, GatewayMetrics.noop(), null, Runnable::run);
    }

    public GatewayChannelInitializer(
            GatewayConfig config,
            SessionRegistry sessionRegistry,
            TokenAuthenticator authenticator,
            BizReportSink bizReportSink,
            GatewayMetrics metrics,
            EventExecutorGroup businessExecutorGroup
    ) {
        this(config, sessionRegistry, authenticator, bizReportSink, metrics, businessExecutorGroup, Runnable::run);
    }

    public GatewayChannelInitializer(
            GatewayConfig config,
            SessionRegistry sessionRegistry,
            TokenAuthenticator authenticator,
            BizReportSink bizReportSink,
            GatewayMetrics metrics,
            EventExecutorGroup businessExecutorGroup,
            Executor authExecutor
    ) {
        this.config = config;
        this.sessionRegistry = sessionRegistry;
        this.authenticator = authenticator;
        this.bizReportSink = bizReportSink;
        this.metrics = metrics;
        this.businessExecutorGroup = businessExecutorGroup;
        this.authExecutor = authExecutor;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        // ChannelPipeline 可以理解为一条处理链：
        // 入站数据从上往下流动（TCP bytes -> HTTP -> WebSocketFrame -> Protobuf Frame -> 业务 handler），
        // 出站写入反向经过编码器（业务 Frame -> BinaryWebSocketFrame -> bytes）。
        ch.pipeline()
                // IdleStateHandler 只负责产生“读空闲”事件，真正关闭连接的逻辑在 GatewayPushHandler。
                .addLast(new IdleStateHandler(config.getReaderIdleTimeout().toMillis(), 0, 0, TimeUnit.MILLISECONDS))
                // HTTP codec 和 aggregator 只服务 WebSocket 握手阶段，握手本身仍是一次 HTTP Upgrade 请求。
                .addLast(new HttpServerCodec())
                .addLast(new HttpObjectAggregator(config.getMaxHttpContentLength()))
                // 完成 /ws 路径校验、HTTP Upgrade 和 WebSocket 协议控制帧处理。
                .addLast(new WebSocketServerProtocolHandler(
                        config.getWebsocketPath(),
                        null,
                        true,
                        config.getMaxWebSocketFrameBytes()))
                // 客户端可能把一条二进制消息拆成多个 WebSocket 分片；这里聚合后再交给 Protobuf 解码。
                .addLast(new WebSocketFrameAggregator(config.getMaxWebSocketFrameBytes()))
                .addLast(new AuthTimeoutHandler(sessionRegistry, config.getConnectTimeout(), metrics))
                // 从这里开始，后面的业务 handler 看到的就是项目自己的 Protobuf Frame 对象。
                .addLast(new WebSocketProtobufDecoder(metrics))
                .addLast(new WebSocketProtobufEncoder())
                .addLast(new GatewayPushHandler(sessionRegistry, authenticator, metrics, authExecutor));

        // BizReportHandler 放在最后，只接收 GatewayPushHandler 主动 fireChannelRead 的 BIZ_REPORT。
        // 传入 EventExecutorGroup 后，该 handler 的事件会切到业务线程池，和 Netty IO 线程隔离。
        ch.pipeline().addLast(new BizReportHandler(
                sessionRegistry,
                bizReportSink,
                metrics,
                businessExecutorGroup == null ? Runnable::run : businessExecutorGroup));
    }
}
