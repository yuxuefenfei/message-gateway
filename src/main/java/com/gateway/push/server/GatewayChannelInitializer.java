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
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.concurrent.TimeUnit;

/**
 * 单个客户端连接的 Netty pipeline 装配器。
 *
 * <p>pipeline 顺序遵循：HTTP 握手编解码 -> WebSocket 协议升级 -> CONNECT 超时保护 ->
 * Protobuf 编解码 -> 网关快路径处理 -> 业务上报处理。BIZ_REPORT handler 可挂在独立业务线程池，
 * 从而把高频业务上报与 IO 线程隔离。</p>
 */
public final class GatewayChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final GatewayConfig config;
    private final SessionRegistry sessionRegistry;
    private final TokenAuthenticator authenticator;
    private final BizReportSink bizReportSink;
    private final GatewayMetrics metrics;
    private final EventExecutorGroup businessExecutorGroup;

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
        this(config, sessionRegistry, authenticator, bizReportSink, GatewayMetrics.noop(), null);
    }

    public GatewayChannelInitializer(
            GatewayConfig config,
            SessionRegistry sessionRegistry,
            TokenAuthenticator authenticator,
            BizReportSink bizReportSink,
            GatewayMetrics metrics,
            EventExecutorGroup businessExecutorGroup
    ) {
        this.config = config;
        this.sessionRegistry = sessionRegistry;
        this.authenticator = authenticator;
        this.bizReportSink = bizReportSink;
        this.metrics = metrics;
        this.businessExecutorGroup = businessExecutorGroup;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline()
                .addLast(new IdleStateHandler(config.getReaderIdleTimeout().getSeconds(), 0, 0, TimeUnit.SECONDS))
                .addLast(new HttpServerCodec())
                .addLast(new HttpObjectAggregator(config.getMaxHttpContentLength()))
                .addLast(new WebSocketServerProtocolHandler(
                        config.getWebsocketPath(),
                        null,
                        true,
                        config.getMaxWebSocketFrameBytes()))
                .addLast(new AuthTimeoutHandler(sessionRegistry, config.getConnectTimeout(), metrics))
                .addLast(new WebSocketProtobufDecoder(metrics))
                .addLast(new WebSocketProtobufEncoder())
                .addLast(new GatewayPushHandler(sessionRegistry, authenticator, metrics));

        BizReportHandler bizReportHandler = new BizReportHandler(sessionRegistry, bizReportSink, metrics);
        if (businessExecutorGroup == null) {
            ch.pipeline().addLast(bizReportHandler);
        } else {
            ch.pipeline().addLast(businessExecutorGroup, bizReportHandler);
        }
    }
}
