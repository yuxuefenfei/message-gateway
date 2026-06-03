package com.gateway.push.handler;

import com.gateway.push.session.SessionRegistry;
import com.gateway.push.metrics.GatewayMetrics;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * CONNECT 鉴权超时处理器。
 *
 * <p>WebSocket 握手成功只代表底层连接建立，并不代表业务身份可信。
 * 本 handler 在握手完成后启动一个定时任务，如果客户端没有在规定时间内发送
 * CONNECT 并完成认证，就关闭连接，防止未认证连接长期占用文件描述符和内存。</p>
 */
@Slf4j
public final class AuthTimeoutHandler extends ChannelInboundHandlerAdapter {
    private final SessionRegistry sessionRegistry;
    private final Duration connectTimeout;
    private final GatewayMetrics metrics;
    private ScheduledFuture<?> timeoutFuture;

    public AuthTimeoutHandler(SessionRegistry sessionRegistry, Duration connectTimeout) {
        this(sessionRegistry, connectTimeout, GatewayMetrics.noop());
    }

    public AuthTimeoutHandler(SessionRegistry sessionRegistry, Duration connectTimeout, GatewayMetrics metrics) {
        this.sessionRegistry = sessionRegistry;
        this.connectTimeout = connectTimeout;
        this.metrics = metrics;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
            timeoutFuture = ctx.executor().schedule(() -> {
                if (ctx.channel().isActive() && sessionRegistry.findClientId(ctx.channel()).isEmpty()) {
                    metrics.authTimeoutClosed();
                    log.warn("Close unauthenticated channel after connect timeout: {}", ctx.channel().id());
                    ctx.close();
                }
            }, connectTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cancelTimeout();
        super.channelInactive(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        cancelTimeout();
    }

    private void cancelTimeout() {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
            timeoutFuture = null;
        }
    }
}
