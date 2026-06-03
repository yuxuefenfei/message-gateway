package com.gateway.push.handler;

import com.gateway.push.auth.TokenAuthenticator;
import com.gateway.push.protocol.ConnectRequest;
import com.gateway.push.protocol.ConnectResponse;
import com.gateway.push.protocol.Frame;
import com.gateway.push.metrics.GatewayMetrics;
import com.gateway.push.session.SessionRegistry;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

/**
 * 网关核心快路径处理器。
 *
 * <p>该 handler 运行在 Netty IO 线程，职责保持轻量：
 * 连接鉴权、认证前拦截、PING/PONG 心跳、空闲关闭以及将 BIZ_REPORT 转发给后续业务 handler。
 * 业务上报的实际处理不在这里执行，避免高频业务逻辑拖慢 IO 线程。</p>
 */
@Slf4j
public final class GatewayPushHandler extends SimpleChannelInboundHandler<Frame> {
    private final SessionRegistry sessionRegistry;
    private final TokenAuthenticator authenticator;
    private final GatewayMetrics metrics;

    public GatewayPushHandler(SessionRegistry sessionRegistry, TokenAuthenticator authenticator) {
        this(sessionRegistry, authenticator, GatewayMetrics.noop());
    }

    public GatewayPushHandler(SessionRegistry sessionRegistry, TokenAuthenticator authenticator, GatewayMetrics metrics) {
        this.sessionRegistry = sessionRegistry;
        this.authenticator = authenticator;
        this.metrics = metrics;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
        if (frame.getType() != Frame.Type.CONNECT && sessionRegistry.findClientId(ctx.channel()).isEmpty()) {
            metrics.unauthenticatedFrameRejected();
            log.warn("Close unauthenticated channel for frame type: {}", frame.getType());
            ctx.close();
            return;
        }

        switch (frame.getType()) {
            case PING:
                handlePing(ctx, frame);
                break;
            case CONNECT:
                handleConnect(ctx, frame);
                break;
            case BIZ_REPORT:
                ctx.fireChannelRead(frame);
                break;
            default:
                log.debug("Ignore unsupported frame type: {}", frame.getType());
        }
    }

    private void handlePing(ChannelHandlerContext ctx, Frame frame) {
        Frame pong = Frame.newBuilder()
                .setType(Frame.Type.PONG)
                .setTimestamp(Instant.now().toEpochMilli())
                .setSequenceId(frame.getSequenceId())
                .build();
        ctx.writeAndFlush(pong);
    }

    private void handleConnect(ChannelHandlerContext ctx, Frame frame) {
        if (sessionRegistry.findClientId(ctx.channel()).isPresent()) {
            writeConnectAck(ctx, frame.getSequenceId(), 409, "ALREADY_CONNECTED")
                    .addListener(ChannelFutureListener.CLOSE);
            return;
        }

        if (!frame.hasConnectRequest()) {
            writeConnectAck(ctx, frame.getSequenceId(), 400, "BAD_REQUEST")
                    .addListener(ChannelFutureListener.CLOSE);
            return;
        }

        ConnectRequest request = frame.getConnectRequest();
        if (request.getClientId().isBlank()) {
            writeConnectAck(ctx, frame.getSequenceId(), 400, "CLIENT_ID_REQUIRED")
                    .addListener(ChannelFutureListener.CLOSE);
            return;
        }

        if (!authenticator.authenticate(request)) {
            writeConnectAck(ctx, frame.getSequenceId(), 401, "UNAUTHORIZED")
                    .addListener(ChannelFutureListener.CLOSE);
            return;
        }

        sessionRegistry.bind(request.getClientId(), ctx.channel());
        metrics.channelAuthenticated(request.getClientId());
        removeAuthTimeoutHandler(ctx);
        writeConnectAck(ctx, frame.getSequenceId(), 200, "SUCCESS");
        log.info("Client connected: clientId={}, channel={}", request.getClientId(), ctx.channel().id());
    }

    private void removeAuthTimeoutHandler(ChannelHandlerContext ctx) {
        AuthTimeoutHandler timeoutHandler = ctx.pipeline().get(AuthTimeoutHandler.class);
        if (timeoutHandler != null) {
            ctx.pipeline().remove(timeoutHandler);
        }
    }

    private ChannelFuture writeConnectAck(ChannelHandlerContext ctx, String sequenceId, int code, String message) {
        Frame response = Frame.newBuilder()
                .setType(Frame.Type.CONNECT_ACK)
                .setSequenceId(sequenceId)
                .setTimestamp(Instant.now().toEpochMilli())
                .setConnectResponse(ConnectResponse.newBuilder()
                        .setCode(code)
                        .setMsg(message)
                        .build())
                .build();
        return ctx.writeAndFlush(response);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == IdleState.READER_IDLE) {
            metrics.idleClosed();
            log.warn("Close idle channel: {}", ctx.channel().id());
            ctx.close();
            return;
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        sessionRegistry.unbind(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Close channel after exception: {}", ctx.channel().id(), cause);
        ctx.close();
    }
}
