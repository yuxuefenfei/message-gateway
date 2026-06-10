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
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

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
    private final Executor authExecutor;
    private boolean authInProgress;

    public GatewayPushHandler(SessionRegistry sessionRegistry, TokenAuthenticator authenticator) {
        this(sessionRegistry, authenticator, GatewayMetrics.noop(), Runnable::run);
    }

    public GatewayPushHandler(SessionRegistry sessionRegistry, TokenAuthenticator authenticator, GatewayMetrics metrics) {
        this(sessionRegistry, authenticator, metrics, Runnable::run);
    }

    public GatewayPushHandler(
            SessionRegistry sessionRegistry,
            TokenAuthenticator authenticator,
            GatewayMetrics metrics,
            Executor authExecutor
    ) {
        this.sessionRegistry = sessionRegistry;
        this.authenticator = authenticator;
        this.metrics = metrics;
        this.authExecutor = authExecutor;
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
                closeUnsupportedFrame(ctx, frame);
        }
    }

    private void closeUnsupportedFrame(ChannelHandlerContext ctx, Frame frame) {
        metrics.unsupportedFrameRejected();
        log.warn("Close channel for unsupported frame type: {}", frame.getType());
        ctx.close();
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

        if (authInProgress) {
            writeConnectAck(ctx, frame.getSequenceId(), 409, "AUTHENTICATION_IN_PROGRESS")
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

        submitAuthentication(ctx, frame.getSequenceId(), request);
    }

    private void submitAuthentication(ChannelHandlerContext ctx, String sequenceId, ConnectRequest request) {
        authInProgress = true;
        // 鉴权完成前暂停继续读取，避免同一连接上的后续业务帧越过认证边界。
        ctx.channel().config().setAutoRead(false);
        try {
            authExecutor.execute(() -> {
                boolean authenticated = false;
                Throwable failure = null;
                try {
                    authenticated = authenticator.authenticate(request);
                } catch (RuntimeException e) {
                    failure = e;
                }

                boolean result = authenticated;
                Throwable cause = failure;
                try {
                    ctx.executor().execute(() -> completeAuthentication(ctx, sequenceId, request, result, cause));
                } catch (RejectedExecutionException ignored) {
                    // 连接或服务正在关闭时，IO EventLoop 可能已经拒绝回调，无需继续写响应。
                }
            });
        } catch (RejectedExecutionException e) {
            authInProgress = false;
            metrics.authTaskRejected();
            writeConnectAck(ctx, sequenceId, 503, "AUTH_BUSY").addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void completeAuthentication(
            ChannelHandlerContext ctx,
            String sequenceId,
            ConnectRequest request,
            boolean authenticated,
            Throwable failure
    ) {
        // 本方法始终切回当前 Channel 的 EventLoop，保证 pipeline 和会话状态串行修改。
        authInProgress = false;
        if (!ctx.channel().isActive()) {
            return;
        }
        if (failure != null) {
            metrics.authFailed();
            log.warn("Authentication failed unexpectedly: clientId={}", request.getClientId(), failure);
            writeConnectAck(ctx, sequenceId, 500, "AUTH_ERROR").addListener(ChannelFutureListener.CLOSE);
            return;
        }
        if (!authenticated) {
            writeConnectAck(ctx, sequenceId, 401, "UNAUTHORIZED").addListener(ChannelFutureListener.CLOSE);
            return;
        }

        sessionRegistry.bind(request.getClientId(), ctx.channel());
        metrics.channelAuthenticated(request.getClientId());
        removeAuthTimeoutHandler(ctx);
        writeConnectAck(ctx, sequenceId, 200, "SUCCESS");
        ctx.channel().config().setAutoRead(true);
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
