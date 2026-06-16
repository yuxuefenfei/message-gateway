package com.gateway.push.handler;

import com.gateway.push.metrics.GatewayMetrics;
import com.gateway.push.protocol.Frame;
import com.gateway.push.session.SessionRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * 业务上报处理器。
 *
 * <p>该 handler 在 IO 线程完成轻量校验，再把 BizReportSink 调用投递到独立有界线程池。
 * 这样既不会阻塞 Netty IO 线程，也不会让连接生命周期事件参与业务队列竞争。</p>
 */
@Slf4j
@RequiredArgsConstructor
public final class BizReportHandler extends SimpleChannelInboundHandler<Frame> {
    private final SessionRegistry sessionRegistry;
    private final BizReportSink bizReportSink;
    private final GatewayMetrics metrics;
    private final Executor businessExecutor;

    public BizReportHandler(
            SessionRegistry sessionRegistry,
            BizReportSink bizReportSink,
            GatewayMetrics metrics
    ) {
        this(sessionRegistry, bizReportSink, metrics, Runnable::run);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
        // 理论上只有 GatewayPushHandler 放行的 BIZ_REPORT 会到这里；保留类型判断让 handler 更稳妥。
        if (frame.getType() != Frame.Type.BIZ_REPORT) {
            return;
        }

        Optional<String> clientId = sessionRegistry.findClientId(ctx.channel());
        if (clientId.isEmpty()) {
            metrics.unauthenticatedFrameRejected();
            log.warn("Close channel for unauthenticated BIZ_REPORT: {}", ctx.channel().id());
            ctx.close();
            return;
        }

        if (!frame.hasReportData()) {
            metrics.bizReportFailed();
            log.warn("Client {} sent BIZ_REPORT without reportData", clientId.get());
            return;
        }

        try {
            // 这里只提交业务任务，不把 handler 本身挂到业务池，避免连接生命周期事件排队。
            // 队列满时拒绝本次上报，比阻塞 Netty IO 线程或无限堆积内存更可控。
            businessExecutor.execute(() -> acceptReport(clientId.get(), frame));
        } catch (RejectedExecutionException e) {
            metrics.businessTaskRejected();
            log.warn("Drop biz report because business executor is full: clientId={}, metric={}",
                    clientId.get(), frame.getReportData().getMetric());
        }
    }

    private void acceptReport(String clientId, Frame frame) {
        try {
            // 真正的业务落地由 BizReportSink 实现，网关核心只负责鉴权、限流和投递。
            bizReportSink.accept(clientId, frame.getReportData());
            metrics.bizReportAccepted();
        } catch (RuntimeException e) {
            metrics.bizReportFailed();
            log.warn("Biz report sink failed: clientId={}, metric={}", clientId, frame.getReportData().getMetric(), e);
        }
    }
}
