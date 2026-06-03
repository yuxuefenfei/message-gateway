package com.gateway.push.handler;

import com.gateway.push.metrics.GatewayMetrics;
import com.gateway.push.protocol.Frame;
import com.gateway.push.session.SessionRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * 业务上报处理器。
 *
 * <p>该 handler 只处理 BIZ_REPORT 帧，并在 pipeline 中被挂载到独立业务线程池。
 * 这样即使后续的 BizReportSink 接入 Kafka、队列或其它业务系统，也不会阻塞
 * Netty IO 线程上的心跳、鉴权与网络读写。</p>
 */
@Slf4j
@RequiredArgsConstructor
public final class BizReportHandler extends SimpleChannelInboundHandler<Frame> {
    private final SessionRegistry sessionRegistry;
    private final BizReportSink bizReportSink;
    private final GatewayMetrics metrics;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
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
            bizReportSink.accept(clientId.get(), frame.getReportData());
            metrics.bizReportAccepted();
        } catch (RuntimeException e) {
            metrics.bizReportFailed();
            log.warn("Biz report sink failed: clientId={}, metric={}", clientId.get(), frame.getReportData().getMetric(), e);
        }
    }
}
