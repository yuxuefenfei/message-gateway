package com.gateway.push.handler;

import com.gateway.push.metrics.GatewayMetrics;
import com.gateway.push.protocol.Frame;
import com.gateway.push.session.SessionRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BizReportHandler extends SimpleChannelInboundHandler<Frame> {
    private static final Logger log = LoggerFactory.getLogger(BizReportHandler.class);

    private final SessionRegistry sessionRegistry;
    private final BizReportSink bizReportSink;
    private final GatewayMetrics metrics;

    public BizReportHandler(SessionRegistry sessionRegistry, BizReportSink bizReportSink, GatewayMetrics metrics) {
        this.sessionRegistry = sessionRegistry;
        this.bizReportSink = bizReportSink;
        this.metrics = metrics;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
        if (frame.getType() != Frame.Type.BIZ_REPORT) {
            return;
        }

        String clientId = sessionRegistry.findClientId(ctx.channel()).orElseThrow();
        if (!frame.hasReportData()) {
            log.warn("Client {} sent BIZ_REPORT without reportData", clientId);
            return;
        }

        try {
            bizReportSink.accept(clientId, frame.getReportData());
            metrics.bizReportAccepted();
        } catch (RuntimeException e) {
            metrics.bizReportFailed();
            log.warn("Biz report sink failed: clientId={}, metric={}", clientId, frame.getReportData().getMetric(), e);
        }
    }
}
