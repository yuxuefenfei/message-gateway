package com.gateway.push.handler;

import com.gateway.push.protocol.ReportData;

/**
 * 业务上报落地接口。
 *
 * <p>网关本身不应该做重业务计算，本接口用于把设备指标、客户端状态等 BIZ_REPORT
 * 投递到 Kafka、队列、日志系统或业务服务。实现方应尽量保证非阻塞，或者依赖
 * BizReportHandler 所在的业务线程池隔离阻塞风险。</p>
 */
@FunctionalInterface
public interface BizReportSink {
    void accept(String clientId, ReportData reportData);

    static BizReportSink noop() {
        return (clientId, reportData) -> {
        };
    }
}
