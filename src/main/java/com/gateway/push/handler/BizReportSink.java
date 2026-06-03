package com.gateway.push.handler;

import com.gateway.push.protocol.ReportData;

@FunctionalInterface
public interface BizReportSink {
    void accept(String clientId, ReportData reportData);

    static BizReportSink noop() {
        return (clientId, reportData) -> {
        };
    }
}
