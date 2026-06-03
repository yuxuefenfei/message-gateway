package com.gateway.push.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryGatewayMetricsTest {
    @Test
    void capturesCountersInSnapshot() {
        InMemoryGatewayMetrics metrics = new InMemoryGatewayMetrics();

        metrics.channelAuthenticated("client-a");
        metrics.unauthenticatedFrameRejected();
        metrics.authTimeoutClosed();
        metrics.idleClosed();
        metrics.decodeFailed();
        metrics.bizReportAccepted();
        metrics.bizReportFailed();
        metrics.pushSucceeded();
        metrics.pushFailed();
        metrics.pushRejectedBackpressure();

        InMemoryGatewayMetrics.Snapshot snapshot = metrics.snapshot();
        assertEquals(1, snapshot.getAuthenticatedChannels());
        assertEquals(1, snapshot.getUnauthenticatedFrameRejects());
        assertEquals(1, snapshot.getAuthTimeoutCloses());
        assertEquals(1, snapshot.getIdleCloses());
        assertEquals(1, snapshot.getDecodeFailures());
        assertEquals(1, snapshot.getBizReportsAccepted());
        assertEquals(1, snapshot.getBizReportsFailed());
        assertEquals(1, snapshot.getPushSuccesses());
        assertEquals(1, snapshot.getPushFailures());
        assertEquals(1, snapshot.getPushBackpressureRejects());
    }
}
