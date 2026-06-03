package com.gateway.push.metrics;

public interface GatewayMetrics {
    static GatewayMetrics noop() {
        return new GatewayMetrics() {
        };
    }

    default void channelAuthenticated(String clientId) {
    }

    default void unauthenticatedFrameRejected() {
    }

    default void authTimeoutClosed() {
    }

    default void idleClosed() {
    }

    default void decodeFailed() {
    }

    default void bizReportAccepted() {
    }

    default void bizReportFailed() {
    }

    default void pushSucceeded() {
    }

    default void pushFailed() {
    }

    default void pushRejectedBackpressure() {
    }
}
