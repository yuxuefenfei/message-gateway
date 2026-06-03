package com.gateway.push.metrics;

/**
 * 网关指标扩展接口。
 *
 * <p>主流程只依赖这个轻量接口，不直接绑定 Micrometer、Prometheus 或其它监控库。
 * 默认方法全部为空，业务方可以按需覆盖关注的指标，也可以使用 InMemoryGatewayMetrics
 * 在压测阶段快速观察计数。</p>
 */
public interface GatewayMetrics {
    static GatewayMetrics noop() {
        return new GatewayMetrics() {
        };
    }

    default void channelAuthenticated(String clientId) {
    }

    default void unauthenticatedFrameRejected() {
    }

    default void unsupportedFrameRejected() {
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
