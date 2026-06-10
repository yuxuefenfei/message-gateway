package com.gateway.push.metrics;

import lombok.Value;

import java.util.concurrent.atomic.LongAdder;

/**
 * 基于内存计数器的指标实现。
 *
 * <p>该实现使用 LongAdder，在高并发下比 AtomicLong 更适合做热点计数。
 * 它适合本地压测、单机观测或尚未接入 Micrometer/Prometheus 前的过渡阶段。</p>
 */
public final class InMemoryGatewayMetrics implements GatewayMetrics {
    private final LongAdder authenticatedChannels = new LongAdder();
    private final LongAdder unauthenticatedFrameRejects = new LongAdder();
    private final LongAdder unsupportedFrameRejects = new LongAdder();
    private final LongAdder authTimeoutCloses = new LongAdder();
    private final LongAdder authFailures = new LongAdder();
    private final LongAdder authTaskRejects = new LongAdder();
    private final LongAdder businessTaskRejects = new LongAdder();
    private final LongAdder idleCloses = new LongAdder();
    private final LongAdder decodeFailures = new LongAdder();
    private final LongAdder bizReportsAccepted = new LongAdder();
    private final LongAdder bizReportsFailed = new LongAdder();
    private final LongAdder pushSuccesses = new LongAdder();
    private final LongAdder pushFailures = new LongAdder();
    private final LongAdder pushBackpressureRejects = new LongAdder();

    @Override
    public void channelAuthenticated(String clientId) {
        authenticatedChannels.increment();
    }

    @Override
    public void unauthenticatedFrameRejected() {
        unauthenticatedFrameRejects.increment();
    }

    @Override
    public void unsupportedFrameRejected() {
        unsupportedFrameRejects.increment();
    }

    @Override
    public void authTimeoutClosed() {
        authTimeoutCloses.increment();
    }

    @Override
    public void authFailed() {
        authFailures.increment();
    }

    @Override
    public void authTaskRejected() {
        authTaskRejects.increment();
    }

    @Override
    public void businessTaskRejected() {
        businessTaskRejects.increment();
    }

    @Override
    public void idleClosed() {
        idleCloses.increment();
    }

    @Override
    public void decodeFailed() {
        decodeFailures.increment();
    }

    @Override
    public void bizReportAccepted() {
        bizReportsAccepted.increment();
    }

    @Override
    public void bizReportFailed() {
        bizReportsFailed.increment();
    }

    @Override
    public void pushSucceeded() {
        pushSuccesses.increment();
    }

    @Override
    public void pushFailed() {
        pushFailures.increment();
    }

    @Override
    public void pushRejectedBackpressure() {
        pushBackpressureRejects.increment();
    }

    public Snapshot snapshot() {
        return new Snapshot(
                authenticatedChannels.sum(),
                unauthenticatedFrameRejects.sum(),
                unsupportedFrameRejects.sum(),
                authTimeoutCloses.sum(),
                authFailures.sum(),
                authTaskRejects.sum(),
                businessTaskRejects.sum(),
                idleCloses.sum(),
                decodeFailures.sum(),
                bizReportsAccepted.sum(),
                bizReportsFailed.sum(),
                pushSuccesses.sum(),
                pushFailures.sum(),
                pushBackpressureRejects.sum()
        );
    }

    /**
     * 指标快照。
     *
     * <p>使用 Lombok @Value 生成不可变字段、构造器和 getter，调用方拿到快照后不会受后续计数变化影响。</p>
     */
    @Value
    public static class Snapshot {
        long authenticatedChannels;
        long unauthenticatedFrameRejects;
        long unsupportedFrameRejects;
        long authTimeoutCloses;
        long authFailures;
        long authTaskRejects;
        long businessTaskRejects;
        long idleCloses;
        long decodeFailures;
        long bizReportsAccepted;
        long bizReportsFailed;
        long pushSuccesses;
        long pushFailures;
        long pushBackpressureRejects;
    }
}
