package com.gateway.push.metrics;

import java.util.concurrent.atomic.LongAdder;

public final class InMemoryGatewayMetrics implements GatewayMetrics {
    private final LongAdder authenticatedChannels = new LongAdder();
    private final LongAdder unauthenticatedFrameRejects = new LongAdder();
    private final LongAdder authTimeoutCloses = new LongAdder();
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
    public void authTimeoutClosed() {
        authTimeoutCloses.increment();
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
                authTimeoutCloses.sum(),
                idleCloses.sum(),
                decodeFailures.sum(),
                bizReportsAccepted.sum(),
                bizReportsFailed.sum(),
                pushSuccesses.sum(),
                pushFailures.sum(),
                pushBackpressureRejects.sum()
        );
    }

    public static final class Snapshot {
        private final long authenticatedChannels;
        private final long unauthenticatedFrameRejects;
        private final long authTimeoutCloses;
        private final long idleCloses;
        private final long decodeFailures;
        private final long bizReportsAccepted;
        private final long bizReportsFailed;
        private final long pushSuccesses;
        private final long pushFailures;
        private final long pushBackpressureRejects;

        private Snapshot(
                long authenticatedChannels,
                long unauthenticatedFrameRejects,
                long authTimeoutCloses,
                long idleCloses,
                long decodeFailures,
                long bizReportsAccepted,
                long bizReportsFailed,
                long pushSuccesses,
                long pushFailures,
                long pushBackpressureRejects
        ) {
            this.authenticatedChannels = authenticatedChannels;
            this.unauthenticatedFrameRejects = unauthenticatedFrameRejects;
            this.authTimeoutCloses = authTimeoutCloses;
            this.idleCloses = idleCloses;
            this.decodeFailures = decodeFailures;
            this.bizReportsAccepted = bizReportsAccepted;
            this.bizReportsFailed = bizReportsFailed;
            this.pushSuccesses = pushSuccesses;
            this.pushFailures = pushFailures;
            this.pushBackpressureRejects = pushBackpressureRejects;
        }

        public long getAuthenticatedChannels() {
            return authenticatedChannels;
        }

        public long getUnauthenticatedFrameRejects() {
            return unauthenticatedFrameRejects;
        }

        public long getAuthTimeoutCloses() {
            return authTimeoutCloses;
        }

        public long getIdleCloses() {
            return idleCloses;
        }

        public long getDecodeFailures() {
            return decodeFailures;
        }

        public long getBizReportsAccepted() {
            return bizReportsAccepted;
        }

        public long getBizReportsFailed() {
            return bizReportsFailed;
        }

        public long getPushSuccesses() {
            return pushSuccesses;
        }

        public long getPushFailures() {
            return pushFailures;
        }

        public long getPushBackpressureRejects() {
            return pushBackpressureRejects;
        }
    }
}
