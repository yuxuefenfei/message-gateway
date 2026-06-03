package com.gateway.push.config;

import java.time.Duration;

public final class GatewayConfig {
    private static final int DEFAULT_PORT = 8080;
    private static final String DEFAULT_WEBSOCKET_PATH = "/ws";
    private static final Duration DEFAULT_READER_IDLE_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final int DEFAULT_MAX_HTTP_CONTENT_LENGTH = 64 * 1024;
    private static final int DEFAULT_MAX_WEBSOCKET_FRAME_BYTES = 64 * 1024;
    private static final int DEFAULT_WRITE_BUFFER_LOW_WATER_MARK = 32 * 1024;
    private static final int DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK = 64 * 1024;
    private static final int DEFAULT_BUSINESS_EXECUTOR_THREADS = Math.max(2, Runtime.getRuntime().availableProcessors());

    private final int port;
    private final String websocketPath;
    private final Duration readerIdleTimeout;
    private final Duration connectTimeout;
    private final int maxHttpContentLength;
    private final int maxWebSocketFrameBytes;
    private final int writeBufferLowWaterMark;
    private final int writeBufferHighWaterMark;
    private final int businessExecutorThreads;

    public GatewayConfig(int port, String websocketPath, Duration readerIdleTimeout) {
        this(
                port,
                websocketPath,
                readerIdleTimeout,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_MAX_HTTP_CONTENT_LENGTH,
                DEFAULT_MAX_WEBSOCKET_FRAME_BYTES,
                DEFAULT_WRITE_BUFFER_LOW_WATER_MARK,
                DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK,
                DEFAULT_BUSINESS_EXECUTOR_THREADS
        );
    }

    public GatewayConfig(
            int port,
            String websocketPath,
            Duration readerIdleTimeout,
            Duration connectTimeout,
            int maxHttpContentLength,
            int maxWebSocketFrameBytes
    ) {
        this(
                port,
                websocketPath,
                readerIdleTimeout,
                connectTimeout,
                maxHttpContentLength,
                maxWebSocketFrameBytes,
                DEFAULT_WRITE_BUFFER_LOW_WATER_MARK,
                DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK,
                DEFAULT_BUSINESS_EXECUTOR_THREADS
        );
    }

    public GatewayConfig(
            int port,
            String websocketPath,
            Duration readerIdleTimeout,
            Duration connectTimeout,
            int maxHttpContentLength,
            int maxWebSocketFrameBytes,
            int writeBufferLowWaterMark,
            int writeBufferHighWaterMark,
            int businessExecutorThreads
    ) {
        this.port = port;
        this.websocketPath = websocketPath;
        this.readerIdleTimeout = readerIdleTimeout;
        this.connectTimeout = connectTimeout;
        this.maxHttpContentLength = maxHttpContentLength;
        this.maxWebSocketFrameBytes = maxWebSocketFrameBytes;
        this.writeBufferLowWaterMark = writeBufferLowWaterMark;
        this.writeBufferHighWaterMark = writeBufferHighWaterMark;
        this.businessExecutorThreads = businessExecutorThreads;
        validate();
    }

    public static GatewayConfig fromSystemProperties() {
        int port = Integer.getInteger("gateway.port", DEFAULT_PORT);
        String websocketPath = System.getProperty("gateway.websocket.path", DEFAULT_WEBSOCKET_PATH);
        long idleSeconds = Long.getLong("gateway.idle.seconds", DEFAULT_READER_IDLE_TIMEOUT.getSeconds());
        long connectTimeoutSeconds = Long.getLong("gateway.connect.timeout.seconds", DEFAULT_CONNECT_TIMEOUT.getSeconds());
        int maxHttpContentLength = Integer.getInteger("gateway.max.http.content.bytes", DEFAULT_MAX_HTTP_CONTENT_LENGTH);
        int maxWebSocketFrameBytes = Integer.getInteger("gateway.max.websocket.frame.bytes", DEFAULT_MAX_WEBSOCKET_FRAME_BYTES);
        int writeBufferLowWaterMark = Integer.getInteger("gateway.write.buffer.low.bytes", DEFAULT_WRITE_BUFFER_LOW_WATER_MARK);
        int writeBufferHighWaterMark = Integer.getInteger("gateway.write.buffer.high.bytes", DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK);
        int businessExecutorThreads = Integer.getInteger("gateway.business.executor.threads", DEFAULT_BUSINESS_EXECUTOR_THREADS);
        return new GatewayConfig(
                port,
                websocketPath,
                Duration.ofSeconds(idleSeconds),
                Duration.ofSeconds(connectTimeoutSeconds),
                maxHttpContentLength,
                maxWebSocketFrameBytes,
                writeBufferLowWaterMark,
                writeBufferHighWaterMark,
                businessExecutorThreads
        );
    }

    public int getPort() {
        return port;
    }

    public String getWebsocketPath() {
        return websocketPath;
    }

    public Duration getReaderIdleTimeout() {
        return readerIdleTimeout;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public int getMaxHttpContentLength() {
        return maxHttpContentLength;
    }

    public int getMaxWebSocketFrameBytes() {
        return maxWebSocketFrameBytes;
    }

    public int getWriteBufferLowWaterMark() {
        return writeBufferLowWaterMark;
    }

    public int getWriteBufferHighWaterMark() {
        return writeBufferHighWaterMark;
    }

    public int getBusinessExecutorThreads() {
        return businessExecutorThreads;
    }

    private void validate() {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        if (websocketPath == null || websocketPath.isBlank() || !websocketPath.startsWith("/")) {
            throw new IllegalArgumentException("websocketPath must start with /");
        }
        if (readerIdleTimeout == null || readerIdleTimeout.isNegative() || readerIdleTimeout.isZero()) {
            throw new IllegalArgumentException("readerIdleTimeout must be positive");
        }
        if (connectTimeout == null || connectTimeout.isNegative() || connectTimeout.isZero()) {
            throw new IllegalArgumentException("connectTimeout must be positive");
        }
        if (maxHttpContentLength <= 0) {
            throw new IllegalArgumentException("maxHttpContentLength must be positive");
        }
        if (maxWebSocketFrameBytes <= 0) {
            throw new IllegalArgumentException("maxWebSocketFrameBytes must be positive");
        }
        if (writeBufferLowWaterMark < 0) {
            throw new IllegalArgumentException("writeBufferLowWaterMark must be non-negative");
        }
        if (writeBufferHighWaterMark <= writeBufferLowWaterMark) {
            throw new IllegalArgumentException("writeBufferHighWaterMark must be greater than writeBufferLowWaterMark");
        }
        if (businessExecutorThreads <= 0) {
            throw new IllegalArgumentException("businessExecutorThreads must be positive");
        }
    }
}
