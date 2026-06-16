package com.gateway.push.config;

import lombok.Getter;

import java.time.Duration;

/**
 * 网关运行配置。
 *
 * <p>这里集中管理 Netty 监听端口、WebSocket 路径、空闲检测、鉴权超时、
 * HTTP/WebSocket 消息大小限制、写缓冲水位以及业务线程池规模。集中配置的好处是：
 * 启动参数、测试构造和后续配置中心接入都可以复用同一份校验逻辑。</p>
 */
@Getter
public final class GatewayConfig {
    private static final int MIN_EXECUTOR_MAX_PENDING_TASKS = 16;
    /** 默认服务端口。 */
    private static final int DEFAULT_PORT = 8080;
    /** 默认 WebSocket 访问路径。 */
    private static final String DEFAULT_WEBSOCKET_PATH = "/ws";
    /** 客户端读空闲超时时间，超过该时间未收到任何数据则关闭连接。 */
    private static final Duration DEFAULT_READER_IDLE_TIMEOUT = Duration.ofSeconds(90);
    /** WebSocket 握手完成后，等待 CONNECT 鉴权帧的最长时间。 */
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    /** HTTP 握手阶段聚合请求体的默认上限。 */
    private static final int DEFAULT_MAX_HTTP_CONTENT_LENGTH = 64 * 1024;
    /** 单个 WebSocket 二进制帧的默认最大载荷。 */
    private static final int DEFAULT_MAX_WEBSOCKET_FRAME_BYTES = 64 * 1024;
    /** Netty 写缓冲低水位，低于该值时 channel 会重新变为 writable。 */
    private static final int DEFAULT_WRITE_BUFFER_LOW_WATER_MARK = 32 * 1024;
    /** Netty 写缓冲高水位，高于该值时 channel 会变为不可写，用于背压。 */
    private static final int DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK = 64 * 1024;
    /** 业务处理线程数，至少为 2，避免高频上报阻塞 IO 线程。 */
    private static final int DEFAULT_BUSINESS_EXECUTOR_THREADS = Math.max(2, Runtime.getRuntime().availableProcessors());
    /** 单个业务执行线程允许排队的最大任务数。 */
    private static final int DEFAULT_BUSINESS_EXECUTOR_MAX_PENDING_TASKS = 4096;
    /** 鉴权线程数，鉴权可能包含签名计算或远程调用。 */
    private static final int DEFAULT_AUTH_EXECUTOR_THREADS = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
    /** 单个鉴权执行线程允许排队的最大任务数。 */
    private static final int DEFAULT_AUTH_EXECUTOR_MAX_PENDING_TASKS = 1024;
    /** 批量推送每次写入事件循环的最大消息数。 */
    private static final int DEFAULT_PUSH_BATCH_CHUNK_SIZE = 64;

    /** Netty 监听端口，0 表示由系统自动分配端口，主要用于测试。 */
    private final int port;
    /** WebSocket 入口路径，例如 /ws 或 /push。 */
    private final String websocketPath;
    /** 读空闲关闭时间。 */
    private final Duration readerIdleTimeout;
    /** WebSocket 握手后 CONNECT 鉴权超时时间。 */
    private final Duration connectTimeout;
    /** HTTP 聚合大小上限。 */
    private final int maxHttpContentLength;
    /** WebSocket 二进制帧大小上限。 */
    private final int maxWebSocketFrameBytes;
    /** 写缓冲低水位。 */
    private final int writeBufferLowWaterMark;
    /** 写缓冲高水位。 */
    private final int writeBufferHighWaterMark;
    /** 业务线程池线程数。 */
    private final int businessExecutorThreads;
    /** 单个业务线程的最大待处理任务数。 */
    private final int businessExecutorMaxPendingTasks;
    /** 鉴权线程池线程数。 */
    private final int authExecutorThreads;
    /** 单个鉴权线程的最大待处理任务数。 */
    private final int authExecutorMaxPendingTasks;
    /** 批量推送分块大小。 */
    private final int pushBatchChunkSize;

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
                DEFAULT_BUSINESS_EXECUTOR_THREADS,
                DEFAULT_BUSINESS_EXECUTOR_MAX_PENDING_TASKS,
                DEFAULT_AUTH_EXECUTOR_THREADS,
                DEFAULT_AUTH_EXECUTOR_MAX_PENDING_TASKS,
                DEFAULT_PUSH_BATCH_CHUNK_SIZE
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
                DEFAULT_BUSINESS_EXECUTOR_THREADS,
                DEFAULT_BUSINESS_EXECUTOR_MAX_PENDING_TASKS,
                DEFAULT_AUTH_EXECUTOR_THREADS,
                DEFAULT_AUTH_EXECUTOR_MAX_PENDING_TASKS,
                DEFAULT_PUSH_BATCH_CHUNK_SIZE
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
        this(
                port,
                websocketPath,
                readerIdleTimeout,
                connectTimeout,
                maxHttpContentLength,
                maxWebSocketFrameBytes,
                writeBufferLowWaterMark,
                writeBufferHighWaterMark,
                businessExecutorThreads,
                DEFAULT_BUSINESS_EXECUTOR_MAX_PENDING_TASKS,
                DEFAULT_AUTH_EXECUTOR_THREADS,
                DEFAULT_AUTH_EXECUTOR_MAX_PENDING_TASKS,
                DEFAULT_PUSH_BATCH_CHUNK_SIZE
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
            int businessExecutorThreads,
            int businessExecutorMaxPendingTasks,
            int authExecutorThreads,
            int authExecutorMaxPendingTasks,
            int pushBatchChunkSize
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
        this.businessExecutorMaxPendingTasks = businessExecutorMaxPendingTasks;
        this.authExecutorThreads = authExecutorThreads;
        this.authExecutorMaxPendingTasks = authExecutorMaxPendingTasks;
        this.pushBatchChunkSize = pushBatchChunkSize;
        validate();
    }

    /**
     * 从 JVM 系统属性读取配置，未提供的项目使用类内默认值。
     *
     * <p>时间配置统一使用毫秒，既覆盖生产常见的秒级超时，也支持集成测试和低延迟场景。</p>
     */
    public static GatewayConfig fromSystemProperties() {
        int port = Integer.getInteger("gateway.port", DEFAULT_PORT);
        String websocketPath = System.getProperty("gateway.websocket.path", DEFAULT_WEBSOCKET_PATH);
        long idleMillis = Long.getLong("gateway.idle.millis", DEFAULT_READER_IDLE_TIMEOUT.toMillis());
        long connectTimeoutMillis = Long.getLong("gateway.connect.timeout.millis", DEFAULT_CONNECT_TIMEOUT.toMillis());
        int maxHttpContentLength = Integer.getInteger("gateway.max.http.content.bytes", DEFAULT_MAX_HTTP_CONTENT_LENGTH);
        int maxWebSocketFrameBytes = Integer.getInteger("gateway.max.websocket.frame.bytes", DEFAULT_MAX_WEBSOCKET_FRAME_BYTES);
        int writeBufferLowWaterMark = Integer.getInteger("gateway.write.buffer.low.bytes", DEFAULT_WRITE_BUFFER_LOW_WATER_MARK);
        int writeBufferHighWaterMark = Integer.getInteger("gateway.write.buffer.high.bytes", DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK);
        int businessExecutorThreads = Integer.getInteger("gateway.business.executor.threads", DEFAULT_BUSINESS_EXECUTOR_THREADS);
        int businessExecutorMaxPendingTasks = Integer.getInteger(
                "gateway.business.executor.max.pending.tasks",
                DEFAULT_BUSINESS_EXECUTOR_MAX_PENDING_TASKS);
        int authExecutorThreads = Integer.getInteger("gateway.auth.executor.threads", DEFAULT_AUTH_EXECUTOR_THREADS);
        int authExecutorMaxPendingTasks = Integer.getInteger(
                "gateway.auth.executor.max.pending.tasks",
                DEFAULT_AUTH_EXECUTOR_MAX_PENDING_TASKS);
        int pushBatchChunkSize = Integer.getInteger("gateway.push.batch.chunk.size", DEFAULT_PUSH_BATCH_CHUNK_SIZE);
        return new GatewayConfig(
                port,
                websocketPath,
                Duration.ofMillis(idleMillis),
                Duration.ofMillis(connectTimeoutMillis),
                maxHttpContentLength,
                maxWebSocketFrameBytes,
                writeBufferLowWaterMark,
                writeBufferHighWaterMark,
                businessExecutorThreads,
                businessExecutorMaxPendingTasks,
                authExecutorThreads,
                authExecutorMaxPendingTasks,
                pushBatchChunkSize
        );
    }

    /**
     * 对所有配置项做启动前校验。
     *
     * <p>这些校验尽量在服务启动前失败，而不是等到 Netty bind 或 handler 初始化阶段才暴露。
     * 对网关这类长连接服务来说，启动即失败比半启动状态更容易排查。</p>
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void validate() {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        if (websocketPath == null || websocketPath.isBlank() || !websocketPath.startsWith("/")) {
            throw new IllegalArgumentException("websocketPath must start with /");
        }
        if (readerIdleTimeout == null || readerIdleTimeout.compareTo(Duration.ofMillis(1)) < 0) {
            throw new IllegalArgumentException("readerIdleTimeout must be at least 1 millisecond");
        }
        if (connectTimeout == null || connectTimeout.compareTo(Duration.ofMillis(1)) < 0) {
            throw new IllegalArgumentException("connectTimeout must be at least 1 millisecond");
        }
        try {
            readerIdleTimeout.toMillis();
            connectTimeout.toMillis();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("timeouts are too large", e);
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
        if (businessExecutorMaxPendingTasks < MIN_EXECUTOR_MAX_PENDING_TASKS) {
            throw new IllegalArgumentException("businessExecutorMaxPendingTasks must be at least 16");
        }
        if (authExecutorThreads <= 0) {
            throw new IllegalArgumentException("authExecutorThreads must be positive");
        }
        if (authExecutorMaxPendingTasks < MIN_EXECUTOR_MAX_PENDING_TASKS) {
            throw new IllegalArgumentException("authExecutorMaxPendingTasks must be at least 16");
        }
        if (pushBatchChunkSize <= 0) {
            throw new IllegalArgumentException("pushBatchChunkSize must be positive");
        }
    }
}
