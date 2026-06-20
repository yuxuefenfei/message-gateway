package com.gateway.push.bench;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.gateway.push.auth.TokenAuthenticator;
import com.gateway.push.codec.WebSocketProtobufDecoder;
import com.gateway.push.codec.WebSocketProtobufEncoder;
import com.gateway.push.config.GatewayConfig;
import com.gateway.push.metrics.InMemoryGatewayMetrics;
import com.gateway.push.protocol.ConnectRequest;
import com.gateway.push.protocol.Frame;
import com.gateway.push.protocol.ReportData;
import com.gateway.push.server.GatewayServer;
import com.gateway.push.session.SessionRegistry;
import com.google.protobuf.ByteString;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本机真实协议压测入口。
 *
 * <p>该 runner 会启动真实 GatewayServer，再用 Netty WebSocket 客户端建立真实连接，
 * 发送 Protobuf CONNECT、PING 和 BIZ_REPORT。它不替代专业压测平台，但适合在开发机上
 * 快速观察单机网关在当前配置下的连接建立、心跳延迟和业务上报吞吐。</p>
 */
public final class GatewayLocalLoadRunner {
    private GatewayLocalLoadRunner() {
    }

    public static void main(String[] args) throws Exception {
        quietLogs();
        Options options = Options.parse(args);
        printEnvironment(options);

        InMemoryGatewayMetrics metrics = new InMemoryGatewayMetrics();
        SessionRegistry registry = new SessionRegistry();
        GatewayServer server = new GatewayServer(
                config(options),
                registry,
                TokenAuthenticator.nonBlankToken(),
                (clientId, reportData) -> {
                },
                metrics);
        NioEventLoopGroup clientGroup = new NioEventLoopGroup(options.clientThreads);
        List<ClientSession> sessions = new ArrayList<>(options.connections);

        try {
            server.start();
            URI uri = URI.create("ws://127.0.0.1:" + server.getPort() + "/ws");
            Bootstrap baseBootstrap = new Bootstrap()
                    .group(clientGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10_000);

            long connectStart = System.nanoTime();
            for (int i = 0; i < options.connections; i++) {
                ClientSession session = new ClientSession(i);
                sessions.add(session);
                Bootstrap bootstrap = baseBootstrap.clone();
                bootstrap.handler(new ClientInitializer(uri, session));
                session.channel = bootstrap.connect(uri.getHost(), uri.getPort()).sync().channel();
            }

            for (ClientSession session : sessions) {
                session.handshake.get(10, TimeUnit.SECONDS);
            }
            for (ClientSession session : sessions) {
                session.channel.writeAndFlush(connectFrame(session.index)).sync();
            }
            int authenticated = 0;
            for (ClientSession session : sessions) {
                if (session.connectAck.get(10, TimeUnit.SECONDS) == 200) {
                    authenticated++;
                }
            }
            long connectElapsed = System.nanoTime() - connectStart;
            System.out.printf(Locale.ROOT,
                    "CONNECT connections=%d authenticated=%d elapsedMs=%.2f rate=%.2f conn/s%n",
                    options.connections,
                    authenticated,
                    millis(connectElapsed),
                    options.connections / seconds(connectElapsed));

            if (options.pingMessagesPerConnection > 0) {
                runPingLoad(sessions, options);
            }
            if (options.reportMessagesPerConnection > 0) {
                runReportLoad(sessions, options, metrics);
            }

            InMemoryGatewayMetrics.Snapshot snapshot = metrics.snapshot();
            System.out.printf(Locale.ROOT,
                    "METRICS authenticated=%d bizAccepted=%d bizFailed=%d pushBackpressureRejects=%d%n",
                    snapshot.getAuthenticatedChannels(),
                    snapshot.getBizReportsAccepted(),
                    snapshot.getBizReportsFailed(),
                    snapshot.getPushBackpressureRejects());
            printMemory("AFTER");
            printGc();
        } finally {
            for (ClientSession session : sessions) {
                if (session.channel != null) {
                    session.channel.close().syncUninterruptibly();
                }
            }
            clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            server.stop();
        }
    }

    private static GatewayConfig config(Options options) {
        int processors = Runtime.getRuntime().availableProcessors();
        return new GatewayConfig(
                0,
                "/ws",
                Duration.ofSeconds(120),
                Duration.ofSeconds(30),
                64 * 1024,
                64 * 1024,
                512 * 1024,
                1024 * 1024,
                Math.max(2, processors),
                options.executorMaxPendingTasks,
                Math.max(2, processors / 2),
                options.executorMaxPendingTasks,
                256);
    }

    private static void runPingLoad(List<ClientSession> sessions, Options options) throws Exception {
        int total = checkedTotal(options.connections, options.pingMessagesPerConnection, "ping");
        CountDownLatch pongLatch = new CountDownLatch(total);
        LatencyRecorder latencies = new LatencyRecorder(total);
        for (ClientSession session : sessions) {
            session.pongLatch = pongLatch;
            session.latencies = latencies;
        }

        long start = System.nanoTime();
        for (int message = 0; message < options.pingMessagesPerConnection; message++) {
            for (ClientSession session : sessions) {
                session.sendPing(message);
            }
            for (ClientSession session : sessions) {
                session.channel.flush();
            }
        }

        boolean completed = pongLatch.await(options.awaitSeconds, TimeUnit.SECONDS);
        long elapsed = System.nanoTime() - start;
        LatencySnapshot snapshot = latencies.snapshot();
        System.out.printf(Locale.ROOT,
                "PING sent=%d received=%d completed=%s elapsedMs=%.2f throughput=%.2f msg/s " +
                        "latencyMs[p50=%.3f p90=%.3f p95=%.3f p99=%.3f max=%.3f]%n",
                total,
                snapshot.count,
                completed,
                millis(elapsed),
                snapshot.count / seconds(elapsed),
                nanosToMillis(snapshot.p50),
                nanosToMillis(snapshot.p90),
                nanosToMillis(snapshot.p95),
                nanosToMillis(snapshot.p99),
                nanosToMillis(snapshot.max));
    }

    private static void runReportLoad(
            List<ClientSession> sessions,
            Options options,
            InMemoryGatewayMetrics metrics
    ) throws Exception {
        int total = checkedTotal(options.connections, options.reportMessagesPerConnection, "report");
        ByteString payload = ByteString.copyFrom(new byte[options.reportPayloadBytes]);
        Frame[] frames = new Frame[options.reportMessagesPerConnection];
        for (int i = 0; i < frames.length; i++) {
            frames[i] = Frame.newBuilder()
                    .setType(Frame.Type.BIZ_REPORT)
                    .setReportData(ReportData.newBuilder()
                            .setMetric("load-" + i)
                            .setData(payload)
                            .build())
                    .build();
        }

        long start = System.nanoTime();
        for (int message = 0; message < options.reportMessagesPerConnection; message++) {
            Frame frame = frames[message];
            for (ClientSession session : sessions) {
                session.channel.write(frame);
            }
            for (ClientSession session : sessions) {
                session.channel.flush();
            }
        }

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(options.awaitSeconds);
        long accepted;
        do {
            accepted = metrics.snapshot().getBizReportsAccepted();
            if (accepted >= total) {
                break;
            }
            Thread.sleep(10);
        } while (System.nanoTime() < deadline);

        long elapsed = System.nanoTime() - start;
        InMemoryGatewayMetrics.Snapshot snapshot = metrics.snapshot();
        System.out.printf(Locale.ROOT,
                "BIZ_REPORT sent=%d accepted=%d failed=%d rejected=%d payloadBytes=%d elapsedMs=%.2f throughput=%.2f msg/s%n",
                total,
                snapshot.getBizReportsAccepted(),
                snapshot.getBizReportsFailed(),
                snapshot.getBusinessTaskRejects(),
                options.reportPayloadBytes,
                millis(elapsed),
                snapshot.getBizReportsAccepted() / seconds(elapsed));
    }

    private static Frame connectFrame(int index) {
        return Frame.newBuilder()
                .setType(Frame.Type.CONNECT)
                .setSequenceId("connect-" + index)
                .setConnectRequest(ConnectRequest.newBuilder()
                        .setToken("token")
                        .setClientId("load-client-" + index)
                        .setClientVersion("bench")
                        .build())
                .build();
    }

    private static int checkedTotal(int connections, int messagesPerConnection, String name) {
        long total = (long) connections * messagesPerConnection;
        if (total > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " total messages is too large for this runner: " + total);
        }
        return (int) total;
    }

    private static void quietLogs() {
        try {
            Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            root.setLevel(Level.WARN);
            ((Logger) LoggerFactory.getLogger("com.gateway.push")).setLevel(Level.WARN);
        } catch (RuntimeException ignored) {
            // Logging setup is best effort; benchmark correctness does not depend on it.
        }
    }

    private static void printEnvironment(Options options) {
        Runtime runtime = Runtime.getRuntime();
        System.out.printf(Locale.ROOT,
                "ENV java=%s os=%s arch=%s processors=%d maxHeapMB=%d%n",
                System.getProperty("java.version"),
                System.getProperty("os.name") + " " + System.getProperty("os.version"),
                System.getProperty("os.arch"),
                runtime.availableProcessors(),
                runtime.maxMemory() / 1024 / 1024);
        printMemory("BEFORE");
        System.out.printf(Locale.ROOT,
                "OPTIONS connections=%d clientThreads=%d pingPerConnection=%d reportPerConnection=%d " +
                        "reportPayloadBytes=%d awaitSeconds=%d executorMaxPendingTasks=%d%n",
                options.connections,
                options.clientThreads,
                options.pingMessagesPerConnection,
                options.reportMessagesPerConnection,
                options.reportPayloadBytes,
                options.awaitSeconds,
                options.executorMaxPendingTasks);
    }

    private static void printMemory(String phase) {
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = memory.getHeapMemoryUsage();
        MemoryUsage nonHeap = memory.getNonHeapMemoryUsage();
        System.out.printf(Locale.ROOT,
                "%s_MEMORY heapUsedMB=%d heapCommittedMB=%d nonHeapUsedMB=%d%n",
                phase,
                heap.getUsed() / 1024 / 1024,
                heap.getCommitted() / 1024 / 1024,
                nonHeap.getUsed() / 1024 / 1024);
    }

    private static void printGc() {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            System.out.printf(Locale.ROOT,
                    "GC name=\"%s\" count=%d timeMs=%d%n",
                    bean.getName(),
                    bean.getCollectionCount(),
                    bean.getCollectionTime());
        }
    }

    private static double millis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static double seconds(long nanos) {
        return nanos / 1_000_000_000.0;
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static final class ClientInitializer extends ChannelInitializer<SocketChannel> {
        private final URI uri;
        private final ClientSession session;

        private ClientInitializer(URI uri, ClientSession session) {
            this.uri = uri;
            this.session = session;
        }

        @Override
        protected void initChannel(SocketChannel ch) {
            ch.pipeline()
                    .addLast(new HttpClientCodec())
                    .addLast(new HttpObjectAggregator(64 * 1024))
                    .addLast(new WebSocketClientProtocolHandler(WebSocketClientHandshakerFactory.newHandshaker(
                            uri,
                            WebSocketVersion.V13,
                            null,
                            true,
                            new DefaultHttpHeaders())))
                    .addLast(new WebSocketProtobufDecoder())
                    .addLast(new WebSocketProtobufEncoder())
                    .addLast(new ClientHandler(session));
        }
    }

    private static final class ClientHandler extends SimpleChannelInboundHandler<Frame> {
        private final ClientSession session;

        private ClientHandler(ClientSession session) {
            this.session = session;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            if (frame.getType() == Frame.Type.CONNECT_ACK) {
                session.connectAck.complete(frame.getConnectResponse().getCode());
                return;
            }
            if (frame.getType() == Frame.Type.PONG) {
                Long sentAt = session.sentPings.remove();
                session.latencies.record(System.nanoTime() - sentAt);
                session.pongLatch.countDown();
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                session.handshake.complete(null);
                return;
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            session.handshake.completeExceptionally(cause);
            session.connectAck.completeExceptionally(cause);
            ctx.close();
        }
    }

    private static final class ClientSession {
        private final int index;
        private final CompletableFuture<Void> handshake = new CompletableFuture<>();
        private final CompletableFuture<Integer> connectAck = new CompletableFuture<>();
        private final java.util.Queue<Long> sentPings = new java.util.concurrent.ConcurrentLinkedQueue<>();
        private volatile Channel channel;
        private volatile CountDownLatch pongLatch;
        private volatile LatencyRecorder latencies;

        private ClientSession(int index) {
            this.index = index;
        }

        private void sendPing(int messageIndex) {
            sentPings.add(System.nanoTime());
            channel.write(Frame.newBuilder()
                    .setType(Frame.Type.PING)
                    .setSequenceId(index + "-" + messageIndex)
                    .build());
        }
    }

    private static final class LatencyRecorder {
        private final long[] values;
        private final AtomicInteger count = new AtomicInteger();

        private LatencyRecorder(int total) {
            this.values = new long[total];
        }

        private void record(long nanos) {
            int index = count.getAndIncrement();
            if (index < values.length) {
                values[index] = nanos;
            }
        }

        private LatencySnapshot snapshot() {
            int actual = Math.min(count.get(), values.length);
            if (actual == 0) {
                return new LatencySnapshot(0, 0, 0, 0, 0, 0);
            }
            long[] copy = Arrays.copyOf(values, actual);
            Arrays.sort(copy);
            return new LatencySnapshot(
                    actual,
                    percentile(copy, 0.50),
                    percentile(copy, 0.90),
                    percentile(copy, 0.95),
                    percentile(copy, 0.99),
                    copy[copy.length - 1]);
        }

        private static long percentile(long[] sorted, double percentile) {
            int index = (int) Math.ceil(percentile * sorted.length) - 1;
            return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
        }
    }

    private static final class LatencySnapshot {
        private final int count;
        private final long p50;
        private final long p90;
        private final long p95;
        private final long p99;
        private final long max;

        private LatencySnapshot(int count, long p50, long p90, long p95, long p99, long max) {
            this.count = count;
            this.p50 = p50;
            this.p90 = p90;
            this.p95 = p95;
            this.p99 = p99;
            this.max = max;
        }
    }

    private static final class Options {
        private int connections = 500;
        private int clientThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        private int pingMessagesPerConnection = 100;
        private int reportMessagesPerConnection = 100;
        private int reportPayloadBytes = 128;
        private int awaitSeconds = 60;
        private int executorMaxPendingTasks = 262_144;

        private static Options parse(String[] args) {
            Options options = new Options();
            Map<String, Setter> setters = Map.of(
                    "--connections", value -> options.connections = positiveInt("--connections", value),
                    "--client-threads", value -> options.clientThreads = positiveInt("--client-threads", value),
                    "--ping-per-connection", value -> options.pingMessagesPerConnection = nonNegativeInt("--ping-per-connection", value),
                    "--report-per-connection", value -> options.reportMessagesPerConnection = nonNegativeInt("--report-per-connection", value),
                    "--report-payload-bytes", value -> options.reportPayloadBytes = nonNegativeInt("--report-payload-bytes", value),
                    "--await-seconds", value -> options.awaitSeconds = positiveInt("--await-seconds", value),
                    "--executor-max-pending-tasks", value -> options.executorMaxPendingTasks = positiveInt("--executor-max-pending-tasks", value));
            for (int i = 0; i < args.length; i += 2) {
                Setter setter = setters.get(args[i]);
                if (setter == null || i + 1 >= args.length) {
                    throw new IllegalArgumentException("Unknown or incomplete option: " + args[i]);
                }
                setter.set(args[i + 1]);
            }
            return options;
        }

        private static int positiveInt(String name, String value) {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return parsed;
        }

        private static int nonNegativeInt(String name, String value) {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new IllegalArgumentException(name + " must be non-negative");
            }
            return parsed;
        }
    }

    @FunctionalInterface
    private interface Setter {
        void set(String value);
    }
}
