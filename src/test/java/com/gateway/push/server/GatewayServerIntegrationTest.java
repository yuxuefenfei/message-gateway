package com.gateway.push.server;

import com.gateway.push.auth.TokenAuthenticator;
import com.gateway.push.codec.WebSocketProtobufDecoder;
import com.gateway.push.codec.WebSocketProtobufEncoder;
import com.gateway.push.config.GatewayConfig;
import com.gateway.push.protocol.ConnectRequest;
import com.gateway.push.protocol.Frame;
import com.gateway.push.protocol.ReportData;
import com.gateway.push.metrics.GatewayMetrics;
import com.google.protobuf.ByteString;
import com.gateway.push.session.SessionRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
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
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayServerIntegrationTest {
    @Test
    void acceptsWebSocketProtobufConnectAndPingEndToEnd() throws Exception {
        SessionRegistry registry = new SessionRegistry();
        GatewayServer server = new GatewayServer(
                new GatewayConfig(0, "/ws", Duration.ofSeconds(30)),
                registry,
                TokenAuthenticator.nonBlankToken());
        EventLoopGroup clientGroup = new NioEventLoopGroup(1);

        try {
            server.start();
            TestClientHandler clientHandler = new TestClientHandler();
            URI uri = URI.create("ws://127.0.0.1:" + server.getPort() + "/ws");
            Channel clientChannel = connectClient(clientGroup, clientHandler, uri);

            assertTrue(clientHandler.awaitHandshake());

            clientChannel.writeAndFlush(Frame.newBuilder()
                    .setType(Frame.Type.CONNECT)
                    .setSequenceId("connect-e2e")
                    .setConnectRequest(ConnectRequest.newBuilder()
                            .setToken("token")
                            .setClientId("client-e2e")
                            .setClientVersion("1.0.0")
                            .build())
                    .build()).sync();

            Frame ack = clientHandler.takeFrame();
            assertEquals(Frame.Type.CONNECT_ACK, ack.getType());
            assertEquals(200, ack.getConnectResponse().getCode());
            assertTrue(registry.findChannel("client-e2e").isPresent());

            clientChannel.writeAndFlush(Frame.newBuilder()
                    .setType(Frame.Type.PING)
                    .setSequenceId("ping-e2e")
                    .build()).sync();

            Frame pong = clientHandler.takeFrame();
            assertEquals(Frame.Type.PONG, pong.getType());
            assertEquals("ping-e2e", pong.getSequenceId());

            clientChannel.close().sync();
        } finally {
            clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            server.stop();
        }
    }

    @Test
    void handlesBizReportOnBusinessExecutorThread() throws Exception {
        CountDownLatch reportHandled = new CountDownLatch(1);
        AtomicReference<String> handlerThreadName = new AtomicReference<>();
        GatewayServer server = new GatewayServer(
                new GatewayConfig(0, "/ws", Duration.ofSeconds(30)),
                new SessionRegistry(),
                TokenAuthenticator.nonBlankToken(),
                (clientId, reportData) -> {
                    handlerThreadName.set(Thread.currentThread().getName());
                    reportHandled.countDown();
                });
        EventLoopGroup clientGroup = new NioEventLoopGroup(1);

        try {
            server.start();
            TestClientHandler clientHandler = new TestClientHandler();
            URI uri = URI.create("ws://127.0.0.1:" + server.getPort() + "/ws");
            Channel clientChannel = connectClient(clientGroup, clientHandler, uri);

            assertTrue(clientHandler.awaitHandshake());
            clientChannel.writeAndFlush(Frame.newBuilder()
                    .setType(Frame.Type.CONNECT)
                    .setSequenceId("connect-report")
                    .setConnectRequest(ConnectRequest.newBuilder()
                            .setToken("token")
                            .setClientId("client-report")
                            .build())
                    .build()).sync();
            clientHandler.takeFrame();

            clientChannel.writeAndFlush(Frame.newBuilder()
                    .setType(Frame.Type.BIZ_REPORT)
                    .setReportData(ReportData.newBuilder()
                            .setMetric("temperature")
                            .setData(ByteString.copyFromUtf8("36.5"))
                            .build())
                    .build()).sync();

            assertTrue(reportHandled.await(5, TimeUnit.SECONDS));
            assertTrue(handlerThreadName.get().startsWith("gateway-business"));

            clientChannel.close().sync();
        } finally {
            clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            server.stop();
        }
    }

    @Test
    void sendsConnectAckBeforeClosingUnauthorizedConnection() throws Exception {
        GatewayServer server = new GatewayServer(
                new GatewayConfig(0, "/ws", Duration.ofSeconds(30)),
                new SessionRegistry(),
                TokenAuthenticator.nonBlankToken());
        EventLoopGroup clientGroup = new NioEventLoopGroup(1);

        try {
            server.start();
            TestClientHandler clientHandler = new TestClientHandler();
            URI uri = URI.create("ws://127.0.0.1:" + server.getPort() + "/ws");
            Channel clientChannel = connectClient(clientGroup, clientHandler, uri);

            assertTrue(clientHandler.awaitHandshake());

            clientChannel.writeAndFlush(Frame.newBuilder()
                    .setType(Frame.Type.CONNECT)
                    .setSequenceId("connect-unauthorized")
                    .setConnectRequest(ConnectRequest.newBuilder()
                            .setClientId("client-unauthorized")
                            .build())
                    .build()).sync();

            Frame ack = clientHandler.takeFrame();
            assertEquals(Frame.Type.CONNECT_ACK, ack.getType());
            assertEquals(401, ack.getConnectResponse().getCode());
            assertTrue(clientHandler.awaitClose());
        } finally {
            clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            server.stop();
        }
    }

    @Test
    void closesConnectionWhenConnectFrameIsNotReceivedBeforeTimeout() throws Exception {
        GatewayServer server = new GatewayServer(
                new GatewayConfig(
                        0,
                        "/ws",
                        Duration.ofSeconds(30),
                        Duration.ofMillis(100),
                        64 * 1024,
                        64 * 1024),
                new SessionRegistry(),
                TokenAuthenticator.nonBlankToken());
        EventLoopGroup clientGroup = new NioEventLoopGroup(1);

        try {
            server.start();
            TestClientHandler clientHandler = new TestClientHandler();
            URI uri = URI.create("ws://127.0.0.1:" + server.getPort() + "/ws");
            connectClient(clientGroup, clientHandler, uri);

            assertTrue(clientHandler.awaitHandshake());
            assertTrue(clientHandler.awaitClose());
        } finally {
            clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            server.stop();
        }
    }

    @Test
    void rejectsRepeatedStartWithoutStop() throws Exception {
        GatewayServer server = new GatewayServer(
                new GatewayConfig(0, "/ws", Duration.ofSeconds(30)),
                new SessionRegistry(),
                TokenAuthenticator.nonBlankToken());

        try {
            server.start();
            assertThrows(IllegalStateException.class, server::start);
        } finally {
            server.stop();
        }
    }

    @Test
    void aggregatesFragmentedBinaryWebSocketMessage() throws Exception {
        SessionRegistry registry = new SessionRegistry();
        GatewayServer server = new GatewayServer(
                new GatewayConfig(0, "/ws", Duration.ofSeconds(30)),
                registry,
                TokenAuthenticator.nonBlankToken());
        EventLoopGroup clientGroup = new NioEventLoopGroup(1);

        try {
            server.start();
            TestClientHandler clientHandler = new TestClientHandler();
            URI uri = URI.create("ws://127.0.0.1:" + server.getPort() + "/ws");
            Channel clientChannel = connectClient(clientGroup, clientHandler, uri);
            assertTrue(clientHandler.awaitHandshake());

            byte[] payload = Frame.newBuilder()
                    .setType(Frame.Type.CONNECT)
                    .setSequenceId("fragmented-connect")
                    .setConnectRequest(ConnectRequest.newBuilder()
                            .setToken("token")
                            .setClientId("client-fragmented")
                            .build())
                    .build()
                    .toByteArray();
            int split = payload.length / 2;
            clientChannel.write(new BinaryWebSocketFrame(false, 0, Unpooled.wrappedBuffer(payload, 0, split)));
            clientChannel.writeAndFlush(new ContinuationWebSocketFrame(
                    true,
                    0,
                    Unpooled.wrappedBuffer(payload, split, payload.length - split))).sync();

            assertEquals(200, clientHandler.takeFrame().getConnectResponse().getCode());
            assertTrue(registry.findChannel("client-fragmented").isPresent());
            clientChannel.close().sync();
        } finally {
            clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            server.stop();
        }
    }

    @Test
    void closesAuthenticatedConnectionAfterSubSecondReaderIdleTimeout() throws Exception {
        GatewayServer server = new GatewayServer(
                new GatewayConfig(0, "/ws", Duration.ofMillis(150)),
                new SessionRegistry(),
                TokenAuthenticator.nonBlankToken());
        EventLoopGroup clientGroup = new NioEventLoopGroup(1);

        try {
            server.start();
            TestClientHandler clientHandler = new TestClientHandler();
            URI uri = URI.create("ws://127.0.0.1:" + server.getPort() + "/ws");
            Channel clientChannel = connectClient(clientGroup, clientHandler, uri);
            assertTrue(clientHandler.awaitHandshake());
            clientChannel.writeAndFlush(Frame.newBuilder()
                    .setType(Frame.Type.CONNECT)
                    .setSequenceId("connect-idle")
                    .setConnectRequest(ConnectRequest.newBuilder()
                            .setToken("token")
                            .setClientId("client-idle")
                            .build())
                    .build()).sync();
            assertEquals(200, clientHandler.takeFrame().getConnectResponse().getCode());

            assertTrue(clientHandler.awaitClose());
        } finally {
            clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            server.stop();
        }
    }

    @Test
    void authenticatesOutsideNettyIoThread() throws Exception {
        AtomicReference<String> authThreadName = new AtomicReference<>();
        GatewayServer server = new GatewayServer(
                new GatewayConfig(0, "/ws", Duration.ofSeconds(30)),
                new SessionRegistry(),
                request -> {
                    authThreadName.set(Thread.currentThread().getName());
                    return true;
                });
        EventLoopGroup clientGroup = new NioEventLoopGroup(1);

        try {
            server.start();
            TestClientHandler clientHandler = new TestClientHandler();
            URI uri = URI.create("ws://127.0.0.1:" + server.getPort() + "/ws");
            Channel clientChannel = connectClient(clientGroup, clientHandler, uri);
            assertTrue(clientHandler.awaitHandshake());
            clientChannel.writeAndFlush(Frame.newBuilder()
                    .setType(Frame.Type.CONNECT)
                    .setSequenceId("connect-async-auth")
                    .setConnectRequest(ConnectRequest.newBuilder()
                            .setToken("token")
                            .setClientId("client-async-auth")
                            .build())
                    .build()).sync();

            assertEquals(200, clientHandler.takeFrame().getConnectResponse().getCode());
            assertTrue(authThreadName.get().startsWith("gateway-auth"));
            clientChannel.close().sync();
        } finally {
            clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            server.stop();
        }
    }

    @Test
    void rejectsBizReportWhenBoundedBusinessQueueIsFull() throws Exception {
        CountDownLatch sinkEntered = new CountDownLatch(1);
        CountDownLatch releaseSink = new CountDownLatch(1);
        CountDownLatch taskRejected = new CountDownLatch(1);
        AtomicInteger rejectedTasks = new AtomicInteger();
        GatewayMetrics metrics = new GatewayMetrics() {
            @Override
            public void businessTaskRejected() {
                rejectedTasks.incrementAndGet();
                taskRejected.countDown();
            }
        };
        GatewayConfig config = new GatewayConfig(
                0,
                "/ws",
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                64 * 1024,
                64 * 1024,
                32 * 1024,
                64 * 1024,
                1,
                16,
                1,
                16,
                64);
        GatewayServer server = new GatewayServer(
                config,
                new SessionRegistry(),
                TokenAuthenticator.nonBlankToken(),
                (clientId, reportData) -> {
                    sinkEntered.countDown();
                    try {
                        releaseSink.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                metrics);
        EventLoopGroup clientGroup = new NioEventLoopGroup(1);

        try {
            server.start();
            TestClientHandler clientHandler = new TestClientHandler();
            URI uri = URI.create("ws://127.0.0.1:" + server.getPort() + "/ws");
            Channel clientChannel = connectClient(clientGroup, clientHandler, uri);
            assertTrue(clientHandler.awaitHandshake());
            clientChannel.writeAndFlush(Frame.newBuilder()
                    .setType(Frame.Type.CONNECT)
                    .setConnectRequest(ConnectRequest.newBuilder()
                            .setToken("token")
                            .setClientId("client-overload")
                            .build())
                    .build()).sync();
            assertEquals(200, clientHandler.takeFrame().getConnectResponse().getCode());

            clientChannel.writeAndFlush(reportFrame("first")).sync();
            assertTrue(sinkEntered.await(5, TimeUnit.SECONDS));
            for (int i = 0; i < 17; i++) {
                clientChannel.write(reportFrame("queued-" + i));
            }
            clientChannel.flush();

            assertTrue(taskRejected.await(5, TimeUnit.SECONDS));
            assertTrue(rejectedTasks.get() >= 1);
            releaseSink.countDown();
            clientChannel.close().sync();
        } finally {
            releaseSink.countDown();
            clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            server.stop();
        }
    }

    private static Frame reportFrame(String metric) {
        return Frame.newBuilder()
                .setType(Frame.Type.BIZ_REPORT)
                .setReportData(ReportData.newBuilder().setMetric(metric).build())
                .build();
    }

    private static Channel connectClient(
            EventLoopGroup clientGroup,
            TestClientHandler clientHandler,
            URI uri
    ) throws InterruptedException {
        return new Bootstrap()
                .group(clientGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
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
                                .addLast(clientHandler);
                    }
                })
                .connect(uri.getHost(), uri.getPort())
                .sync()
                .channel();
    }

    private static final class TestClientHandler extends SimpleChannelInboundHandler<Frame> {
        private final CountDownLatch handshakeComplete = new CountDownLatch(1);
        private final CountDownLatch channelClosed = new CountDownLatch(1);
        private final BlockingQueue<Frame> frames = new LinkedBlockingQueue<>();

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame msg) {
            frames.add(msg);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                handshakeComplete.countDown();
                return;
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            channelClosed.countDown();
            super.channelInactive(ctx);
        }

        boolean awaitHandshake() throws InterruptedException {
            return handshakeComplete.await(5, TimeUnit.SECONDS);
        }

        boolean awaitClose() throws InterruptedException {
            return channelClosed.await(5, TimeUnit.SECONDS);
        }

        Frame takeFrame() throws InterruptedException {
            Frame frame = frames.poll(5, TimeUnit.SECONDS);
            assertNotNull(frame);
            return frame;
        }
    }
}
