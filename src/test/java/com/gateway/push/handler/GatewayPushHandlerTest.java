package com.gateway.push.handler;

import com.gateway.push.auth.TokenAuthenticator;
import com.gateway.push.protocol.ConnectRequest;
import com.gateway.push.protocol.Frame;
import com.gateway.push.protocol.ReportData;
import com.gateway.push.session.SessionRegistry;
import com.google.protobuf.ByteString;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.IdleStateEvent;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayPushHandlerTest {
    @Test
    void bindsSessionAndReturnsConnectAckWhenTokenIsValid() {
        SessionRegistry registry = new SessionRegistry();
        EmbeddedChannel channel = new EmbeddedChannel(
                new GatewayPushHandler(registry, TokenAuthenticator.nonBlankToken()));

        channel.writeInbound(Frame.newBuilder()
                .setType(Frame.Type.CONNECT)
                .setSequenceId("connect-1")
                .setConnectRequest(ConnectRequest.newBuilder()
                        .setToken("token")
                        .setClientId("client-a")
                        .setClientVersion("1.0.0")
                        .build())
                .build());

        Frame ack = channel.readOutbound();
        assertEquals(Frame.Type.CONNECT_ACK, ack.getType());
        assertEquals(200, ack.getConnectResponse().getCode());
        assertTrue(registry.findChannel("client-a").isPresent());

        channel.finishAndReleaseAll();
    }

    @Test
    void rejectsInvalidTokenAndDoesNotBindSession() {
        SessionRegistry registry = new SessionRegistry();
        EmbeddedChannel channel = new EmbeddedChannel(
                new GatewayPushHandler(registry, TokenAuthenticator.nonBlankToken()));

        channel.writeInbound(Frame.newBuilder()
                .setType(Frame.Type.CONNECT)
                .setSequenceId("connect-2")
                .setConnectRequest(ConnectRequest.newBuilder()
                        .setClientId("client-b")
                        .setClientVersion("1.0.0")
                        .build())
                .build());

        Frame ack = channel.readOutbound();
        assertEquals(401, ack.getConnectResponse().getCode());
        assertFalse(registry.findChannel("client-b").isPresent());
        assertFalse(channel.isActive());

        channel.finishAndReleaseAll();
    }

    @Test
    void rejectsRepeatedConnectOnSameChannel() {
        SessionRegistry registry = new SessionRegistry();
        EmbeddedChannel channel = new EmbeddedChannel(
                new GatewayPushHandler(registry, TokenAuthenticator.nonBlankToken()));

        channel.writeInbound(Frame.newBuilder()
                .setType(Frame.Type.CONNECT)
                .setSequenceId("connect-3")
                .setConnectRequest(ConnectRequest.newBuilder()
                        .setToken("token")
                        .setClientId("client-c")
                        .build())
                .build());
        channel.readOutbound();

        channel.writeInbound(Frame.newBuilder()
                .setType(Frame.Type.CONNECT)
                .setSequenceId("connect-4")
                .setConnectRequest(ConnectRequest.newBuilder()
                        .setToken("token")
                        .setClientId("client-d")
                        .build())
                .build());

        Frame ack = channel.readOutbound();
        assertEquals(409, ack.getConnectResponse().getCode());
        assertFalse(registry.findChannel("client-c").isPresent());
        assertFalse(registry.findChannel("client-d").isPresent());
        assertFalse(channel.isActive());

        channel.finishAndReleaseAll();
    }

    @Test
    void respondsToPingWithPong() {
        SessionRegistry registry = new SessionRegistry();
        EmbeddedChannel channel = new EmbeddedChannel(
                new GatewayPushHandler(registry, TokenAuthenticator.nonBlankToken()));
        registry.bind("client-ping", channel);

        channel.writeInbound(Frame.newBuilder()
                .setType(Frame.Type.PING)
                .setSequenceId("ping-1")
                .build());

        Frame pong = channel.readOutbound();
        assertEquals(Frame.Type.PONG, pong.getType());
        assertEquals("ping-1", pong.getSequenceId());

        channel.finishAndReleaseAll();
    }

    @Test
    void closesUnauthenticatedPingWithoutPong() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new GatewayPushHandler(new SessionRegistry(), TokenAuthenticator.nonBlankToken()));

        channel.writeInbound(Frame.newBuilder()
                .setType(Frame.Type.PING)
                .setSequenceId("ping-unauthenticated")
                .build());

        assertFalse(channel.isActive());
        assertFalse(channel.outboundMessages().iterator().hasNext());
        channel.finishAndReleaseAll();
    }

    @Test
    void closesUnsupportedFrameFromAuthenticatedClient() {
        SessionRegistry registry = new SessionRegistry();
        AtomicInteger unsupportedRejects = new AtomicInteger();
        EmbeddedChannel channel = new EmbeddedChannel(
                new GatewayPushHandler(registry, TokenAuthenticator.nonBlankToken(), new com.gateway.push.metrics.GatewayMetrics() {
                    @Override
                    public void unsupportedFrameRejected() {
                        unsupportedRejects.incrementAndGet();
                    }
                }));
        registry.bind("client-unsupported", channel);

        channel.writeInbound(Frame.newBuilder()
                .setType(Frame.Type.TYPE_UNSPECIFIED)
                .build());

        assertFalse(channel.isActive());
        assertEquals(1, unsupportedRejects.get());
        channel.finishAndReleaseAll();
    }

    @Test
    void closesUnauthenticatedBusinessReportWhenHandlerIsUsedDirectly() {
        AtomicInteger unauthenticatedRejects = new AtomicInteger();
        EmbeddedChannel channel = new EmbeddedChannel(new BizReportHandler(
                new SessionRegistry(),
                (clientId, reportData) -> {
                },
                new com.gateway.push.metrics.GatewayMetrics() {
                    @Override
                    public void unauthenticatedFrameRejected() {
                        unauthenticatedRejects.incrementAndGet();
                    }
                }));

        channel.writeInbound(Frame.newBuilder()
                .setType(Frame.Type.BIZ_REPORT)
                .setReportData(ReportData.newBuilder().setMetric("temperature").build())
                .build());

        assertFalse(channel.isActive());
        assertEquals(1, unauthenticatedRejects.get());
        channel.finishAndReleaseAll();
    }

    @Test
    void closesChannelWhenReaderIdleEventFires() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new GatewayPushHandler(new SessionRegistry(), TokenAuthenticator.nonBlankToken()));

        channel.pipeline().fireUserEventTriggered(IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT);

        assertFalse(channel.isActive());
        channel.finishAndReleaseAll();
    }

    @Test
    void acceptsBusinessReportFromBoundClient() {
        SessionRegistry registry = new SessionRegistry();
        AtomicReference<String> reportedMetric = new AtomicReference<>();
        EmbeddedChannel channel = new EmbeddedChannel(
                new GatewayPushHandler(registry, TokenAuthenticator.nonBlankToken()),
                new BizReportHandler(
                        registry,
                        (clientId, reportData) -> reportedMetric.set(reportData.getMetric()),
                        com.gateway.push.metrics.GatewayMetrics.noop()));
        registry.bind("client-report", channel);

        channel.writeInbound(Frame.newBuilder()
                .setType(Frame.Type.BIZ_REPORT)
                .setReportData(ReportData.newBuilder()
                        .setMetric("temperature")
                        .setData(ByteString.copyFromUtf8("36.5"))
                        .build())
                .build());

        assertTrue(channel.isActive());
        assertTrue(registry.findClientId(channel).isPresent());
        assertEquals("temperature", reportedMetric.get());
        channel.finishAndReleaseAll();
    }
}
