package com.gateway.push.session;

import com.gateway.push.metrics.GatewayMetrics;
import com.gateway.push.protocol.Frame;
import com.gateway.push.protocol.Notification;
import com.google.protobuf.ByteString;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PushServiceTest {
    @Test
    void completesTrueWhenWriteSucceeds() throws Exception {
        SessionRegistry registry = new SessionRegistry();
        EmbeddedChannel channel = new EmbeddedChannel();
        registry.bind("client-a", channel);
        AtomicInteger pushSucceeded = new AtomicInteger();
        PushService pushService = new PushService(registry, new GatewayMetrics() {
            @Override
            public void pushSucceeded() {
                pushSucceeded.incrementAndGet();
            }
        });

        boolean sent = pushService.pushToClient("client-a", Notification.newBuilder()
                .setTopic("topic-a")
                .setBizId("biz-1")
                .setTitle("title")
                .setPayload(ByteString.copyFromUtf8("payload"))
                .build()).get(5, TimeUnit.SECONDS);

        Frame outbound = channel.readOutbound();
        assertTrue(sent);
        assertTrue(outbound.hasNotification());
        assertEquals(1, pushSucceeded.get());

        channel.finishAndReleaseAll();
    }

    @Test
    void completesFalseWhenClientIsOffline() throws Exception {
        PushService pushService = new PushService(new SessionRegistry());

        boolean sent = pushService.pushToClient("missing-client", Notification.newBuilder()
                .setTopic("topic-a")
                .build()).get(5, TimeUnit.SECONDS);

        assertFalse(sent);
    }

    @Test
    void completesFalseAndRecordsBackpressureWhenChannelIsNotWritable() throws Exception {
        SessionRegistry registry = new SessionRegistry();
        EmbeddedChannel channel = new EmbeddedChannel();
        registry.bind("client-slow", channel);
        AtomicInteger backpressureRejects = new AtomicInteger();
        PushService pushService = new PushService(registry, new GatewayMetrics() {
            @Override
            public void pushRejectedBackpressure() {
                backpressureRejects.incrementAndGet();
            }
        });
        channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);

        boolean sent = pushService.pushToClient("client-slow", Notification.newBuilder()
                .setTopic("topic-a")
                .build()).get(5, TimeUnit.SECONDS);

        assertFalse(sent);
        assertEquals(1, backpressureRejects.get());
        channel.unsafe().outboundBuffer().setUserDefinedWritability(1, true);
        channel.finishAndReleaseAll();
    }

    @Test
    void writesBatchAndFlushesOnceForSameClient() throws Exception {
        SessionRegistry registry = new SessionRegistry();
        EmbeddedChannel channel = new EmbeddedChannel();
        registry.bind("client-batch", channel);
        PushService pushService = new PushService(registry);

        boolean sent = pushService.pushManyToClient("client-batch", List.of(
                Notification.newBuilder().setTopic("topic-a").build(),
                Notification.newBuilder().setTopic("topic-b").build()
        )).get(5, TimeUnit.SECONDS);

        Frame first = channel.readOutbound();
        Frame second = channel.readOutbound();
        assertTrue(sent);
        assertEquals("topic-a", first.getNotification().getTopic());
        assertEquals("topic-b", second.getNotification().getTopic());

        channel.finishAndReleaseAll();
    }
}
