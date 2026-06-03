package com.gateway.push.codec;

import com.gateway.push.metrics.GatewayMetrics;
import com.gateway.push.protocol.Frame;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class WebSocketProtobufCodecTest {
    @Test
    void encodesFrameAsBinaryWebSocketFrame() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new WebSocketProtobufEncoder());
        Frame frame = Frame.newBuilder()
                .setType(Frame.Type.PING)
                .setTimestamp(100L)
                .setSequenceId("seq-1")
                .build();

        channel.writeOutbound(frame);

        BinaryWebSocketFrame encoded = channel.readOutbound();
        assertNotNull(encoded);
        assertEquals(frame, Frame.parseFrom(encoded.content().nioBuffer()));
        encoded.release();
        channel.finishAndReleaseAll();
    }

    @Test
    void decodesBinaryWebSocketFrameAsFrame() {
        EmbeddedChannel channel = new EmbeddedChannel(new WebSocketProtobufDecoder());
        Frame frame = Frame.newBuilder()
                .setType(Frame.Type.PING)
                .setTimestamp(100L)
                .setSequenceId("seq-2")
                .build();

        channel.writeInbound(new BinaryWebSocketFrame(channel.alloc().buffer().writeBytes(frame.toByteArray())));

        Frame decoded = channel.readInbound();
        assertEquals(frame, decoded);
        channel.finishAndReleaseAll();
    }

    @Test
    void decodesCompositeBinaryWebSocketFrameAsFrame() {
        EmbeddedChannel channel = new EmbeddedChannel(new WebSocketProtobufDecoder());
        Frame frame = Frame.newBuilder()
                .setType(Frame.Type.PING)
                .setTimestamp(100L)
                .setSequenceId("seq-3")
                .build();
        byte[] bytes = frame.toByteArray();
        CompositeByteBuf composite = channel.alloc().compositeBuffer()
                .addComponent(true, channel.alloc().buffer().writeBytes(bytes, 0, 2))
                .addComponent(true, channel.alloc().buffer().writeBytes(bytes, 2, bytes.length - 2));

        channel.writeInbound(new BinaryWebSocketFrame(composite));

        Frame decoded = channel.readInbound();
        assertEquals(frame, decoded);
        channel.finishAndReleaseAll();
    }

    @Test
    void closesChannelAndRecordsMetricWhenFrameCannotBeDecoded() {
        AtomicInteger decodeFailures = new AtomicInteger();
        GatewayMetrics metrics = new GatewayMetrics() {
            @Override
            public void decodeFailed() {
                decodeFailures.incrementAndGet();
            }
        };
        EmbeddedChannel channel = new EmbeddedChannel(new WebSocketProtobufDecoder(metrics));

        channel.writeInbound(new BinaryWebSocketFrame(channel.alloc().buffer().writeBytes(new byte[]{26, 5, 1})));

        assertEquals(1, decodeFailures.get());
        assertFalse(channel.isActive());
        channel.finishAndReleaseAll();
    }
}
