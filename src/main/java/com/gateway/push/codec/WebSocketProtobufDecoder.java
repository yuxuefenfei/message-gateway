package com.gateway.push.codec;

import com.gateway.push.protocol.Frame;
import com.gateway.push.metrics.GatewayMetrics;
import com.google.protobuf.CodedInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

import java.nio.ByteBuffer;
import java.util.List;

public final class WebSocketProtobufDecoder extends MessageToMessageDecoder<BinaryWebSocketFrame> {
    private final GatewayMetrics metrics;

    public WebSocketProtobufDecoder() {
        this(GatewayMetrics.noop());
    }

    public WebSocketProtobufDecoder(GatewayMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, BinaryWebSocketFrame frame, List<Object> out) throws Exception {
        ByteBuf content = frame.content();
        if (!content.isReadable()) {
            return;
        }

        try {
            if (content.nioBufferCount() == 1) {
                ByteBuffer byteBuffer = content.nioBuffer();
                CodedInputStream input = CodedInputStream.newInstance(byteBuffer);
                out.add(Frame.parseFrom(input));
                return;
            }

            try (ByteBufInputStream input = new ByteBufInputStream(content, false)) {
                out.add(Frame.parseFrom(input));
            }
        } catch (Exception e) {
            metrics.decodeFailed();
            ctx.close();
        }
    }
}
