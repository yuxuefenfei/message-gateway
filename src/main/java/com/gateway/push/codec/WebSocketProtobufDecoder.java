package com.gateway.push.codec;

import com.gateway.push.protocol.Frame;
import com.gateway.push.metrics.GatewayMetrics;
import com.google.protobuf.CodedInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * WebSocket 二进制帧到 Protobuf Frame 的解码器。
 *
 * <p>正常情况下 WebSocket 的二进制 payload 会是单个连续 ByteBuf，解码器会走
 * NIO ByteBuffer 快路径，减少临时数组分配；如果遇到 composite buffer，则回退到
 * ByteBufInputStream，保证协议解析的完整性。</p>
 */
@RequiredArgsConstructor
public final class WebSocketProtobufDecoder extends MessageToMessageDecoder<BinaryWebSocketFrame> {
    private final GatewayMetrics metrics;

    public WebSocketProtobufDecoder() {
        this(GatewayMetrics.noop());
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
            // 坏帧直接关闭连接，并记录指标；不继续向后传播异常，避免 pipeline 输出重复噪音。
            metrics.decodeFailed();
            ctx.close();
        }
    }
}
