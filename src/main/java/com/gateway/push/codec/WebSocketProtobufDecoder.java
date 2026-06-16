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
        // BinaryWebSocketFrame 是 Netty 对 WebSocket 二进制消息的封装；真正的协议字节在 content() 的 ByteBuf 里。
        // MessageToMessageDecoder 会在本方法返回后释放入站 frame，因此不要把 content 长期保存到别的线程。
        ByteBuf content = frame.content();
        if (!content.isReadable()) {
            return;
        }

        try {
            if (content.nioBufferCount() == 1) {
                // 单段 ByteBuf 可以直接暴露成 ByteBuffer，让 Protobuf 从 Netty 缓冲区读取，少一次 byte[] 拷贝。
                ByteBuffer byteBuffer = content.nioBuffer();
                CodedInputStream input = CodedInputStream.newInstance(byteBuffer);
                out.add(Frame.parseFrom(input));
                return;
            }

            // Composite ByteBuf 可能由多个内存片段组成，用 InputStream 适配器交给 Protobuf 顺序读取。
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
