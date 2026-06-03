package com.gateway.push.codec;

import com.gateway.push.protocol.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

import java.util.List;

/**
 * Protobuf Frame 到 WebSocket 二进制帧的编码器。
 *
 * <p>编码时根据 Protobuf 序列化大小直接申请 ByteBuf，并通过 ByteBufOutputStream 写入，
 * 避免先构造 byte[] 再拷贝到 ByteBuf 的额外内存分配。</p>
 */
public final class WebSocketProtobufEncoder extends MessageToMessageEncoder<Frame> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Frame msg, List<Object> out) throws Exception {
        ByteBuf byteBuf = ctx.alloc().buffer(msg.getSerializedSize());
        try (ByteBufOutputStream output = new ByteBufOutputStream(byteBuf)) {
            msg.writeTo(output);
        } catch (Exception e) {
            byteBuf.release();
            throw e;
        }
        out.add(new BinaryWebSocketFrame(byteBuf));
    }
}
