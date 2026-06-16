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
        // 出站方向与入站方向相反：业务代码写出 Protobuf Frame，
        // 本编码器把它序列化成 WebSocket 二进制帧，后续 Netty handler 再写到 socket。
        ByteBuf byteBuf = ctx.alloc().buffer(msg.getSerializedSize());
        try (ByteBufOutputStream output = new ByteBufOutputStream(byteBuf)) {
            msg.writeTo(output);
        } catch (Exception e) {
            // ByteBuf 是引用计数对象；如果编码失败且没有交给 out，必须主动 release，避免堆外内存泄漏。
            byteBuf.release();
            throw e;
        }
        // 加入 out 以后，ByteBuf 的生命周期交给后续 handler 和 Netty outbound 流程管理。
        out.add(new BinaryWebSocketFrame(byteBuf));
    }
}
