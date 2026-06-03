package com.gateway.push.codec;

import com.gateway.push.protocol.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

import java.util.List;

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
