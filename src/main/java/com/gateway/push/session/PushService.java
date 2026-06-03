package com.gateway.push.session;

import com.gateway.push.protocol.Frame;
import com.gateway.push.protocol.Notification;
import com.gateway.push.metrics.GatewayMetrics;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * 服务端主动推送能力封装。
 *
 * <p>PushService 根据 clientId 查找本机连接并写出 NOTIFY 帧。
 * 写出前会检查 Channel 是否 active/writable，避免慢客户端导致 Netty outbound buffer
 * 持续堆积。批量推送同一客户端时，会多次 write 后一次 flush，减少系统调用压力。</p>
 */
@RequiredArgsConstructor
public final class PushService {
    private final SessionRegistry sessionRegistry;
    private final GatewayMetrics metrics;

    public PushService(SessionRegistry sessionRegistry) {
        this(sessionRegistry, GatewayMetrics.noop());
    }

    public CompletableFuture<Boolean> pushToClient(String clientId, Notification notification) {
        Optional<Channel> channel = writableChannel(clientId);
        if (channel.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }

        return writeAndTrack(channel.get(), toNotifyFrame(notification), true);
    }

    public CompletableFuture<Boolean> pushManyToClient(String clientId, Collection<Notification> notifications) {
        if (notifications == null || notifications.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }

        Optional<Channel> channel = writableChannel(clientId);
        if (channel.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }

        List<Frame> frames = new ArrayList<>(notifications.size());
        for (Notification notification : notifications) {
            frames.add(toNotifyFrame(notification));
        }

        for (int i = 0; i < frames.size() - 1; i++) {
            channel.get().write(frames.get(i));
        }
        return writeAndTrack(channel.get(), frames.get(frames.size() - 1), true);
    }

    private Optional<Channel> writableChannel(String clientId) {
        Optional<Channel> channel = sessionRegistry.findChannel(clientId);
        if (channel.isEmpty() || !channel.get().isActive()) {
            // 连接已经不可用时同步清理本地路由，避免后续推送反复命中无效 Channel。
            channel.ifPresent(sessionRegistry::unbind);
            return Optional.empty();
        }

        if (!channel.get().isWritable()) {
            // Netty 写缓冲超过高水位时拒绝本次推送，由调用方决定重试、降级或丢弃。
            metrics.pushRejectedBackpressure();
            return Optional.empty();
        }

        return channel;
    }

    private Frame toNotifyFrame(Notification notification) {
        return Frame.newBuilder()
                .setType(Frame.Type.NOTIFY)
                .setTimestamp(Instant.now().toEpochMilli())
                .setSequenceId(UUID.randomUUID().toString())
                .setNotification(notification)
                .build();
    }

    private CompletableFuture<Boolean> writeAndTrack(Channel channel, Frame frame, boolean flush) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        ChannelFuture channelFuture = flush ? channel.writeAndFlush(frame) : channel.write(frame);
        channelFuture.addListener(future -> {
            if (future.isSuccess()) {
                metrics.pushSucceeded();
                result.complete(true);
            } else {
                metrics.pushFailed();
                result.completeExceptionally(future.cause());
            }
        });
        return result;
    }
}
