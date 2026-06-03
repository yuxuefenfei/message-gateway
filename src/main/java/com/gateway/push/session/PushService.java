package com.gateway.push.session;

import com.gateway.push.protocol.Frame;
import com.gateway.push.protocol.Notification;
import com.gateway.push.metrics.GatewayMetrics;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public final class PushService {
    private final SessionRegistry sessionRegistry;
    private final GatewayMetrics metrics;

    public PushService(SessionRegistry sessionRegistry) {
        this(sessionRegistry, GatewayMetrics.noop());
    }

    public PushService(SessionRegistry sessionRegistry, GatewayMetrics metrics) {
        this.sessionRegistry = sessionRegistry;
        this.metrics = metrics;
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
            channel.ifPresent(sessionRegistry::unbind);
            return Optional.empty();
        }

        if (!channel.get().isWritable()) {
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
