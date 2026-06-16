package com.gateway.push.session;

import com.gateway.push.protocol.Frame;
import com.gateway.push.protocol.Notification;
import com.gateway.push.metrics.GatewayMetrics;
import com.gateway.push.config.GatewayConfig;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 服务端主动推送能力封装。
 *
 * <p>PushService 根据 clientId 查找本机连接并写出 NOTIFY 帧。
 * 写出前会检查 Channel 是否 active/writable，避免慢客户端导致 Netty outbound buffer
 * 持续堆积。批量推送同一客户端时，会多次 write 后一次 flush，减少系统调用压力。</p>
 */
public final class PushService {
    private static final int DEFAULT_BATCH_CHUNK_SIZE = 64;

    private final SessionRegistry sessionRegistry;
    private final GatewayMetrics metrics;
    private final int batchChunkSize;

    public PushService(SessionRegistry sessionRegistry) {
        this(sessionRegistry, GatewayMetrics.noop(), DEFAULT_BATCH_CHUNK_SIZE);
    }

    public PushService(SessionRegistry sessionRegistry, GatewayMetrics metrics) {
        this(sessionRegistry, metrics, DEFAULT_BATCH_CHUNK_SIZE);
    }

    public PushService(SessionRegistry sessionRegistry, GatewayMetrics metrics, int batchChunkSize) {
        if (batchChunkSize <= 0) {
            throw new IllegalArgumentException("batchChunkSize must be positive");
        }
        this.sessionRegistry = sessionRegistry;
        this.metrics = metrics;
        this.batchChunkSize = batchChunkSize;
    }

    public PushService(SessionRegistry sessionRegistry, GatewayMetrics metrics, GatewayConfig config) {
        this(sessionRegistry, metrics, config.getPushBatchChunkSize());
    }

    /**
     * 向指定在线客户端推送一条通知。
     *
     * @return true 表示写入成功；客户端离线或当前受背压限制时返回 false；异步写失败则异常完成
     */
    public CompletableFuture<Boolean> pushToClient(String clientId, Notification notification) {
        Optional<Channel> channel = writableChannel(clientId);
        if (channel.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }

        return writeAndTrack(channel.get(), toNotifyFrame(notification), true);
    }

    /**
     * 向同一客户端分块推送多条通知。
     *
     * <p>每个分块仅 flush 一次，分块之间重新检查 Channel 状态和写水位。返回 false 表示
     * 客户端离线或剩余消息因背压停止发送，已经成功写出的前置分块不会回滚。</p>
     */
    public CompletableFuture<Boolean> pushManyToClient(String clientId, Collection<Notification> notifications) {
        if (notifications == null || notifications.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }

        Optional<Channel> channel = writableChannel(clientId);
        if (channel.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        Iterator<Notification> iterator = notifications.iterator();
        // 首块立即写出；后续块由 write future 串行调度，避免整批消息一次性进入 outbound buffer。
        writeNextChunk(channel.get(), iterator, result);
        return result;
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
        // Netty 的 write 是异步的，返回 ChannelFuture；数据真正写成功或失败要通过 listener 得知。
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

    private CompletableFuture<Boolean> trackBatchWrite(List<ChannelFuture> futures) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        // 一个批次里有多次 write，需要等所有 ChannelFuture 都完成后，才能决定本批整体结果。
        AtomicInteger remaining = new AtomicInteger(futures.size());
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicReference<Throwable> firstCause = new AtomicReference<>();

        for (ChannelFuture channelFuture : futures) {
            channelFuture.addListener(future -> {
                if (future.isSuccess()) {
                    metrics.pushSucceeded();
                } else {
                    metrics.pushFailed();
                    failed.set(true);
                    firstCause.compareAndSet(null, future.cause());
                }

                if (remaining.decrementAndGet() == 0) {
                    if (failed.get()) {
                        Throwable cause = firstCause.get();
                        result.completeExceptionally(cause == null
                                ? new IllegalStateException("Batch push failed without cause")
                                : cause);
                    } else {
                        result.complete(true);
                    }
                }
            });
        }
        return result;
    }

    private void writeNextChunk(
            Channel channel,
            Iterator<Notification> notifications,
            CompletableFuture<Boolean> result
    ) {
        // 每个分块都重新校验连接和写水位，背压出现后停止消费剩余通知。
        if (result.isDone()) {
            return;
        }
        if (!channel.isActive()) {
            sessionRegistry.unbind(channel);
            result.complete(false);
            return;
        }
        if (!channel.isWritable()) {
            metrics.pushRejectedBackpressure();
            result.complete(false);
            return;
        }

        try {
            List<ChannelFuture> futures = new ArrayList<>(batchChunkSize);
            while (futures.size() < batchChunkSize && notifications.hasNext() && channel.isWritable()) {
                futures.add(channel.write(toNotifyFrame(notifications.next())));
            }

            if (futures.isEmpty()) {
                if (notifications.hasNext()) {
                    metrics.pushRejectedBackpressure();
                    result.complete(false);
                } else {
                    result.complete(true);
                }
                return;
            }

            channel.flush();
            trackBatchWrite(futures).whenComplete((sent, failure) -> {
                if (failure != null) {
                    result.completeExceptionally(failure);
                } else {
                    try {
                        if (notifications.hasNext()) {
                            // 后续分块重新投递回 Channel 的 EventLoop，让批量推送和该连接其它 IO 事件串行执行。
                            channel.eventLoop().execute(() -> writeNextChunk(channel, notifications, result));
                        } else {
                            result.complete(true);
                        }
                    } catch (RuntimeException e) {
                        metrics.pushFailed();
                        result.completeExceptionally(e);
                    }
                }
            });
        } catch (RuntimeException e) {
            metrics.pushFailed();
            result.completeExceptionally(e);
        }
    }
}
