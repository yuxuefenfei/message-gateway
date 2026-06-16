package com.gateway.push.session;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;

import java.util.Optional;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 本机有状态会话注册表。
 *
 * <p>网关是长连接服务，必须在内存中维护 clientId 到 Channel 的映射，
 * 才能支持服务端主动推送。这里同时维护正向和反向映射：
 * 正向映射用于按 clientId 查找连接，反向映射用于断线时快速清理。</p>
 *
 * <p>认证成功后还会把 clientId 写入 Channel Attribute，读路径优先访问本地属性，
 * 避免每个业务帧都查 ConcurrentHashMap。</p>
 */
public final class SessionRegistry {
    // AttributeKey 是 Netty 给每个 Channel 挂载本地状态的方式，类似“连接对象上的线程安全小标签”。
    private static final AttributeKey<String> CLIENT_ID_KEY = AttributeKey.valueOf("gateway.clientId");

    // clientId -> Channel：服务端主动推送时使用。
    private final ConcurrentMap<String, Channel> sessions = new ConcurrentHashMap<>();
    // Channel -> clientId：连接断开时反向找到 clientId，用于清理 sessions。
    private final ConcurrentMap<Channel, String> channelToClient = new ConcurrentHashMap<>();

    /**
     * 绑定 clientId 和 Channel。
     *
     * <p>如果同一个 clientId 已经在线，会关闭旧连接，让新连接成为唯一有效会话。
     * 如果同一个 Channel 曾经绑定过其它 clientId，会先清理旧映射，避免重复 CONNECT
     * 或协议异常导致脏会话残留。</p>
     */
    public void bind(String clientId, Channel channel) {
        // 先建立反向映射，再处理正向映射。这样即使旧 Channel 被关闭，channelInactive 也能找到应清理的 clientId。
        String previousClientId = channelToClient.put(channel, clientId);
        if (previousClientId != null && !previousClientId.equals(clientId)) {
            sessions.remove(previousClientId, channel);
        }

        // sessions.put 返回同 clientId 的旧连接；网关采用“新连接踢旧连接”的策略。
        Channel oldChannel = sessions.put(clientId, channel);
        if (oldChannel != null && oldChannel != channel) {
            channelToClient.remove(oldChannel, clientId);
            oldChannel.attr(CLIENT_ID_KEY).set(null);
            oldChannel.close();
        }
        channel.attr(CLIENT_ID_KEY).set(clientId);
    }

    public Optional<Channel> findChannel(String clientId) {
        return Optional.ofNullable(sessions.get(clientId));
    }

    /**
     * 查询 Channel 当前绑定的 clientId。
     *
     * <p>优先读取 Channel Attribute，这是认证后热路径；如果属性不存在，再回退到反向映射。</p>
     */
    public Optional<String> findClientId(Channel channel) {
        String clientId = channel.attr(CLIENT_ID_KEY).get();
        if (clientId != null) {
            return Optional.of(clientId);
        }
        return Optional.ofNullable(channelToClient.get(channel));
    }

    /**
     * 解除 Channel 绑定。
     *
     * <p>通常在 channelInactive 时调用，确保连接断开后正向、反向映射和 Channel Attribute
     * 都被清理。</p>
     */
    public void unbind(Channel channel) {
        String clientId = channelToClient.remove(channel);
        channel.attr(CLIENT_ID_KEY).set(null);
        if (clientId != null) {
            sessions.remove(clientId, channel);
        }
    }

    /**
     * 关闭并清空所有已认证会话。
     *
     * <p>服务停机时调用，避免只关闭监听 socket 而遗留已经建立的客户端连接。</p>
     */
    public void closeAll() {
        List<ChannelFuture> closeFutures = new ArrayList<>(sessions.size());
        for (Channel channel : sessions.values()) {
            channel.attr(CLIENT_ID_KEY).set(null);
            closeFutures.add(channel.close());
        }
        for (ChannelFuture closeFuture : closeFutures) {
            closeFuture.syncUninterruptibly();
        }
        sessions.clear();
        channelToClient.clear();
    }

    public int size() {
        return sessions.size();
    }
}
