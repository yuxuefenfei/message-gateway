package com.gateway.push.session;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class SessionRegistry {
    private static final AttributeKey<String> CLIENT_ID_KEY = AttributeKey.valueOf("gateway.clientId");

    private final ConcurrentMap<String, Channel> sessions = new ConcurrentHashMap<>();
    private final ConcurrentMap<Channel, String> channelToClient = new ConcurrentHashMap<>();

    public void bind(String clientId, Channel channel) {
        String previousClientId = channelToClient.put(channel, clientId);
        if (previousClientId != null && !previousClientId.equals(clientId)) {
            sessions.remove(previousClientId, channel);
        }

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

    public Optional<String> findClientId(Channel channel) {
        String clientId = channel.attr(CLIENT_ID_KEY).get();
        if (clientId != null) {
            return Optional.of(clientId);
        }
        return Optional.ofNullable(channelToClient.get(channel));
    }

    public void unbind(Channel channel) {
        String clientId = channelToClient.remove(channel);
        channel.attr(CLIENT_ID_KEY).set(null);
        if (clientId != null) {
            sessions.remove(clientId, channel);
        }
    }

    public void closeAll() {
        for (Channel channel : sessions.values()) {
            channel.attr(CLIENT_ID_KEY).set(null);
            channel.close();
        }
        sessions.clear();
        channelToClient.clear();
    }

    public int size() {
        return sessions.size();
    }
}
